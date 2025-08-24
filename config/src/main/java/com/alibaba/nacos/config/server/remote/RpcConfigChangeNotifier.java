/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.config.server.remote;

import com.alibaba.nacos.api.config.remote.request.ConfigChangeNotifyRequest;
import com.alibaba.nacos.api.remote.AbstractPushCallBack;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.config.server.configuration.ConfigCommonConfig;
import com.alibaba.nacos.config.server.model.event.LocalDataChangeEvent;
import com.alibaba.nacos.config.server.utils.ConfigExecutor;
import com.alibaba.nacos.config.server.utils.GroupKey;
import com.alibaba.nacos.core.remote.Connection;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.remote.ConnectionMeta;
import com.alibaba.nacos.core.remote.RpcPushService;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.plugin.control.ControlManagerCenter;
import com.alibaba.nacos.plugin.control.tps.TpsControlManager;
import com.alibaba.nacos.plugin.control.tps.request.TpsCheckRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * ConfigChangeNotifier.
 *
 * @author liuzunfei
 * @version $Id: ConfigChangeNotifier.java, v 0.1 2020年07月20日 3:00 PM liuzunfei Exp $
 */
@Component(value = "rpcConfigChangeNotifier")
public class RpcConfigChangeNotifier extends Subscriber<LocalDataChangeEvent> {
    
    private static final String POINT_CONFIG_PUSH = "CONFIG_PUSH_COUNT";
    
    private static final String POINT_CONFIG_PUSH_SUCCESS = "CONFIG_PUSH_SUCCESS";
    
    private static final String POINT_CONFIG_PUSH_FAIL = "CONFIG_PUSH_FAIL";
    
    TpsControlManager tpsControlManager = ControlManagerCenter.getInstance().getTpsControlManager();
    
    public RpcConfigChangeNotifier() {
        NotifyCenter.registerSubscriber(this);
    }
    
    @PostConstruct
    void registerTpsPoint() {
        tpsControlManager.registerTpsPoint(POINT_CONFIG_PUSH);
        tpsControlManager.registerTpsPoint(POINT_CONFIG_PUSH_SUCCESS);
        tpsControlManager.registerTpsPoint(POINT_CONFIG_PUSH_FAIL);
        
    }
    
    @Autowired
    ConfigChangeListenContext configChangeListenContext;
    
    @Autowired
    private RpcPushService rpcPushService;
    
    @Autowired
    private ConnectionManager connectionManager;
    
    /**
     * adaptor to config module ,when server side config change ,invoke this method.
     *
     * @param groupKey groupKey
     */
    public void configDataChanged(String groupKey, String dataId, String group, String tenant) {
        Set<String> listeners = configChangeListenContext.getListeners(groupKey);
        if (CollectionUtils.isEmpty(listeners)) {
            return;
        }
        int notifyClientCount = 0;
        // 获取所有监听该配置的客户端连接
        for (final String client : listeners) {
            Connection connection = connectionManager.getConnection(client);
            if (connection == null) {
                continue;
            }
            boolean ifNamespaceTransfer = configChangeListenContext.getConfigListenState(client, groupKey).isNamespaceTransfer();
            if (ifNamespaceTransfer) {
                tenant = null;
            }
            ConnectionMeta metaInfo = connection.getMetaInfo();
            String clientIp = metaInfo.getClientIp();
            
            // 构建ConfigChangeNotifyRequest消息，包含变更的配置信息
            ConfigChangeNotifyRequest notifyRequest = ConfigChangeNotifyRequest.build(dataId, group, tenant);
            
            // 创建RpcPushTask异步推送任务，支持重试机制
            RpcPushTask rpcPushRetryTask = new RpcPushTask(notifyRequest,
                    ConfigCommonConfig.getInstance().getMaxPushRetryTimes(), client, clientIp, metaInfo.getAppName());
            // 异步推送通知
            push(rpcPushRetryTask, connectionManager);
            notifyClientCount++;
        }
        Loggers.REMOTE_PUSH.info("push [{}] clients, groupKey=[{}]", notifyClientCount, groupKey);
    }
    
    @Override
    public void onEvent(LocalDataChangeEvent event) {
        String groupKey = event.groupKey;
        
        String[] strings = GroupKey.parseKey(groupKey);
        String dataId = strings[0];
        String group = strings[1];
        String tenant = strings.length > 2 ? strings[2] : "";

        // [notifyConfig] server 步骤11: 监听LocalDataChangeEvent事件，通过gRPC双向流推送配置变更通知到客户端
        configDataChanged(groupKey, dataId, group, tenant);
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return LocalDataChangeEvent.class;
    }
    
    class RpcPushTask implements Runnable {
        
        ConfigChangeNotifyRequest notifyRequest;
        
        int maxRetryTimes = -1;
        
        int tryTimes = 0;
        
        String connectionId;
        
        String clientIp;
        
        String appName;
        
        public RpcPushTask(ConfigChangeNotifyRequest notifyRequest, int maxRetryTimes, String connectionId,
                String clientIp, String appName) {
            this.notifyRequest = notifyRequest;
            this.maxRetryTimes = maxRetryTimes;
            this.connectionId = connectionId;
            this.clientIp = clientIp;
            this.appName = appName;
        }
        
        public boolean isOverTimes() {
            return maxRetryTimes > 0 && this.tryTimes >= maxRetryTimes;
        }
        
        public int getTryTimes() {
            return tryTimes;
        }
        
        public ConfigChangeNotifyRequest getNotifyRequest() {
            return notifyRequest;
        }
        
        public int getMaxRetryTimes() {
            return maxRetryTimes;
        }
        
        public String getClientIp() {
            return clientIp;
        }
        
        public String getAppName() {
            return appName;
        }
        
        public String getConnectionId() {
            return connectionId;
        }
        
        @Override
        public void run() {
            // [notifyConfig] server 步骤13: 异步执行配置推送任务
            tryTimes++;
            TpsCheckRequest tpsCheckRequest = new TpsCheckRequest();
            
            tpsCheckRequest.setPointName(POINT_CONFIG_PUSH);
            if (!tpsControlManager.check(tpsCheckRequest).isSuccess()) {
                // TPS限流检查失败，延迟重试推送任务
                push(this, connectionManager);
            } else {
                // TPS检查通过，通过 gRPC 连接推送配置变更通知到客户端
                rpcPushService.pushWithCallback(connectionId, notifyRequest,
                        new RpcPushCallback(this, tpsControlManager, connectionManager),
                        ConfigExecutor.getClientConfigNotifierServiceExecutor());
            }
        }
    }
    
    static class RpcPushCallback extends AbstractPushCallBack {
        
        RpcPushTask rpcPushTask;
        
        TpsControlManager tpsControlManager;
        
        ConnectionManager connectionManager;
        
        public RpcPushCallback(RpcPushTask rpcPushTask, TpsControlManager tpsControlManager,
                ConnectionManager connectionManager) {
            super(3000L);
            this.rpcPushTask = rpcPushTask;
            this.tpsControlManager = tpsControlManager;
            this.connectionManager = connectionManager;
        }
        
        @Override
        public void onSuccess() {
            // [notifyConfig] server 步骤14a: 客户端成功接收配置变更通知，记录推送成功统计
            TpsCheckRequest tpsCheckRequest = new TpsCheckRequest();
            tpsCheckRequest.setPointName(POINT_CONFIG_PUSH_SUCCESS);
            tpsControlManager.check(tpsCheckRequest);
        }
        
        @Override
        public void onFail(Throwable e) {
            // [notifyConfig] server 步骤14b: 推送失败，记录失败统计并进行重试
            TpsCheckRequest tpsCheckRequest = new TpsCheckRequest();
            tpsCheckRequest.setPointName(POINT_CONFIG_PUSH_FAIL);
            tpsControlManager.check(tpsCheckRequest);
            Loggers.REMOTE_PUSH.warn("Push fail, dataId={}, group={}, tenant={}, clientId={}",
                    rpcPushTask.getNotifyRequest().getDataId(), rpcPushTask.getNotifyRequest().getGroup(),
                    rpcPushTask.getNotifyRequest().getTenant(), rpcPushTask.getConnectionId(), e);
            push(rpcPushTask, connectionManager);
        }
    }
    
    // [notifyConfig] server 步骤12: 处理推送任务重试逻辑，支持延迟重试和连接管理
    private static void push(RpcPushTask retryTask, ConnectionManager connectionManager) {
        ConfigChangeNotifyRequest notifyRequest = retryTask.getNotifyRequest();
        if (retryTask.isOverTimes()) {
            // 重试次数超限，注销客户端连接
            Loggers.REMOTE_PUSH.warn(
                    "push callback retry fail over times. dataId={},group={},tenant={},clientId={}, will unregister client.",
                    notifyRequest.getDataId(), notifyRequest.getGroup(), notifyRequest.getTenant(),
                    retryTask.getConnectionId());
            connectionManager.unregister(retryTask.getConnectionId());
        } else if (connectionManager.getConnection(retryTask.getConnectionId()) != null) {
            // 客户端连接存在，延迟重试推送（首次延迟0s，第二次2s，第三次4s）
            ConfigExecutor.scheduleClientConfigNotifier(retryTask, retryTask.getTryTimes() * 2, TimeUnit.SECONDS);
        } else {
            // 客户端已离线，忽略推送任务
        }
    }
    
}

