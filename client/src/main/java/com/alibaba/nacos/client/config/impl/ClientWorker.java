/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.ability.constant.AbilityKey;
import com.alibaba.nacos.api.ability.constant.AbilityStatus;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.FuzzyWatchEventWatcher;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.config.remote.request.ClientConfigMetricRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigBatchListenRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigChangeNotifyRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigPublishRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigQueryRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigRemoveRequest;
import com.alibaba.nacos.api.config.remote.response.ClientConfigMetricResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigChangeBatchListenResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigChangeNotifyResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigPublishResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigQueryResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigRemoveResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.client.address.ServerListChangeEvent;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.env.SourceType;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.utils.AppNameUtils;
import com.alibaba.nacos.client.utils.EnvUtil;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.labels.impl.DefaultLabelsCollectorManager;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.Connection;
import com.alibaba.nacos.common.remote.client.ConnectionEventListener;
import com.alibaba.nacos.common.remote.client.RpcClient;
import com.alibaba.nacos.common.remote.client.RpcClientConfigFactory;
import com.alibaba.nacos.common.remote.client.RpcClientFactory;
import com.alibaba.nacos.common.remote.client.ServerListFactory;
import com.alibaba.nacos.common.remote.client.grpc.GrpcClientConfig;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConnLabelsUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.common.utils.VersionUtils;
import com.alibaba.nacos.plugin.auth.api.RequestResource;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.nacos.api.common.Constants.APP_CONN_PREFIX;
import static com.alibaba.nacos.api.common.Constants.ENCODE;

/**
 * Long polling.
 *
 * @author Nacos
 */
public class ClientWorker implements Closeable {
    
    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);
    
    private static final String NOTIFY_HEADER = "notify";
    
    private static final String TAG_PARAM = "tag";
    
    private static final String APP_NAME_PARAM = "appName";
    
    private static final String BETAIPS_PARAM = "betaIps";
    
    private static final String TYPE_PARAM = "type";
    
    private static final String ENCRYPTED_DATA_KEY_PARAM = "encryptedDataKey";
    
    /**
     * groupKey -> cacheData.
     */
    private final AtomicReference<Map<String, CacheData>> cacheMap = new AtomicReference<>(new HashMap<>());
    
    private final DefaultLabelsCollectorManager defaultLabelsCollectorManager = new DefaultLabelsCollectorManager();
    
    private ConfigFuzzyWatchGroupKeyHolder configFuzzyWatchGroupKeyHolder;
    
    private Map<String, String> appLabels = new HashMap<>();
    
    private final ConfigFilterChainManager configFilterChainManager;
    
    private final String uuid = UUID.randomUUID().toString();
    
    private long requestTimeout;
    
    private final ConfigRpcTransportClient agent;
    
    private boolean enableRemoteSyncConfig = false;
    
    private static final int MIN_THREAD_NUM = 2;
    
    private static final int THREAD_MULTIPLE = 1;
    
    private boolean enableClientMetrics = true;
    
    /**
     * index(taskId)-> total cache count for this taskId.
     */
    private final List<AtomicInteger> taskIdCacheCountList = new ArrayList<>();
    
    /**
     * Add listeners for data.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     */
    public void addListeners(String dataId, String group, List<? extends Listener> listeners) throws NacosException {
        group = blank2defaultGroup(group);
        CacheData cache = addCacheDataIfAbsent(dataId, group);
        synchronized (cache) {
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            cache.setDiscard(false);
            cache.setConsistentWithServer(false);
            // make sure cache exists in cacheMap
            if (getCache(dataId, group) != cache) {
                putCache(GroupKey.getKey(dataId, group), cache);
            }
            agent.notifyListenConfig();
        }
    }
    
    /**
     * Add listeners for tenant.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners)
            throws NacosException {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        // 构建 CacheData 对象，标识唯一的配置项
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        synchronized (cache) {
            // 为缓存数据添加监听器
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            cache.setDiscard(false);
            cache.setConsistentWithServer(false);

            // 确保缓存存在于 cacheMap 中
            if (getCache(dataId, group, tenant) != cache) {
                putCache(GroupKey.getKeyTenant(dataId, group, tenant), cache);
            }
            // 关键：通知监听配置变更 - 这会触发长轮询检查
            agent.notifyListenConfig();
        }
        
    }
    
    /**
     * Add listeners for tenant with content.
     *
     * @param dataId           dataId of data
     * @param group            group of data
     * @param content          content
     * @param encryptedDataKey encryptedDataKey
     * @param listeners        listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListenersWithContent(String dataId, String group, String content, String encryptedDataKey,
            List<? extends Listener> listeners) throws NacosException {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        synchronized (cache) {
            cache.setEncryptedDataKey(encryptedDataKey);
            cache.setContent(content);
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            cache.setDiscard(false);
            cache.setConsistentWithServer(false);
            // make sure cache exists in cacheMap
            if (getCache(dataId, group, tenant) != cache) {
                putCache(GroupKey.getKeyTenant(dataId, group, tenant), cache);
            }
            agent.notifyListenConfig();
        }
        
    }
    
    /**
     * Remove listener.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeListener(String dataId, String group, Listener listener) {
        group = blank2defaultGroup(group);
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            synchronized (cache) {
                cache.removeListener(listener);
                if (cache.getListeners().isEmpty()) {
                    cache.setConsistentWithServer(false);
                    cache.setDiscard(true);
                    agent.removeCache(dataId, group);
                }
            }
            
        }
    }
    
    /**
     * Remove listeners for tenant.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            synchronized (cache) {
                cache.removeListener(listener);
                if (cache.getListeners().isEmpty()) {
                    cache.setConsistentWithServer(false);
                    cache.setDiscard(true);
                    agent.removeCache(dataId, group);
                }
            }
        }
    }
    
    /**
     * Adds a list of fuzzy listen listeners for the specified data ID pattern and group.
     *
     * @param dataIdPattern          The pattern of the data ID to listen for.
     * @param groupPattern           The group of the configuration.
     * @param fuzzyWatchEventWatcher The configFuzzyWatcher to add.
     * @throws NacosException If an error occurs while adding the listeners.
     */
    public ConfigFuzzyWatchContext addTenantFuzzyWatcher(String dataIdPattern, String groupPattern,
            FuzzyWatchEventWatcher fuzzyWatchEventWatcher) {
        return configFuzzyWatchGroupKeyHolder.registerFuzzyWatcher(dataIdPattern, groupPattern, fuzzyWatchEventWatcher);
    }
    
    /**
     * Removes a fuzzy listen listener for the specified data ID pattern, group, and listener.
     *
     * @param dataIdPattern The pattern of the data ID.
     * @param group         The group of the configuration.
     * @param watcher       The listener to remove.
     * @throws NacosException If an error occurs while removing the listener.
     */
    public void removeFuzzyListenListener(String dataIdPattern, String group, FuzzyWatchEventWatcher watcher) {
        configFuzzyWatchGroupKeyHolder.removeFuzzyWatcher(dataIdPattern, group, watcher);
    }
    
    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<>(cacheMap.get());
            CacheData remove = copy.remove(groupKey);
            if (remove != null) {
                decreaseTaskIdCount(remove.getTaskId());
            }
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);
        
        if (enableClientMetrics) {
            try {
                MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
            } catch (Throwable t) {
                LOGGER.error("Failed to update metrics for listen config count", t);
            }
        }
    }
    
    /**
     * remove config.
     *
     * @param dataId dataId.
     * @param group  group.
     * @param tenant tenant.
     * @param tag    tag.
     * @return success or not.
     * @throws NacosException exception to throw.
     */
    public boolean removeConfig(String dataId, String group, String tenant, String tag) throws NacosException {
        return agent.removeConfig(dataId, group, tenant, tag);
    }
    
    /**
     * publish config.
     *
     * @param dataId  dataId.
     * @param group   group.
     * @param tenant  tenant.
     * @param appName appName.
     * @param tag     tag.
     * @param betaIps betaIps.
     * @param content content.
     * @param casMd5  casMd5.
     * @param type    type.
     * @return success or not.
     * @throws NacosException exception throw.
     */
    public boolean publishConfig(String dataId, String group, String tenant, String appName, String tag, String betaIps,
            String content, String encryptedDataKey, String casMd5, String type) throws NacosException {
        return agent.publishConfig(dataId, group, tenant, appName, tag, betaIps, content, encryptedDataKey, casMd5,
                type);
    }
    
    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group) {
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            return cache;
        }
        
        String key = GroupKey.getKey(dataId, group);
        cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group);
        
        synchronized (cacheMap) {
            CacheData cacheFromMap = getCache(dataId, group);
            // multiple listeners on the same dataid+group and race condition,so double check again
            //other listener thread beat me to set to cacheMap
            if (null != cacheFromMap) {
                cache = cacheFromMap;
                //reset so that server not hang this check
                cache.setInitializing(true);
            } else {
                int taskId = calculateTaskId();
                increaseTaskIdCount(taskId);
                cache.setTaskId(taskId);
            }
            
            Map<String, CacheData> copy = new HashMap<>(cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }
        
        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);
        
        if (enableClientMetrics) {
            try {
                MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
            } catch (Throwable t) {
                LOGGER.error("Failed to update metrics for listen config count", t);
            }
        }
        
        return cache;
    }
    
    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @param tenant tenant of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            return cache;
        }
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            CacheData cacheFromMap = getCache(dataId, group, tenant);
            // multiple listeners on the same dataid+group and race condition,so
            // double check again
            // other listener thread beat me to set to cacheMap
            if (null != cacheFromMap) {
                cache = cacheFromMap;
                // reset so that server not hang this check
                cache.setInitializing(true);
            } else {
                cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
                int taskId = calculateTaskId();
                increaseTaskIdCount(taskId);
                cache.setTaskId(taskId);
                // fix issue # 1317
                if (enableRemoteSyncConfig) {
                    ConfigResponse response = getServerConfig(dataId, group, tenant, requestTimeout, false);
                    cache.setEncryptedDataKey(response.getEncryptedDataKey());
                    cache.setContent(response.getContent());
                }
            }
            
            Map<String, CacheData> copy = new HashMap<>(this.cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);
        
        if (enableClientMetrics) {
            try {
                MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
            } catch (Throwable t) {
                LOGGER.error("Failed to update metrics for listen config count", t);
            }
        }
        
        return cache;
    }
    
    /**
     * Put cache.
     *
     * @param key   groupKey
     * @param cache cache
     */
    private void putCache(String key, CacheData cache) {
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<>(this.cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }
    }
    
    private void increaseTaskIdCount(int taskId) {
        taskIdCacheCountList.get(taskId).incrementAndGet();
    }
    
    private void decreaseTaskIdCount(int taskId) {
        taskIdCacheCountList.get(taskId).decrementAndGet();
    }
    
    private int calculateTaskId() {
        int perTaskSize = (int) ParamUtil.getPerTaskConfigSize();
        for (int index = 0; index < taskIdCacheCountList.size(); index++) {
            if (taskIdCacheCountList.get(index).get() < perTaskSize) {
                return index;
            }
        }
        taskIdCacheCountList.add(new AtomicInteger(0));
        return taskIdCacheCountList.size() - 1;
    }
    
    public CacheData getCache(String dataId, String group) {
        return getCache(dataId, group, TenantUtil.getUserTenantForAcm());
    }
    
    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        return cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
    }
    
    public ConfigResponse getServerConfig(String dataId, String group, String tenant, long readTimeout, boolean notify)
            throws NacosException {
        if (StringUtils.isBlank(group)) {
            group = Constants.DEFAULT_GROUP;
        }
        return agent.queryConfig(dataId, group, tenant, readTimeout, notify);
    }
    
    private String blank2defaultGroup(String group) {
        return StringUtils.isBlank(group) ? Constants.DEFAULT_GROUP : group.trim();
    }
    
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public ClientWorker(final ConfigFilterChainManager configFilterChainManager,
            ConfigServerListManager serverListManager, final NacosClientProperties properties) throws NacosException {
        this.configFilterChainManager = configFilterChainManager;
        
        init(properties);
        
        // 创建RPC传输客户端 - 这是长轮询的核心组件
        agent = new ConfigRpcTransportClient(properties, serverListManager);

        // 创建模糊配置 ConfigFuzzyWatchNotifyEvent 和 ConfigFuzzyWatchLoadEvent 事件的监听器
        configFuzzyWatchGroupKeyHolder = new ConfigFuzzyWatchGroupKeyHolder(agent, uuid);
        // 创建调度线程池，用于执行长轮询任务
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(initWorkerThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.Worker"));
        agent.setExecutor(executorService);
        // 启动RPC客户端，在这里启动长轮询
        agent.start();
        // 启动模糊配置监听器
        configFuzzyWatchGroupKeyHolder.start();
    }
    
    void initAppLabels(Properties properties) {
        this.appLabels = ConnLabelsUtils.addPrefixForEachKey(defaultLabelsCollectorManager.getLabels(properties),
                APP_CONN_PREFIX);
    }
    
    private int initWorkerThreadCount(NacosClientProperties properties) {
        int count = ThreadUtils.getSuitableThreadCount(THREAD_MULTIPLE);
        if (properties == null) {
            return count;
        }
        count = Math.min(count, properties.getInteger(PropertyKeyConst.CLIENT_WORKER_MAX_THREAD_COUNT, count));
        count = Math.max(count, MIN_THREAD_NUM);
        return properties.getInteger(PropertyKeyConst.CLIENT_WORKER_THREAD_COUNT, count);
    }
    
    private void init(NacosClientProperties properties) {
        requestTimeout = ConvertUtils.toLong(properties.getProperty(PropertyKeyConst.CONFIG_REQUEST_TIMEOUT, "-1"));
        
        this.enableRemoteSyncConfig = Boolean.parseBoolean(
                properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
        this.enableClientMetrics = Boolean.parseBoolean(
                properties.getProperty(PropertyKeyConst.ENABLE_CLIENT_METRICS, "true"));
        initAppLabels(properties.getProperties(SourceType.PROPERTIES));
    }
    
    Map<String, Object> getMetrics(List<ClientConfigMetricRequest.MetricsKey> metricsKeys) {
        Map<String, Object> metric = new HashMap<>(16);
        metric.put("listenConfigSize", String.valueOf(this.cacheMap.get().size()));
        metric.put("clientVersion", VersionUtils.getFullClientVersion());
        metric.put("snapshotDir", LocalConfigInfoProcessor.LOCAL_SNAPSHOT_PATH);
        metric.put("addressUrl", agent.serverListManager.getAddressSource());
        metric.put("isFixedServer", agent.serverListManager.isFixed());
        metric.put("serverUrls", agent.serverListManager.getUrlString());
        
        Map<ClientConfigMetricRequest.MetricsKey, Object> metricValues = getMetricsValue(metricsKeys);
        metric.put("metricValues", metricValues);
        Map<String, Object> metrics = new HashMap<>(1);
        metrics.put(uuid, JacksonUtils.toJson(metric));
        return metrics;
    }
    
    private Map<ClientConfigMetricRequest.MetricsKey, Object> getMetricsValue(
            List<ClientConfigMetricRequest.MetricsKey> metricsKeys) {
        if (metricsKeys == null) {
            return null;
        }
        Map<ClientConfigMetricRequest.MetricsKey, Object> values = new HashMap<>(16);
        for (ClientConfigMetricRequest.MetricsKey metricsKey : metricsKeys) {
            if (ClientConfigMetricRequest.MetricsKey.CACHE_DATA.equals(metricsKey.getType())) {
                CacheData cacheData = cacheMap.get().get(metricsKey.getKey());
                values.putIfAbsent(metricsKey,
                        cacheData == null ? null : cacheData.getContent() + ":" + cacheData.getMd5());
            }
            if (ClientConfigMetricRequest.MetricsKey.SNAPSHOT_DATA.equals(metricsKey.getType())) {
                String[] configStr = GroupKey.parseKey(metricsKey.getKey());
                String snapshot = LocalConfigInfoProcessor.getSnapshot(agent.getName(), configStr[0], configStr[1],
                        configStr[2]);
                values.putIfAbsent(metricsKey,
                        snapshot == null ? null : snapshot + ":" + MD5Utils.md5Hex(snapshot, ENCODE));
            }
        }
        return values;
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        if (configFuzzyWatchGroupKeyHolder != null) {
            configFuzzyWatchGroupKeyHolder.shutdown();
            // help gc
            configFuzzyWatchGroupKeyHolder = null;
        }
        if (agent != null) {
            agent.shutdown();
        }
        LOGGER.info("{} do shutdown stop", className);
    }
    
    /**
     * check if it has any connectable server endpoint.
     *
     * @return true: that means has atleast one connected rpc client. flase: that means does not have any connected rpc
     * client.
     */
    public boolean isHealthServer() {
        return agent.isHealthServer();
    }
    
    public class ConfigRpcTransportClient extends ConfigTransportClient {
        
        Map<String, ExecutorService> multiTaskExecutor = new HashMap<>();
        
        private final BlockingQueue<Object> listenExecutebell = new ArrayBlockingQueue<>(1);
        
        private final Object bellItem = new Object();
        
        private long lastAllSyncTime = System.currentTimeMillis();
        
        Subscriber subscriber = null;
        
        /**
         * 3 minutes to check all listen cache keys.
         */
        private static final long ALL_SYNC_INTERNAL = 3 * 60 * 1000L;
        
        public ConfigRpcTransportClient(NacosClientProperties properties, ConfigServerListManager serverListManager) {
            super(properties, serverListManager);
        }
        
        private ConnectionType getConnectionType() {
            return ConnectionType.GRPC;
        }
        
        @Override
        public void shutdown() throws NacosException {
            super.shutdown();
            synchronized (RpcClientFactory.getAllClientEntries()) {
                LOGGER.info("Trying to shutdown transport client {}", this);
                Set<Map.Entry<String, RpcClient>> allClientEntries = RpcClientFactory.getAllClientEntries();
                Iterator<Map.Entry<String, RpcClient>> iterator = allClientEntries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, RpcClient> entry = iterator.next();
                    if (entry.getKey().startsWith(uuid)) {
                        LOGGER.info("Trying to shutdown rpc client {}", entry.getKey());
                        
                        try {
                            entry.getValue().shutdown();
                        } catch (NacosException nacosException) {
                            nacosException.printStackTrace();
                        }
                        LOGGER.info("Remove rpc client {}", entry.getKey());
                        iterator.remove();
                    }
                }
                
                LOGGER.info("Shutdown executor {}", agent.getExecutor());
                agent.getExecutor().shutdown();
                Map<String, CacheData> stringCacheDataMap = cacheMap.get();
                for (Map.Entry<String, CacheData> entry : stringCacheDataMap.entrySet()) {
                    entry.getValue().setConsistentWithServer(false);
                }
                if (subscriber != null) {
                    NotifyCenter.deregisterSubscriber(subscriber);
                }

                multiTaskExecutor.values().forEach((executor) -> {
                    if (executor != null && !executor.isShutdown()) {
                        LOGGER.info("Shutdown multi task executor {}", executor);
                        executor.shutdown();
                    }
                });
            }
            
        }
        
        private Map<String, String> getLabels() {
            
            Map<String, String> labels = new HashMap<>(2, 1);
            labels.put(RemoteConstants.LABEL_SOURCE, RemoteConstants.LABEL_SOURCE_SDK);
            labels.put(RemoteConstants.LABEL_MODULE, RemoteConstants.LABEL_MODULE_CONFIG);
            labels.put(Constants.APPNAME, AppNameUtils.getAppName());
            if (EnvUtil.getSelfVipserverTag() != null) {
                labels.put(Constants.VIPSERVER_TAG, EnvUtil.getSelfVipserverTag());
            }
            if (EnvUtil.getSelfAmoryTag() != null) {
                labels.put(Constants.AMORY_TAG, EnvUtil.getSelfAmoryTag());
            }
            if (EnvUtil.getSelfLocationTag() != null) {
                labels.put(Constants.LOCATION_TAG, EnvUtil.getSelfLocationTag());
            }
            
            labels.putAll(appLabels);
            return labels;
        }
        
        // 当服务端配置变更时，处理配置变更通知请求
        ConfigChangeNotifyResponse handleConfigChangeNotifyRequest(ConfigChangeNotifyRequest configChangeNotifyRequest,
                String clientName) {
            LOGGER.info("[{}] [server-push] config changed. dataId={}, group={},tenant={}", clientName,
                    configChangeNotifyRequest.getDataId(), configChangeNotifyRequest.getGroup(),
                    configChangeNotifyRequest.getTenant());
            // 构建配置唯一标识
            String groupKey = GroupKey.getKeyTenant(configChangeNotifyRequest.getDataId(),
                    configChangeNotifyRequest.getGroup(), configChangeNotifyRequest.getTenant());
            
            // 获取对应的缓存数据
            CacheData cacheData = cacheMap.get().get(groupKey);
            if (cacheData != null) {
                synchronized (cacheData) {
                    // 标记收到变更通知
                    cacheData.getReceiveNotifyChanged().set(true);
                    // 标记与服务端不一致
                    cacheData.setConsistentWithServer(false);
                    // 立即触发监听配置检查
                    notifyListenConfig();
                }
                
            }
            return new ConfigChangeNotifyResponse();
        }
        
        ClientConfigMetricResponse handleClientMetricsRequest(ClientConfigMetricRequest configMetricRequest) {
            ClientConfigMetricResponse response = new ClientConfigMetricResponse();
            response.setMetrics(getMetrics(configMetricRequest.getMetricsKeys()));
            return response;
        }
        
        @SuppressWarnings("PMD.MethodTooLongRule")
        private void initRpcClientHandler(final RpcClient rpcClientInner) {
            // Register Config Change /Config ReSync Handler
            // 注册 配置变更 或 配置重同步处理器
            rpcClientInner.registerServerRequestHandler((request, connection) -> {
                // config change notify
                if (request instanceof ConfigChangeNotifyRequest) {
                    return handleConfigChangeNotifyRequest((ConfigChangeNotifyRequest) request,
                            rpcClientInner.getName());
                }
                return null;
            });
            
            rpcClientInner.registerServerRequestHandler((request, connection) -> {
                if (request instanceof ClientConfigMetricRequest) {
                    return handleClientMetricsRequest((ClientConfigMetricRequest) request);
                }
                return null;
            });
            // 注册模糊监听处理器
            rpcClientInner.registerServerRequestHandler(
                    new ClientFuzzyWatchNotifyRequestHandler(configFuzzyWatchGroupKeyHolder));

            // 注册链接事件监听器
            rpcClientInner.registerConnectionListener(new ConnectionEventListener() {
                
                @Override
                public void onConnected(Connection connection) {
                    LOGGER.info("[{}] Connected,notify listen context...", rpcClientInner.getName());
                    notifyListenConfig();
                    
                    LOGGER.info("[{}] Connected,notify fuzzy listen context...", rpcClientInner.getName());
                    configFuzzyWatchGroupKeyHolder.notifyFuzzyWatchSync();
                }
                
                @Override
                public void onDisConnect(Connection connection) {
                    String taskId = rpcClientInner.getLabels().get("taskId");
                    LOGGER.info("[{}] DisConnected,reset listen context", rpcClientInner.getName());
                    Collection<CacheData> values = cacheMap.get().values();
                    
                    for (CacheData cacheData : values) {
                        if (StringUtils.isNotBlank(taskId)) {
                            if (Integer.valueOf(taskId).equals(cacheData.getTaskId())) {
                                cacheData.setConsistentWithServer(false);
                            }
                        } else {
                            cacheData.setConsistentWithServer(false);
                        }
                    }
                    
                    LOGGER.info("[{}] DisConnected,reset  fuzzy watch consistence status", rpcClientInner.getName());
                    configFuzzyWatchGroupKeyHolder.resetConsistenceStatus();
                }
                
            });
            
            rpcClientInner.serverListFactory(new ServerListFactory() {
                @Override
                public String genNextServer() {
                    return ConfigRpcTransportClient.super.serverListManager.genNextServer();
                    
                }
                
                @Override
                public String getCurrentServer() {
                    return ConfigRpcTransportClient.super.serverListManager.getCurrentServer();
                    
                }
                
                @Override
                public List<String> getServerList() {
                    return ConfigRpcTransportClient.super.serverListManager.getServerList();
                    
                }
            });

            // 注册订阅者
            subscriber = new Subscriber() {
                @Override
                public void onEvent(Event event) {
                    rpcClientInner.onServerListChange();
                }
                
                @Override
                public Class<? extends Event> subscribeType() {
                    return ServerListChangeEvent.class;
                }
            };
            NotifyCenter.registerSubscriber(subscriber);
        }
        
        @Override
        public void startInternal() {
            ScheduledExecutorService executor = getExecutor();
            executor.schedule(() -> {
                // 无限循环，直到执行器被关闭
                while (!executor.isShutdown() && !executor.isTerminated()) {
                    try {
                        // 阻塞等待通知，最多等待5秒
                        listenExecutebell.poll(5L, TimeUnit.SECONDS);
                        if (executor.isShutdown() || executor.isTerminated()) {
                            continue;
                        }
                        // 执行配置监听检查 - 这是长轮询的核心逻辑
                        executeConfigListen();
                    } catch (Throwable e) {
                        LOGGER.error("[rpc listen execute] [rpc listen] exception", e);
                        try {
                            Thread.sleep(50L);
                        } catch (InterruptedException interruptedException) {
                            //ignore
                        }
                        // 出现异常时重新通知监听配置
                        notifyListenConfig();
                    }
                }
            }, 0L, TimeUnit.MILLISECONDS);
            
        }
        
        @Override
        public String getName() {
            return serverListManager.getName();
        }
        
        // 当有新的监听器添加或配置变更时或发生异常时，都会调用这个方法，那么在 startInternal 中的循环就会被唤醒
        @Override
        public void notifyListenConfig() {
            // 向阻塞队列中放入一个元素，唤醒 startInternal 中的循环
            listenExecutebell.offer(bellItem);
        }
        
        @Override
        public void executeConfigListen() throws NacosException {
            // 按 taskId 分组监听缓存和移除监听缓存
            Map<String, List<CacheData>> listenCachesMap = new HashMap<>(16);
            Map<String, List<CacheData>> removeListenCachesMap = new HashMap<>(16);
            long now = System.currentTimeMillis();
            // 每3分钟进行一次全量同步检查
            boolean needAllSync = now - lastAllSyncTime >= ALL_SYNC_INTERNAL;
            // 遍历所有缓存配置，按状态分类
            for (CacheData cache : cacheMap.get().values()) {
                synchronized (cache) {
                    // 检查本地配置（故障转移文件）
                    checkLocalConfig(cache);
                    
                    // 如果与服务端一致且不需要全量同步，跳过
                    if (cache.isConsistentWithServer()) {
                        cache.checkListenerMd5();
                        if (!needAllSync) {
                            continue;
                        }
                    }
                    
                    // 如果使用本地配置信息，跳过处理
                    if (cache.isUseLocalConfigInfo()) {
                        continue;
                    }
                    
                    // 根据缓存状态分类处理
                    if (!cache.isDiscard()) {
                        // 需要监听的配置
                        List<CacheData> cacheDatas = listenCachesMap.computeIfAbsent(String.valueOf(cache.getTaskId()),
                                k -> new LinkedList<>());
                        cacheDatas.add(cache);
                    } else {
                        // 需要移除监听的配置
                        List<CacheData> cacheDatas = removeListenCachesMap.computeIfAbsent(
                                String.valueOf(cache.getTaskId()), k -> new LinkedList<>());
                        cacheDatas.add(cache);
                    }
                }
                
            }
            
            // 执行监听检查，返回是否有变更
            boolean hasChangedKeys = checkListenCache(listenCachesMap);
            
            // 执行移除 discard 的配置
            checkRemoveListenCache(removeListenCachesMap);
            
            if (needAllSync) {
                lastAllSyncTime = now;
            }

            // 如果有变更，重新通知监听配置（形成循环），再立即处理一遍上述逻辑
            if (hasChangedKeys) {
                notifyListenConfig();
            }
            
        }
        
        /**
         * Checks and handles local configuration for a given CacheData object. This method evaluates the use of
         * failover files for local configuration storage and updates the CacheData accordingly.
         * 用于处理本地配置故障转移（failover）机制，用于 Nacos 的高可用场景，当客户端无法连接到 Nacos 服务器时，可以通过本地故障转移文件继续提供配置服务，确保应用程序的正常运行
         *
         * @param cacheData The CacheData object to be processed.
         */
        public void checkLocalConfig(CacheData cacheData) {
            final String dataId = cacheData.dataId;
            final String group = cacheData.group;
            final String tenant = cacheData.tenant;
            final String envName = cacheData.envName;
            
            // Check if a failover file exists for the specified dataId, group, and tenant.
            // 校验故障转移文件是否存在
            File file = LocalConfigInfoProcessor.getFailoverFile(envName, dataId, group, tenant);
            
            // If not using local config info and a failover file exists, load and use it.
            // 如果未使用本地配置信息且故障转移文件存在，加载并使用它。
            if (!cacheData.isUseLocalConfigInfo() && file.exists()) {
                String content = LocalConfigInfoProcessor.getFailover(envName, dataId, group, tenant);
                final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                // 变更为使用本地配置
                cacheData.setUseLocalConfigInfo(true);
                cacheData.setLocalConfigInfoVersion(file.lastModified());
                cacheData.setContent(content);
                LOGGER.warn("[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}",
                        envName, dataId, group, tenant, md5);
                return;
            }
            
            // If use local config info, but the failover file is deleted, switch back to server config.
            // 如果使用本地配置信息，但故障转移文件被删除，则切换回服务器配置
            if (cacheData.isUseLocalConfigInfo() && !file.exists()) {
                cacheData.setUseLocalConfigInfo(false);
                LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", envName,
                        dataId, group, tenant);
                return;
            }
            
            // When the failover file content changes, indicating a change in local configuration.
            // 使用本地配置，文件存在且文件修改时间发生变更，表示本地配置发生了变化，需要更新本地配置信息
            if (cacheData.isUseLocalConfigInfo() && file.exists()
                    && cacheData.getLocalConfigInfoVersion() != file.lastModified()) {
                String content = LocalConfigInfoProcessor.getFailover(envName, dataId, group, tenant);
                final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                cacheData.setUseLocalConfigInfo(true);
                cacheData.setLocalConfigInfoVersion(file.lastModified());
                cacheData.setContent(content);
                LOGGER.warn("[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}",
                        envName, dataId, group, tenant, md5);
            }
        }
        
        private ExecutorService ensureSyncExecutor(String taskId) {
            if (!multiTaskExecutor.containsKey(taskId)) {
                multiTaskExecutor.put(taskId,
                        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
                            Thread thread = new Thread(r, "nacos.client.config.listener.task-" + taskId);
                            thread.setDaemon(true);
                            return thread;
                        }));
            }
            return multiTaskExecutor.get(taskId);
        }
        
        private void refreshContentAndCheck(RpcClient rpcClient, String groupKey, boolean notify) {
            if (cacheMap.get() != null && cacheMap.get().containsKey(groupKey)) {
                CacheData cache = cacheMap.get().get(groupKey);
                refreshContentAndCheck(rpcClient, cache, notify);
            }
        }
        
        private void refreshContentAndCheck(RpcClient rpcClient, CacheData cacheData, boolean notify) {
            try {
                // 查询配置信息
                ConfigResponse response = this.queryConfigInner(rpcClient, cacheData.dataId, cacheData.group,
                        cacheData.tenant, requestTimeout, notify);
                cacheData.setEncryptedDataKey(response.getEncryptedDataKey());
                cacheData.setContent(response.getContent());
                if (null != response.getConfigType()) {
                    cacheData.setType(response.getConfigType());
                }
                if (notify) {
                    LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, type={}", agent.getName(),
                            cacheData.dataId, cacheData.group, cacheData.tenant, cacheData.getMd5(),
                            response.getConfigType());
                }
                // 检验并通知
                cacheData.checkListenerMd5();
            } catch (Exception e) {
                LOGGER.error("refresh content and check md5 fail ,dataId={},group={},tenant={} ", cacheData.dataId,
                        cacheData.group, cacheData.tenant, e);
            }
        }
        
        private void checkRemoveListenCache(Map<String, List<CacheData>> removeListenCachesMap) throws NacosException {
            if (!removeListenCachesMap.isEmpty()) {
                List<Future> listenFutures = new ArrayList<>();
                
                for (Map.Entry<String, List<CacheData>> entry : removeListenCachesMap.entrySet()) {
                    String taskId = entry.getKey();
                    RpcClient rpcClient = ensureRpcClient(taskId);
                    
                    ExecutorService executorService = ensureSyncExecutor(taskId);
                    Future future = executorService.submit(() -> {
                        List<CacheData> removeListenCaches = entry.getValue();
                        ConfigBatchListenRequest configChangeListenRequest = buildConfigRequest(removeListenCaches);
                        configChangeListenRequest.setListen(false);
                        try {
                            // cancel listen
                            boolean removeSuccess = unListenConfigChange(rpcClient, configChangeListenRequest);
                            if (removeSuccess) {
                                for (CacheData cacheData : removeListenCaches) {
                                    synchronized (cacheData) {
                                        if (cacheData.isDiscard() && cacheData.getListeners().isEmpty()) {
                                            ClientWorker.this.removeCache(cacheData.dataId, cacheData.group,
                                                    cacheData.tenant);
                                        }
                                    }
                                }
                            }
                            
                        } catch (Throwable e) {
                            LOGGER.error("Async remove listen config change error ", e);
                            try {
                                Thread.sleep(50L);
                            } catch (InterruptedException interruptedException) {
                                //ignore
                            }
                            // 发生异常的话立即重试
                            notifyListenConfig();
                        }
                    });
                    listenFutures.add(future);
                }
                for (Future future : listenFutures) {
                    try {
                        future.get();
                    } catch (Throwable throwable) {
                        LOGGER.error("Async remove listen config change error ", throwable);
                    }
                }
            }
        }
        
        @SuppressWarnings("PMD.MethodTooLongRule")
        private boolean checkListenCache(Map<String, List<CacheData>> listenCachesMap) throws NacosException {
            // 使用原子布尔值记录是否有配置发生变更，保证线程安全
            final AtomicBoolean hasChangedKeys = new AtomicBoolean(false);

            // 如果没有需要监听的缓存，直接返回false
            if (!listenCachesMap.isEmpty()) {
                List<Future> listenFutures = new ArrayList<>();
                for (Map.Entry<String, List<CacheData>> entry : listenCachesMap.entrySet()) {
                    String taskId = entry.getKey();
                    // 为每个 taskId 都创建一个 RpcClient 客户端
                    RpcClient rpcClient = ensureRpcClient(taskId);
                    // 每个 taskId 专门分配一个线程数为 1 的线程池
                    ExecutorService executorService = ensureSyncExecutor(taskId);
                    Future future = executorService.submit(() -> {
                        List<CacheData> listenCaches = entry.getValue();
                        // 重置通知变更标识
                        for (CacheData cacheData : listenCaches) {
                            cacheData.getReceiveNotifyChanged().set(false);
                        }
                        // 将多个配置的监听请求合并为一个批量请求，提高网络效率
                        // 这是真正的长轮询网络请求
                        ConfigBatchListenRequest configChangeListenRequest = buildConfigRequest(listenCaches);
                        configChangeListenRequest.setListen(true);
                        try {
                            // 向 Nacos 服务端发送批量监听请求，检查配置是否有变更
                            ConfigChangeBatchListenResponse listenResponse = (ConfigChangeBatchListenResponse) requestProxy(
                                    rpcClient, configChangeListenRequest);
                            if (listenResponse != null && listenResponse.isSuccess()) {
                                Set<String> changeKeys = new HashSet<>();
                                
                                List<ConfigChangeBatchListenResponse.ConfigContext> changedConfigs = listenResponse.getChangedConfigs();
                                // 获取服务端返回的变更配置列表，并通知监听者
                                if (!CollectionUtils.isEmpty(changedConfigs)) {
                                    hasChangedKeys.set(true);
                                    for (ConfigChangeBatchListenResponse.ConfigContext changeConfig : changedConfigs) {
                                        // 构建配置的唯一标识key：dataId+group+tenant
                                        String changeKey = GroupKey.getKeyTenant(changeConfig.getDataId(),
                                                changeConfig.getGroup(), changeConfig.getTenant());
                                        changeKeys.add(changeKey);
                                        // 检查配置是否处于初始化状态，初始化状态的配置不需要通知监听器，避免重复通知
                                        boolean isInitializing = cacheMap.get().get(changeKey).isInitializing();
                                        // 刷新配置内容并检查MD5，触发监听器回调
                                        refreshContentAndCheck(rpcClient, changeKey, !isInitializing);
                                    }
                                    
                                }

                                // 某些配置可能通过服务端主动推送得到变更通知，但不在 changedConfigs 列表中
                                for (CacheData cacheData : listenCaches) {
                                    if (cacheData.getReceiveNotifyChanged().get()) {
                                        String changeKey = GroupKey.getKeyTenant(cacheData.dataId, cacheData.group,
                                                cacheData.getTenant());
                                        if (!changeKeys.contains(changeKey)) {
                                            boolean isInitializing = cacheMap.get().get(changeKey).isInitializing();
                                            refreshContentAndCheck(rpcClient, changeKey, !isInitializing);
                                        }
                                    }
                                }

                                // 处理未变更的配置，标记为与服务端一致
                                for (CacheData cacheData : listenCaches) {
                                    cacheData.setInitializing(false);
                                    String groupKey = GroupKey.getKeyTenant(cacheData.dataId, cacheData.group,
                                            cacheData.getTenant());
                                    if (!changeKeys.contains(groupKey)) {
                                        synchronized (cacheData) {
                                            if (!cacheData.getReceiveNotifyChanged().get()) {
                                                cacheData.setConsistentWithServer(true);
                                            }
                                        }
                                    }
                                }
                                
                            }
                        } catch (Throwable e) {
                            // 发生异常的话进行重试
                            LOGGER.error("Execute listen config change error ", e);
                            try {
                                Thread.sleep(50L);
                            } catch (InterruptedException interruptedException) {
                                //ignore
                            }
                            // 重新触发监听检查
                            notifyListenConfig();
                        }
                    });
                    // 将异步任务添加到Future列表中
                    listenFutures.add(future);
                }
                // 阻塞等待任务完成
                for (Future future : listenFutures) {
                    try {
                        future.get();
                    } catch (Throwable throwable) {
                        LOGGER.error("Async listen config change error ", throwable);
                    }
                }
                
            }
            return hasChangedKeys.get();
        }
        
        RpcClient ensureRpcClient(String taskId) throws NacosException {
            synchronized (ClientWorker.this) {
                Map<String, String> labels = getLabels();
                Map<String, String> newLabels = new HashMap<>(labels);
                newLabels.put("taskId", taskId);
                GrpcClientConfig grpcClientConfig = RpcClientConfigFactory.getInstance()
                        .createGrpcClientConfig(properties, newLabels);
                // 新的创建旧的直接返回
                RpcClient rpcClient = RpcClientFactory.createClient(uuid + "_config-" + taskId, getConnectionType(),
                        grpcClientConfig);
                if (rpcClient.isWaitInitiated()) {
                    // 初始化 RPC Client 处理器，包括配置变更和模糊通知处理器等
                    initRpcClientHandler(rpcClient);
                    rpcClient.setTenant(getTenant());
                    rpcClient.start();
                }
                
                return rpcClient;
            }
            
        }
        
        /**
         * build config string.
         *
         * @param caches caches to build config string.
         * @return request.
         */
        private ConfigBatchListenRequest buildConfigRequest(List<CacheData> caches) {
            ConfigBatchListenRequest configChangeListenRequest = new ConfigBatchListenRequest();
            for (CacheData cacheData : caches) {
                configChangeListenRequest.addConfigListenContext(cacheData.group, cacheData.dataId, cacheData.tenant,
                        cacheData.getMd5());
            }
            return configChangeListenRequest;
        }
        
        @Override
        public void removeCache(String dataId, String group) {
            // Notify to rpc un listen ,and remove cache if success.
            notifyListenConfig();
        }
        
        /**
         * send cancel listen config change request .
         *
         * @param configChangeListenRequest request of remove listen config string.
         */
        private boolean unListenConfigChange(RpcClient rpcClient, ConfigBatchListenRequest configChangeListenRequest)
                throws NacosException {
            
            ConfigChangeBatchListenResponse response = (ConfigChangeBatchListenResponse) requestProxy(rpcClient,
                    configChangeListenRequest);
            return response.isSuccess();
        }
        
        @Override
        public ConfigResponse queryConfig(String dataId, String group, String tenant, long readTimeouts, boolean notify)
                throws NacosException {
            RpcClient rpcClient = getOneRunningClient();
            if (notify) {
                CacheData cacheData = cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
                if (cacheData != null) {
                    rpcClient = ensureRpcClient(String.valueOf(cacheData.getTaskId()));
                }
            }
            
            return queryConfigInner(rpcClient, dataId, group, tenant, readTimeouts, notify);
            
        }
        
        ConfigResponse queryConfigInner(RpcClient rpcClient, String dataId, String group, String tenant,
                long readTimeouts, boolean notify) throws NacosException {
            ConfigQueryRequest request = ConfigQueryRequest.build(dataId, group, tenant);
            request.putHeader(NOTIFY_HEADER, String.valueOf(notify));
            
            ConfigQueryResponse response = (ConfigQueryResponse) requestProxy(rpcClient, request, readTimeouts);
            
            ConfigResponse configResponse = new ConfigResponse();
            if (response.isSuccess()) {
                LocalConfigInfoProcessor.saveSnapshot(this.getName(), dataId, group, tenant, response.getContent());
                configResponse.setContent(response.getContent());
                String configType;
                if (StringUtils.isNotBlank(response.getContentType())) {
                    configType = response.getContentType();
                } else {
                    configType = ConfigType.TEXT.getType();
                }
                configResponse.setConfigType(configType);
                String encryptedDataKey = response.getEncryptedDataKey();
                LocalEncryptedDataKeyProcessor.saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant,
                        encryptedDataKey);
                configResponse.setEncryptedDataKey(encryptedDataKey);
                return configResponse;
            } else if (response.getErrorCode() == ConfigQueryResponse.CONFIG_NOT_FOUND) {
                LocalConfigInfoProcessor.saveSnapshot(this.getName(), dataId, group, tenant, null);
                LocalEncryptedDataKeyProcessor.saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant, null);
                return configResponse;
            } else if (response.getErrorCode() == ConfigQueryResponse.CONFIG_QUERY_CONFLICT) {
                LOGGER.error(
                        "[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, "
                                + "tenant={}", this.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT,
                        "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            } else {
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", this.getName(), dataId,
                        group, tenant, response);
                throw new NacosException(response.getErrorCode(),
                        "http error, code=" + response.getErrorCode() + ",msg=" + response.getMessage() + ",dataId="
                                + dataId + ",group=" + group + ",tenant=" + tenant);
                
            }
        }
        
        Response requestProxy(RpcClient rpcClientInner, Request request) throws NacosException {
            return requestProxy(rpcClientInner, request, requestTimeout);
        }
        
        private Response requestProxy(RpcClient rpcClientInner, Request request, long timeoutMills)
                throws NacosException {
            try {
                request.putAllHeader(super.getSecurityHeaders(resourceBuild(request)));
                request.putAllHeader(super.getCommonHeader());
            } catch (Exception e) {
                throw new NacosException(NacosException.CLIENT_INVALID_PARAM, e);
            }
            JsonObject asJsonObjectTemp = new Gson().toJsonTree(request).getAsJsonObject();
            asJsonObjectTemp.remove("headers");
            asJsonObjectTemp.remove("requestId");
            boolean limit = Limiter.isLimit(request.getClass() + asJsonObjectTemp.toString());
            if (limit) {
                throw new NacosException(NacosException.CLIENT_OVER_THRESHOLD,
                        "More than client-side current limit threshold");
            }
            Response response;
            if (timeoutMills < 0) {
                response = rpcClientInner.request(request);
            } else {
                response = rpcClientInner.request(request, timeoutMills);
            }
            // If the 403 login operation is triggered, refresh the accessToken of the client
            if (response.getErrorCode() == ConfigQueryResponse.NO_RIGHT) {
                reLogin();
            }
            return response;
        }
        
        private RequestResource resourceBuild(Request request) {
            if (request instanceof ConfigQueryRequest) {
                String tenant = ((ConfigQueryRequest) request).getTenant();
                String group = ((ConfigQueryRequest) request).getGroup();
                String dataId = ((ConfigQueryRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }
            if (request instanceof ConfigPublishRequest) {
                String tenant = ((ConfigPublishRequest) request).getTenant();
                String group = ((ConfigPublishRequest) request).getGroup();
                String dataId = ((ConfigPublishRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }
            
            if (request instanceof ConfigRemoveRequest) {
                String tenant = ((ConfigRemoveRequest) request).getTenant();
                String group = ((ConfigRemoveRequest) request).getGroup();
                String dataId = ((ConfigRemoveRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }
            return RequestResource.configBuilder().build();
        }
        
        RpcClient getOneRunningClient() throws NacosException {
            return ensureRpcClient("0");
        }
        
        @Override
        public boolean publishConfig(String dataId, String group, String tenant, String appName, String tag,
                String betaIps, String content, String encryptedDataKey, String casMd5, String type)
                throws NacosException {
            try {
                ConfigPublishRequest request = new ConfigPublishRequest(dataId, group, tenant, content);
                request.setCasMd5(casMd5);
                request.putAdditionalParam(TAG_PARAM, tag);
                request.putAdditionalParam(APP_NAME_PARAM, appName);
                request.putAdditionalParam(BETAIPS_PARAM, betaIps);
                request.putAdditionalParam(TYPE_PARAM, type);
                request.putAdditionalParam(ENCRYPTED_DATA_KEY_PARAM, encryptedDataKey == null ? "" : encryptedDataKey);
                ConfigPublishResponse response = (ConfigPublishResponse) requestProxy(getOneRunningClient(), request);
                if (!response.isSuccess()) {
                    LOGGER.warn("[{}] [publish-single] fail, dataId={}, group={}, tenant={}, code={}, msg={}",
                            this.getName(), dataId, group, tenant, response.getErrorCode(), response.getMessage());
                    return false;
                } else {
                    LOGGER.info("[{}] [publish-single] ok, dataId={}, group={}, tenant={}", getName(), dataId, group,
                            tenant);
                    return true;
                }
            } catch (Exception e) {
                LOGGER.warn("[{}] [publish-single] error, dataId={}, group={}, tenant={}, code={}, msg={}",
                        this.getName(), dataId, group, tenant, "unknown", e.getMessage());
                return false;
            }
        }
        
        @Override
        public boolean removeConfig(String dataId, String group, String tenant, String tag) throws NacosException {
            ConfigRemoveRequest request = new ConfigRemoveRequest(dataId, group, tenant, tag);
            ConfigRemoveResponse response = (ConfigRemoveResponse) requestProxy(getOneRunningClient(), request);
            return response.isSuccess();
        }
        
        /**
         * check server is health.
         *
         * @return
         */
        public boolean isHealthServer() {
            try {
                return getOneRunningClient().isRunning();
            } catch (NacosException e) {
                LOGGER.warn("check server status failed.", e);
                return false;
            }
        }
        
        /**
         * Determine whether nacos-server supports the capability.
         *
         * @param abilityKey ability key
         * @return true if supported, otherwise false
         */
        public boolean isAbilitySupportedByServer(AbilityKey abilityKey) {
            try {
                return getOneRunningClient().getConnectionAbility(abilityKey) == AbilityStatus.SUPPORTED;
            } catch (NacosException e) {
                throw new NacosRuntimeException(e.getErrCode(), "Get Running Client failed: ", e);
            }
        }
    }
    
    public String getAgentName() {
        return agent.getName();
    }
    
    public ConfigTransportClient getAgent() {
        return agent;
    }
    
}
