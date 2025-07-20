
- 启动类：com.alibaba.nacos.bootstrap.NacosBootstrap

### 单机启动

1. 添加 VM 参数：-Dnacos.standalone=true
2. 创建表：distribution/conf/mysql-schema.sql
3. 生效的日志配置文件：core/src/main/resources/META-INF/logback/nacos.xml

---

### module

- nacos-console: 控制台相关内容

---

### 长轮训

客户端启动
    |
    |---> ClientWorker 构造函数
    |     |---> 创建 ConfigRpcTransportClient
    |     |---> 启动调度线程池
    |     |---> agent.start() 
    |           |---> startInternal()
    |                 |---> 启动无限循环等待任务
    |
用户调用 addListener
    |
    |---> addTenantListeners()
    |     |---> 添加监听器到 CacheData
    |     |---> agent.notifyListenConfig()
    |           |---> listenExecutebell.offer(bellItem)
    |
startInternal 中的循环被唤醒
    |
    |---> executeConfigListen()
    |     |---> 收集所有需要监听的配置
    |     |---> checkListenCache()
    |           |---> 构建批量监听请求
    |           |---> 发送RPC请求到服务端
    |           |---> 等待服务端响应（长轮询）
    |
服务端配置变更
    |
    |---> 主动推送通知到客户端
    |     |---> handleConfigChangeNotifyRequest()
    |           |---> 标记配置变更
    |           |---> notifyListenConfig() (立即触发检查)
    |
    |---> 或批量监听请求返回变更列表
    |
refreshContentAndCheck()
    |
    |---> 获取最新配置内容
    |---> 更新本地缓存
    |---> 触发监听器回调
    |
继续下一轮循环...

Nacos 长轮询机制的核心特性：

事件驱动：通过 ArrayBlockingQueue 实现事件通知机制
批量处理：将多个配置监听请求合并为批量请求
任务分片：通过 taskId 将配置分组，每组使用独立的 RPC 客户端和线程池
双重通知：支持长轮询响应和服务端主动推送两种通知方式
故障转移：支持本地配置文件作为故障转移方案
异步处理：所有网络请求都通过异步任务执行
这种设计既保证了实时性，又保证了高性能和可扩展性。

### gRpc 双向流通讯

客户端                                服务端
|                                    |
|---> connectToServer()             |
|     |---> createManagedChannel()  |
|     |---> serverCheck() --------->|---> 健康检查响应
|     |<--- ServerCheckResponse ----|
|     |---> bindRequestStream()     |
|           |---> streamStub.requestBiStream()
|                 |                 |
|                 |<----------------|---> 建立双向流连接
|                 |                 |
|     |---> sendRequest(ConnectionSetupRequest)
|           |-------------------->  |---> 注册连接
|           |<--------------------  |<--- SetupAckRequest
|                 |                 |
连接建立完成，双向流保持活跃状态          |
|                 |                 |
配置变更事件 =========================> |
|                 |                 |---> RpcConfigChangeNotifier.onEvent()
|                 |                 |---> connection.asyncRequest(ConfigChangeNotifyRequest)
|                 |                 |
|<--- ConfigChangeNotifyRequest ----|     通过双向流推送
|     |                             |
|     |---> handleServerRequest()   |
|     |---> 标记配置变更             |
|     |---> notifyListenConfig()    |
|     |                             |
|---> ConfigChangeNotifyResponse -->|     确认收到通知
|                                   |
继续监听服务端推送...                   |