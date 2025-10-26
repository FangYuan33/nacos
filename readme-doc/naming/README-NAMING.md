## Nacos 源码深度畅游：注册核心流程详解

- [registerInstance]


以如下源码来作为注册服务实例的入口来验证向 Nacos 注册中心注册服务实例的逻辑：

```java
public class TestNaming {
    @Test
    void testNacosNamingService() throws InterruptedException, NacosException {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, "127.0.0.1:8848");
        properties.put(PropertyKeyConst.NAMESPACE, "public");
        NamingService namingService = NacosFactory.createNamingService(properties);

        try {
            // 注册一个服务实例
            namingService.registerInstance("test-service", "127.0.0.1", 8080);

            // 添加事件监听器
            namingService.subscribe("test-service", event -> System.out.println("服务实例变化: " + event));
        } catch (Exception e) {
            System.out.println("服务注册失败(预期，因为服务器可能未启动): " + e.getMessage());
        }

        TimeUnit.HOURS.sleep(5);

        namingService.shutDown();
    }
}
```

最先它会执行 `NamingService#registerInstance` 方法：

```java
public class NacosNamingService implements NamingService {

    private NamingClientProxy clientProxy;
    
    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        // 默认权重值为 1
        instance.setWeight(1.0);
        instance.setClusterName(clusterName);
        registerInstance(serviceName, groupName, instance);
    }

    @Override
    public void registerInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        // 参数校验
        NamingUtils.checkInstanceIsLegal(instance);
        checkAndStripGroupNamePrefix(instance, groupName);
        // 在这里实际上使用了静态代理模式来区分是使用 HTTPClient 还是 GrpcClient，默认为后者
        clientProxy.registerService(serviceName, groupName, instance);
    }
}
```

实际执行注册的为 `NamingGrpcClientProxy` 实现类，在向注册中心注册服务的逻辑中，我们 **只关注创建临时（Ephemeral）服务实例** 的逻辑：

```java
public class NamingGrpcClientProxy extends AbstractNamingClientProxy {

    private final NamingGrpcRedoService redoService;
    
    @Override
    public void registerService(String serviceName, String groupName, Instance instance) throws NacosException {
        NAMING_LOGGER.info("[REGISTER-SERVICE] {} registering service {} with instance {}", namespaceId, serviceName,
                instance);
        // [registerInstance] 步骤1：创建服务实例区分是否为临时
        if (instance.isEphemeral()) { 
            registerServiceForEphemeral(serviceName, groupName, instance);
        } else {
            doRegisterServiceForPersistent(serviceName, groupName, instance);
        }
    }

    private void registerServiceForEphemeral(String serviceName, String groupName, Instance instance)
            throws NacosException {
        redoService.cacheInstanceForRedo(serviceName, groupName, instance);
        doRegisterService(serviceName, groupName, instance);
    }

    public void doRegisterService(String serviceName, String groupName, Instance instance) throws NacosException {
        // 客户端创建注册实例请求对象，包含命名空间、服务名、分组名和实例信息
        InstanceRequest request = new InstanceRequest(namespaceId, serviceName, groupName,
                NamingRemoteConstants.REGISTER_INSTANCE, instance);
        // [registerInstance] 步骤2：通过gRPC协议向服务端发送注册请求
        requestToServer(request, Response.class);
        redoService.instanceRegistered(serviceName, groupName);
    }
}
```

在上述步骤中，可以发现分别两次调用了 `redoServer` 的 `cacheInstanceForRedo` 和 `instanceRegistered` 方法：

```java
public class NamingGrpcRedoService implements ConnectionEventListener {

    private final ConcurrentMap<String, InstanceRedoData> registeredInstances = new ConcurrentHashMap<>();
    
    // 创建 InstanceRedoData 对象在 ConcurrentMap 中
    public void cacheInstanceForRedo(String serviceName, String groupName, Instance instance) {
        // eg: DEFAULT_GROUP@@test-service
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        InstanceRedoData redoData = InstanceRedoData.build(serviceName, groupName, instance);
        synchronized (registeredInstances) {
            registeredInstances.put(key, redoData);
        }
    }
}
```

首先它会创建 `InstanceRedoData` 对象保存在 `ConcurrentMap` 中，初始字段值如下：

![img.png](img.png)

在成功调用向服务端注册实例后，会将 `InstanceRedoData#registered` 字段标记为 true：

```java
public class NamingGrpcRedoService implements ConnectionEventListener {

    private final ConcurrentMap<String, InstanceRedoData> registeredInstances = new ConcurrentHashMap<>();
    
    public void instanceRegistered(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                // 标记 registered 字段为 true
                redoData.registered();
            }
        }
    }
}
```

至于 `InstanceRedoData` 对象有什么作用我们之后再看，我们还是先回到 gRPC 请求服务端注册实例的逻辑中。Nacos Client 会向服务端发送 `InstanceRequest` 请求，并有 Nacos Server 端的 `InstanceRequestHandler` 承接：

```java
@Component
public class InstanceRequestHandler extends RequestHandler<InstanceRequest, InstanceResponse> {

    private final EphemeralClientOperationServiceImpl clientOperationService;

    public InstanceRequestHandler(EphemeralClientOperationServiceImpl clientOperationService) {
        this.clientOperationService = clientOperationService;
    }

    @Override
    @NamespaceValidation
    @TpsControl(pointName = "RemoteNamingInstanceRegisterDeregister", name = "RemoteNamingInstanceRegisterDeregister")
    @Secured(action = ActionTypes.WRITE)
    @ExtractorManager.Extractor(rpcExtractor = InstanceRequestParamExtractor.class)
    public InstanceResponse handle(InstanceRequest request, RequestMeta meta) throws NacosException {
        // [registerInstance] 步骤3：根据请求参数创建服务对象，设置为ephemeral（临时）服务
        Service service = Service.newService(request.getNamespace(), request.getGroupName(), request.getServiceName(),
                true);
        InstanceUtil.setInstanceIdIfEmpty(request.getInstance(), service.getGroupedServiceName());
        switch (request.getType()) {
            case NamingRemoteConstants.REGISTER_INSTANCE:
                // 根据请求类型分发到具体的注册实例方法
                return registerInstance(service, request, meta);
            case NamingRemoteConstants.DE_REGISTER_INSTANCE:
                return deregisterInstance(service, request, meta);
            default:
                throw new NacosException(NacosException.INVALID_PARAM,
                        String.format("Unsupported request type %s", request.getType()));
        }
    }

    private InstanceResponse registerInstance(Service service, InstanceRequest request, RequestMeta meta)
            throws NacosException {
        // 调用客户端操作服务注册实例，传入服务、实例和连接ID
        clientOperationService.registerInstance(service, request.getInstance(), meta.getConnectionId());
        // 发布实例注册跟踪事件，记录注册操作的详细信息
        NotifyCenter.publishEvent(new RegisterInstanceTraceEvent(System.currentTimeMillis(),
                NamingRequestUtil.getSourceIpForGrpcRequest(meta), true, service.getNamespace(), service.getGroup(),
                service.getName(), request.getInstance().getIp(), request.getInstance().getPort()));
        return new InstanceResponse(NamingRemoteConstants.REGISTER_INSTANCE);
    }
}
```

它会创建 `Service` 对象，如下所示：

![img_1.png](img_1.png)

接下来我们先深入到其中调用的 `EphemeralClientOperationServiceImpl#registerInstance` 方法中：

```java
public class EphemeralClientOperationServiceImpl implements ClientOperationService {
    @Override
    public void registerInstance(Service service, Instance instance, String clientId) throws NacosException {
        // 验证实例的合法性（IP、端口等）
        NamingUtils.checkInstanceIsLegal(instance);

        // [registerInstance] 步骤4：从服务管理器获取单例服务对象
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        if (!singleton.isEphemeral()) {
            throw new NacosRuntimeException(NacosException.INVALID_PARAM,
                    String.format("Current service %s is persistent service, can't register ephemeral instance.",
                            singleton.getGroupedServiceName()));
        }
        // 获取客户端连接对象并验证其合法性
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        // 将实例信息转换为发布信息对象
        InstancePublishInfo instanceInfo = getPublishInfo(instance);
        // 将实例添加到客户端的服务实例列表中
        client.addServiceInstance(singleton, instanceInfo);
        client.setLastUpdatedTime();
        client.recalculateRevision();
        // 发布客户端注册服务事件，通知其他组件
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId));
        // 发布实例元数据事件，完成注册流程
        NotifyCenter
                .publishEvent(new MetadataEvent.InstanceMetadataEvent(singleton, instanceInfo.getMetadataId(), false));
    }
}

public class ServiceManager {

    // 单例模式：饿汉式
    private static final ServiceManager INSTANCE = new ServiceManager();
    
    public static ServiceManager getInstance() {
        return INSTANCE;
    }


    private final ConcurrentHashMap<Service, Service> singletonRepository;

    private final ConcurrentHashMap<String, Set<Service>> namespaceSingletonMaps;

    private ServiceManager() {
        singletonRepository = new ConcurrentHashMap<>(1 << 10);
        namespaceSingletonMaps = new ConcurrentHashMap<>(1 << 2);
    }
    
    public Service getSingleton(Service service) {
        // 首先在 singletonRepository 中查找或创建服务单例
        Service result = singletonRepository.computeIfAbsent(service, key -> {
            NotifyCenter.publishEvent(new MetadataEvent.ServiceMetadataEvent(service, false));
            return service;
        });
        // [registerInstance] 关键数据写入：将服务添加到命名空间服务映射表中 namespaceSingletonMaps
        namespaceSingletonMaps.computeIfAbsent(result.getNamespace(), namespace -> new ConcurrentHashSet<>()).add(result);
        return result;
    }
}
```

我们重点关注 **[registerInstance] 步骤4**，在这个步骤完成了 **服务实例信息注册后本地缓存的写入**，它会被记录到 `ServiceManager#singletonRepository` 和 `ServiceManager#namespaceSingletonMaps` 两个变量中，并且在在首次通过 `ConcurrentHashMap#computeIfAbsent` 方法添加时会触发 `ServiceMetadataEvent` 事件，不过这个事件在我们本次的逻辑中不重要，所以就不再解释了。再回到 `registerInstance` 方法中，完成 `Service` 缓存的写入后还会发布两个事件：`ClientRegisterServiceEvent` 和 `InstanceMetadataEvent`，这两个事件我们按顺序看：

#### ClientRegisterServiceEvent

`ClientRegisterServiceEvent` 事件由 `ClientServiceIndexesManager` 订阅并消费，如下方代码所示，它会触发 `ServiceChangedEvent` 事件：

```java
@Component
public class ClientServiceIndexesManager extends SmartSubscriber {

    private final ConcurrentMap<Service, Set<String>> publisherIndexes = new ConcurrentHashMap<>();
    
    private void handleClientOperation(ClientOperationEvent event) {
        Service service = event.getService();
        String clientId = event.getClientId();
        if (event instanceof ClientOperationEvent.ClientRegisterServiceEvent) {
            // [registerInstance] 步骤4：处理客户端注册服务事件，将服务和客户端ID添加到发布者索引 publisherIndexes 中
            addPublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientDeregisterServiceEvent) {
            removePublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientSubscribeServiceEvent) {
            addSubscriberIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientUnsubscribeServiceEvent) {
            removeSubscriberIndexes(service, clientId);
        }
    }

    private void addPublisherIndexes(Service service, String clientId) {
        String serviceChangedType = Constants.ServiceChangedType.INSTANCE_CHANGED;
        if (!publisherIndexes.containsKey(service)) {
            // 唯一需要更新索引的时间是 "首次" 创建服务的时
            serviceChangedType = Constants.ServiceChangedType.ADD_SERVICE;
        }
        // 发布服务变更事件，通知订阅者有新的服务实例注册
        NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, serviceChangedType, true));
        publisherIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>()).add(clientId);
    }
}
```

`ServiceChangedEvent` 事件被 `NamingSubscriberServiceV2Impl` 订阅并消费：

```java
public class NamingSubscriberServiceV2Impl extends SmartSubscriber implements NamingSubscriberService {

    private final PushDelayTaskExecuteEngine delayTaskEngine;
    
    @Override
    public void onEvent(Event event) {
        if (event instanceof ServiceEvent.ServiceChangedEvent) {
            // [registerInstance] 步骤5：处理服务变更事件，创建推送任务将服务变更通知给所有订阅者
            ServiceEvent.ServiceChangedEvent serviceChangedEvent = (ServiceEvent.ServiceChangedEvent) event;
            Service service = serviceChangedEvent.getService();
            delayTaskEngine.addTask(service, new PushDelayTask(service, PushConfig.getInstance().getPushTaskDelay()));
            MetricsMonitor.incrementServiceChangeCount(service);
        }
    }
}
```

它会创建一个 `PushDelayTask` 添加到 `NacosDelayTaskExecuteEngine#tasks` 中，这个 `NacosDelayTaskExecuteEngine` 我们在配置发布的章节介绍过，本质上它是一个 `ScheduledExecutorService` 在每 100ms 执行一个 `ConcurrentHashMap<Object, AbstractDelayTask> tasks` 的任务。接下来我们先来了解一下 `PushDelayTask` 任务，重点关注注释信息：

```java
public class PushDelayTask extends AbstractDelayTask {

    private final Service service;

    // 是否推送给所有订阅服务信息的 Client
    private boolean pushToAll;

    private Set<String> targetClients;

    // 创建推送所有订阅者的任务，上文中便是调用的这个构造函数
    public PushDelayTask(Service service, long delay) {
        this.service = service;
        pushToAll = true;
        targetClients = null;
        setTaskInterval(delay);
        setLastProcessTime(System.currentTimeMillis());
    }

    // 创建推送某一个订阅者的任务，专门用于处理某个 IP 推送失败的情况
    public PushDelayTask(Service service, long delay, String targetClient) {
        this.service = service;
        this.pushToAll = false;
        this.targetClients = new HashSet<>(1);
        this.targetClients.add(targetClient);
        setTaskInterval(delay);
        setLastProcessTime(System.currentTimeMillis());
    }

    // 合并任务，避免多次重复调用
    @Override
    public void merge(AbstractDelayTask task) {
        if (!(task instanceof PushDelayTask)) {
            return;
        }
        PushDelayTask oldTask = (PushDelayTask) task;
        if (isPushToAll() || oldTask.isPushToAll()) {
            pushToAll = true;
            targetClients = null;
        } else {
            targetClients.addAll(oldTask.getTargetClients());
        }
        setLastProcessTime(Math.min(getLastProcessTime(), task.getLastProcessTime()));
        Loggers.PUSH.info("[PUSH] Task merge for {}", service);
    }

    public Service getService() {
        return service;
    }

    public boolean isPushToAll() {
        return pushToAll;
    }

    // 获取目标推送 Client
    public Set<String> getTargetClients() {
        return targetClients;
    }
}
```

随后 `PushDelayTask` 会被 `PushDelayTaskProcessor` 处理，会被封装到 `PushExecuteTask` 任务中：

```java
private static class PushDelayTaskProcessor implements NacosTaskProcessor {
    
    private final PushDelayTaskExecuteEngine executeEngine;
    
    public PushDelayTaskProcessor(PushDelayTaskExecuteEngine executeEngine) {
        this.executeEngine = executeEngine;
    }
    
    @Override
    public boolean process(NacosTask task) {
        PushDelayTask pushDelayTask = (PushDelayTask) task;
        Service service = pushDelayTask.getService();
        // [registerInstance] 步骤6：分发推送任务到执行器，准备将服务变更推送给客户端
        NamingExecuteTaskDispatcher.getInstance()
                .dispatchAndExecuteTask(service, new PushExecuteTask(service, executeEngine, pushDelayTask));
        return true;
    }
}
```

`NamingExecuteTaskDispatcher#dispatchAndExecuteTask` 方法会执行到如下逻辑中，分配给某一条线程去处理：

```java
public class NacosExecuteTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractExecuteTask> {

    // 本质上是多条线程
    private final TaskExecuteWorker[] executeWorkers;

    public NacosExecuteTaskExecuteEngine(String name, Logger logger, int dispatchWorkerCount) {
        super(logger);
        executeWorkers = new TaskExecuteWorker[dispatchWorkerCount];
        for (int mod = 0; mod < dispatchWorkerCount; ++mod) {
            executeWorkers[mod] = new TaskExecuteWorker(name, mod, dispatchWorkerCount, getEngineLog());
        }
    }
    
    @Override
    public void addTask(Object tag, AbstractExecuteTask task) {
        NacosTaskProcessor processor = getProcessor(tag);
        if (null != processor) {
            processor.process(task);
            return;
        }
        // 分配给某个线程处理
        TaskExecuteWorker worker = getWorker(tag);
        worker.process(task);
    }

    private TaskExecuteWorker getWorker(Object tag) {
        int idx = (tag.hashCode() & Integer.MAX_VALUE) % workersCount();
        return executeWorkers[idx];
    }
}
```

以上逻辑还未涉及 `PushExecuteTask` 推送服务变更的逻辑处理，大家只需要了解到，至此将推送任务转交到了某个单一的线程中去执行了，接下来我们看一下 `PushExecuteTask` 的具体逻辑：

```java
public class PushExecuteTask extends AbstractExecuteTask {

    private final PushDelayTaskExecuteEngine delayTaskEngine;

    private final PushDelayTask delayTask;
    
    @Override
    public void run() {
        try {
            // [registerInstance] 步骤7：生成推送数据，包含服务实例信息和元数据
            PushDataWrapper wrapper = generatePushData();
            ClientManager clientManager = delayTaskEngine.getClientManager();
            // 遍历目标客户端，向订阅了该服务的客户端推送数据
            for (String each : getTargetClientIds()) {
                Client client = clientManager.getClient(each);
                if (null == client) {
                    // means this client has disconnect
                    continue;
                }
                Subscriber subscriber = client.getSubscriber(service);
                // skip if null
                if (subscriber == null) {
                    continue;
                }
                // 通过推送执行器向客户端推送服务变更通知
                delayTaskEngine.getPushExecutor().doPushWithCallback(each, subscriber, wrapper,
                        new ServicePushCallback(each, subscriber, wrapper.getOriginalData(), delayTask.isPushToAll()));
            }
        } catch (Exception e) {
            Loggers.PUSH.error("Push task for service" + service.getGroupedServiceName() + " execute failed ", e);
            // 异常重试
            delayTaskEngine.addTask(service, new PushDelayTask(service, 1000L));
        }
    }

    // 初始推送时获取所有订阅服务信息的 Client；如果不是推送所有，说明是处理失败重试的场景，则只推送目标 Client 即可
    private Collection<String> getTargetClientIds() {
        return delayTask.isPushToAll() ? delayTaskEngine.getIndexesManager().getAllClientsSubscribeService(service)
                : delayTask.getTargetClients();
    }

    private class ServicePushCallback implements NamingPushCallback {
        @Override
        public void onSuccess() {
            // monitor and log
        }

        @Override
        public void onFail(Throwable e) {
            long pushCostTime = System.currentTimeMillis() - executeStartTime;
            if (!(e instanceof NoRequiredRetryException)) {
                Loggers.PUSH.error("Reason detail: ", e);
                // 如果针对某个 IP 推送失败，则创建推送针对目标 IP 的任务重试推送
                delayTaskEngine.addTask(service,
                        new PushDelayTask(service, PushConfig.getInstance().getPushTaskRetryDelay(), clientId));
            }
            PushResult result = PushResult
                    .pushFailed(service, clientId, actualServiceInfo, subscriber, pushCostTime, e, isPushToAll);
            PushResultHookHolder.getInstance().pushFailed(result);
        }
    }

}
```

从以上逻辑中可知：服务注册信息将推送给每个订阅了这个服务的 Client，如果推送失败会重新添加 `PushDelayTask` 任务重试，以此来保证订阅服务实例信息的 Client 都接收到变更。

#### InstanceMetadataEvent

