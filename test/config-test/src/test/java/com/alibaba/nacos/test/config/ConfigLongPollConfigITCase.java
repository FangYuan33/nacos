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

package com.alibaba.nacos.test.config;

import com.alibaba.nacos.Nacos;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.ConfigFuzzyWatchChangeEvent;
import com.alibaba.nacos.api.config.listener.FuzzyWatchEventWatcher;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.client.naming.NacosNamingService;
import com.alibaba.nacos.test.base.ConfigCleanUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Integration test case for long polling configuration updates using Nacos.
 *
 * @author liaochuntao
 * @date 2019-06-07 22:24
 **/
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@ExtendWith(SpringExtension.class)
//@SpringBootTest(classes = Nacos.class, properties = {
//        "server.servlet.context-path=/nacos"}, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class ConfigLongPollConfigITCase {
    
//    @LocalServerPort
    private int port;

    //    @BeforeAll
//    @AfterAll
    static void cleanClientCache() throws Exception {
        ConfigCleanUtils.cleanClientCache();
        ConfigCleanUtils.changeToNewTestNacosHome(ConfigLongPollConfigITCase.class.getSimpleName());
        
    }

    @Test
    void testFuzzyWatch() throws NacosException, InterruptedException {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, "127.0.0.1:8850");
        properties.put(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT, "20000");
        properties.put(PropertyKeyConst.CONFIG_RETRY_TIME, "3000");
        properties.put(PropertyKeyConst.MAX_RETRY, "5");
        properties.put(PropertyKeyConst.CLIENT_WORKER_MAX_THREAD_COUNT, "2");
        ConfigService configService = NacosFactory.createConfigService(properties);

        configService.fuzzyWatch("*", new FuzzyWatchEventWatcher() {
            @Override
            public void onEvent(ConfigFuzzyWatchChangeEvent event) {
                System.out.println("FuzzyWatch: " + event);
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        });

        TimeUnit.HOURS.sleep(2);
    }


    @Test
    void test() throws InterruptedException, NacosException {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, "127.0.0.1:8850");
        properties.put(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT, "20000");
        properties.put(PropertyKeyConst.CONFIG_RETRY_TIME, "3000");
        properties.put(PropertyKeyConst.MAX_RETRY, "5");
        ConfigService configService = NacosFactory.createConfigService(properties);

        MemoryAnalyzer analyzer = new MemoryAnalyzer();
        ThreadPoolAnalyzer threadPoolAnalyzer = new ThreadPoolAnalyzer();

        System.out.println("=== 第一阶段：初始状态 ===");
        analyzer.takeSnapshot("初始状态");
        threadPoolAnalyzer.takeThreadPoolSnapshot("初始状态", configService);
        TimeUnit.SECONDS.sleep(2);

        System.out.println("=== 第二阶段：添加监听器 ===");
        // 创建 ConfigService 的弱引用，用于检测是否被回收
        WeakReference<ConfigService> configServiceRef = new WeakReference<>(configService);

        // 添加监听器
        configService.addListener("default_value", "DEFAULT_GROUP", new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }
            
            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println(configInfo);
            }
        });

        analyzer.takeSnapshot("添加监听器后");
        threadPoolAnalyzer.takeThreadPoolSnapshot("添加监听器后", configService);
        TimeUnit.HOURS.sleep(2);

        System.out.println("=== 第三阶段：执行 shutDown ===");
        // 在 shutdown 前记录线程池状态
        threadPoolAnalyzer.takeThreadPoolSnapshot("shutDown 前", configService);
        TimeUnit.SECONDS.sleep(10);
        configService.shutDown();
        
        // 在 shutdown 后立即记录线程池状态
        threadPoolAnalyzer.takeThreadPoolSnapshot("shutDown 后", configService);
        configService = null;

        analyzer.takeSnapshot("shutDown 后");
        TimeUnit.SECONDS.sleep(8);

        System.out.println("=== 第四阶段：强制 GC ===");
        analyzer.forceFullGCWithRetry();
        analyzer.takeSnapshot("GC 后");
        
        // 检查线程池回收状态
        threadPoolAnalyzer.checkThreadPoolRecycling();

        // 检查 ConfigService 是否被回收
        if (configServiceRef.get() == null) {
            System.out.println("✓ ConfigService 已被成功回收");
        } else {
            System.out.println("⚠ ConfigService 仍然存在，可能存在内存泄漏");
        }

        System.out.println("=== 内存分析报告 ===");
        analyzer.printAnalysisReport();

        threadPoolAnalyzer.printAnalysisReport();
        printActiveThreads();

        // 缩短等待时间
        TimeUnit.SECONDS.sleep(3);
        // 等待足够时间让 JProfiler 捕获快照
        System.out.println("Profiler 快照分析...");
        TimeUnit.SECONDS.sleep(10);
    }

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

    @Test
    void testNacosNamingService2() throws InterruptedException, NacosException {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, "127.0.0.1:8850");
        properties.put(PropertyKeyConst.NAMESPACE, "public");
        NamingService namingService = NacosFactory.createNamingService(properties);

        try {
            // 查询一个服务实例
            System.out.println(namingService.selectOneHealthyInstance("test-service"));
        } catch (Exception e) {
            System.out.println("服务注册失败(预期，因为服务器可能未启动): " + e.getMessage());
        }

        namingService.shutDown();
    }

    private void printActiveThreads() {
    System.out.println("=== 活跃线程检查 ===");
    ThreadGroup rootThreadGroup = Thread.currentThread().getThreadGroup();
    while (rootThreadGroup.getParent() != null) {
        rootThreadGroup = rootThreadGroup.getParent();
    }
    
    Thread[] threads = new Thread[rootThreadGroup.activeCount() * 2];
    int count = rootThreadGroup.enumerate(threads);
    
    int nacosThreadCount = 0;
    for (int i = 0; i < count; i++) {
        if (threads[i] != null && threads[i].getName().contains("nacos.client.config.listener.task")) {
            System.out.printf("发现未关闭的 Nacos 线程: %s (状态: %s)\n", 
                threads[i].getName(), threads[i].getState());
            nacosThreadCount++;
        }
    }
    
    if (nacosThreadCount == 0) {
        System.out.println("✅ 所有 Nacos 相关线程已正确关闭");
    } else {
        System.out.printf("❌ 发现 %d 个未关闭的 Nacos 线程\n", nacosThreadCount);
    }
}

    private void printActiveNamingThreads() {
        System.out.println("=== 活跃 Naming 线程检查 ===");
        ThreadGroup rootThreadGroup = Thread.currentThread().getThreadGroup();
        while (rootThreadGroup.getParent() != null) {
            rootThreadGroup = rootThreadGroup.getParent();
        }
        
        Thread[] threads = new Thread[rootThreadGroup.activeCount() * 2];
        int count = rootThreadGroup.enumerate(threads);
        
        int namingThreadCount = 0;
        for (int i = 0; i < count; i++) {
            if (threads[i] != null && 
                (threads[i].getName().contains("nacos.client.naming") || 
                 threads[i].getName().contains("nacos.client.notify") ||
                 threads[i].getName().contains("nacos.client.transport"))) {
                System.out.printf("发现未关闭的 Nacos Naming 线程: %s (状态: %s)\n", 
                    threads[i].getName(), threads[i].getState());
                namingThreadCount++;
            }
        }
        
        if (namingThreadCount == 0) {
            System.out.println("✅ 所有 Nacos Naming 相关线程已正确关闭");
        } else {
            System.out.printf("❌ 发现 %d 个未关闭的 Nacos Naming 线程\n", namingThreadCount);
        }
    }

    /**
     * Naming Service 线程池状态分析器
     */
    private static class NamingServiceThreadPoolAnalyzer {
        private Map<String, WeakReference<ExecutorService>> threadPoolRefs = new HashMap<>();
        private Map<String, ThreadPoolReflectionUtils.ThreadPoolStatus> lastSnapshot = new HashMap<>();
        
        /**
         * 记录 NamingService 线程池快照
         */
        public void takeThreadPoolSnapshot(String phase, NamingService namingService) {
            System.out.printf("=== %s - Naming Service 线程池状态 ===\n", phase);
            
            try {
                // 获取 NacosNamingService 中的线程池
                Map<String, ExecutorService> threadPools = NamingServiceReflectionUtils.getAllExecutors(namingService);
                if (threadPools == null || threadPools.isEmpty()) {
                    System.out.println("未找到 NamingService 中的线程池");
                    System.out.println();
                    return;
                }
                
                System.out.printf("NamingService 包含 %d 个线程池:\n", threadPools.size());
                
                for (Map.Entry<String, ExecutorService> entry : threadPools.entrySet()) {
                    String poolName = entry.getKey();
                    ExecutorService executor = entry.getValue();
                    
                    if (executor != null) {
                        ThreadPoolReflectionUtils.ThreadPoolStatus status = 
                            ThreadPoolReflectionUtils.getThreadPoolStatus(executor);
                        
                        System.out.printf("  %s: %s\n", poolName, status);
                        
                        // 记录弱引用用于回收检测
                        threadPoolRefs.put(poolName, new WeakReference<>(executor));
                        
                        // 打印状态变化
                        ThreadPoolReflectionUtils.ThreadPoolStatus lastStatus = lastSnapshot.get(poolName);
                        if (lastStatus != null) {
                            printNamingStatusDiff(poolName, lastStatus, status);
                        }
                        
                        lastSnapshot.put(poolName, status);
                    } else {
                        System.out.printf("  %s: null\n", poolName);
                    }
                }
            } catch (Exception e) {
                System.err.println("获取 NamingService 线程池状态失败: " + e.getMessage());
                e.printStackTrace();
            }
            
            System.out.println();
        }
        
        /**
         * 检查线程池是否被回收
         */
        public void checkThreadPoolRecycling() {
            System.out.println("=== 检查 Naming Service 线程池回收状态 ===");
            
            int totalPools = threadPoolRefs.size();
            int recycledPools = 0;
            
            for (Map.Entry<String, WeakReference<ExecutorService>> entry : threadPoolRefs.entrySet()) {
                String poolName = entry.getKey();
                ExecutorService executor = entry.getValue().get();
                
                if (executor == null) {
                    System.out.printf("✓ %s 已被回收\n", poolName);
                    recycledPools++;
                } else {
                    System.out.printf("⚠ %s 仍然存在 (shutdown: %s, terminated: %s)\n", 
                        poolName, executor.isShutdown(), executor.isTerminated());
                }
            }
            
            System.out.printf("Naming Service 回收统计: %d/%d 个线程池已被回收 (%.2f%%)\n",
                recycledPools, totalPools,
                totalPools > 0 ? (double) recycledPools / totalPools * 100 : 0);
            
            if (recycledPools < totalPools) {
                System.out.println("❌ 部分 Naming Service 线程池未被正确回收");
            } else {
                System.out.println("✅ 所有 Naming Service 线程池已被正确回收");
            }
            
            System.out.println();
        }
        
        /**
         * 打印状态变化
         */
        private void printNamingStatusDiff(String poolName, ThreadPoolReflectionUtils.ThreadPoolStatus before,
                                          ThreadPoolReflectionUtils.ThreadPoolStatus after) {
            if (before.isShutdown != after.isShutdown) {
                System.out.printf("    %s shutdown: %s -> %s\n", poolName, before.isShutdown, after.isShutdown);
            }
            if (before.isTerminated != after.isTerminated) {
                System.out.printf("    %s terminated: %s -> %s\n", poolName, before.isTerminated, after.isTerminated);
            }
            if (before.activeCount != after.activeCount) {
                System.out.printf("    %s active threads: %d -> %d\n", poolName, before.activeCount, after.activeCount);
            }
        }
        
        /**
         * 生成分析报告
         */
        public void printAnalysisReport() {
            System.out.println("=== Naming Service 线程池分析报告 ===");
            
            for (Map.Entry<String, ThreadPoolReflectionUtils.ThreadPoolStatus> entry : lastSnapshot.entrySet()) {
                String poolName = entry.getKey();
                ThreadPoolReflectionUtils.ThreadPoolStatus status = entry.getValue();
                
                System.out.printf("%s 最终状态:\n", poolName);
                System.out.printf("  - Shutdown: %s\n", status.isShutdown);
                System.out.printf("  - Terminated: %s\n", status.isTerminated);
                System.out.printf("  - Active Count: %d\n", status.activeCount);
                System.out.printf("  - Pool Size: %d\n", status.poolSize);
                System.out.printf("  - Task Count: %d\n", status.taskCount);
                System.out.printf("  - Completed Task Count: %d\n", status.completedTaskCount);
            }
        }
    }

    /**
     * Naming Service 反射工具类 - 用于获取 NamingService 内部线程池状态
     */
    private static class NamingServiceReflectionUtils {
        
        /**
         * 获取 NamingService 中的所有线程池
         */
        @SuppressWarnings("unchecked")
        public static Map<String, ExecutorService> getAllExecutors(NamingService namingService) {
            Map<String, ExecutorService> executors = new HashMap<>();
            
            try {
                // 获取 NacosNamingService 中的 clientProxy 字段
                Field clientProxyField = namingService.getClass().getDeclaredField("clientProxy");
                clientProxyField.setAccessible(true);
                Object clientProxy = clientProxyField.get(namingService);
                
                if (clientProxy != null) {
                    // 尝试获取代理中的执行器
                    try {
                        Field executorField = clientProxy.getClass().getDeclaredField("executor");
                        executorField.setAccessible(true);
                        ExecutorService executor = (ExecutorService) executorField.get(clientProxy);
                        if (executor != null) {
                            executors.put("clientProxy.executor", executor);
                        }
                    } catch (Exception e) {
                        // 忽略，可能不存在此字段
                    }
                    
                    // 尝试获取其他可能的执行器
                    Field[] fields = clientProxy.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        if (ExecutorService.class.isAssignableFrom(field.getType())) {
                            field.setAccessible(true);
                            ExecutorService executor = (ExecutorService) field.get(clientProxy);
                            if (executor != null) {
                                executors.put("clientProxy." + field.getName(), executor);
                            }
                        }
                    }
                }
                
                // 获取 serviceInfoHolder 中的执行器
                try {
                    Field serviceInfoHolderField = namingService.getClass().getDeclaredField("serviceInfoHolder");
                    serviceInfoHolderField.setAccessible(true);
                    Object serviceInfoHolder = serviceInfoHolderField.get(namingService);
                    
                    if (serviceInfoHolder != null) {
                        Field[] fields = serviceInfoHolder.getClass().getDeclaredFields();
                        for (Field field : fields) {
                            if (ExecutorService.class.isAssignableFrom(field.getType())) {
                                field.setAccessible(true);
                                ExecutorService executor = (ExecutorService) field.get(serviceInfoHolder);
                                if (executor != null) {
                                    executors.put("serviceInfoHolder." + field.getName(), executor);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // 忽略，可能不存在此字段
                }
                
                // 获取 changeNotifier 中的执行器
                try {
                    Field changeNotifierField = namingService.getClass().getDeclaredField("changeNotifier");
                    changeNotifierField.setAccessible(true);
                    Object changeNotifier = changeNotifierField.get(namingService);
                    
                    if (changeNotifier != null) {
                        Field[] fields = changeNotifier.getClass().getDeclaredFields();
                        for (Field field : fields) {
                            if (ExecutorService.class.isAssignableFrom(field.getType())) {
                                field.setAccessible(true);
                                ExecutorService executor = (ExecutorService) field.get(changeNotifier);
                                if (executor != null) {
                                    executors.put("changeNotifier." + field.getName(), executor);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // 忽略，可能不存在此字段
                }
                
            } catch (Exception e) {
                System.err.println("获取 NamingService 执行器失败: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
            
            return executors;
        }
    }

    /**
     * 内存分析工具类
     */
    private static class MemoryAnalyzer {
        private final MemoryMXBean memoryBean;
        private final List<GarbageCollectorMXBean> gcBeans;
        private MemorySnapshot lastSnapshot;

        public MemoryAnalyzer() {
            this.memoryBean = ManagementFactory.getMemoryMXBean();
            this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        }

        public void takeSnapshot(String phase) {
            MemorySnapshot snapshot = new MemorySnapshot(phase);
            System.out.printf("=== %s ===\n", phase);
            snapshot.print();

            if (lastSnapshot != null) {
                printMemoryDiff(lastSnapshot, snapshot);
            }

            lastSnapshot = snapshot;
            System.out.println();
        }

        public void forceFullGCWithRetry() {
            System.out.println("执行多轮 GC 确保内存释放...");

            for (int i = 0; i < 5; i++) {
                long beforeGC = getUsedMemory();

                // 执行 GC
                System.gc();

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                long afterGC = getUsedMemory();
                long released = beforeGC - afterGC;

                System.out.printf("第 %d 轮 GC: 释放内存 %s\n",
                        i + 1, formatBytes(released));

                // 如果连续两轮 GC 释放的内存很少，说明已经稳定
                if (released < 1024 * 1024) { // 小于 1MB
                    System.out.println("内存使用已稳定");
                    break;
                }
            }
        }

        public void printAnalysisReport() {
            System.out.println("=== GC 统计信息 ===");
            for (GarbageCollectorMXBean gcBean : gcBeans) {
                System.out.printf("%s: 执行次数=%d, 总耗时=%d ms\n",
                        gcBean.getName(),
                        gcBean.getCollectionCount(),
                        gcBean.getCollectionTime());
            }
        }

        private long getUsedMemory() {
            return memoryBean.getHeapMemoryUsage().getUsed();
        }

        private void printMemoryDiff(MemorySnapshot before, MemorySnapshot after) {
            long heapDiff = after.heapUsed - before.heapUsed;
            long nonHeapDiff = after.nonHeapUsed - before.nonHeapUsed;

            System.out.printf("内存变化: 堆内存 %s%s, 非堆内存 %s%s\n",
                    heapDiff >= 0 ? "+" : "", formatBytes(heapDiff),
                    nonHeapDiff >= 0 ? "+" : "", formatBytes(nonHeapDiff));
        }

        private class MemorySnapshot {
            final String phase;
            final long heapUsed;
            final long heapMax;
            final long nonHeapUsed;
            final long timestamp;

            MemorySnapshot(String phase) {
                this.phase = phase;
                this.timestamp = System.currentTimeMillis();

                MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
                this.heapUsed = heapUsage.getUsed();
                this.heapMax = heapUsage.getMax();

                MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
                this.nonHeapUsed = nonHeapUsage.getUsed();
            }

            void print() {
                System.out.printf("堆内存: %s/%s (%.2f%%)\n",
                        formatBytes(heapUsed),
                        formatBytes(heapMax),
                        (double) heapUsed / heapMax * 100);

                System.out.printf("非堆内存: %s\n", formatBytes(nonHeapUsed));
                System.out.printf("时间戳: %d\n", timestamp);
            }
        }
    }

    /**
     * 线程池反射工具类 - 用于获取 ClientWorker 内部线程池状态
     */
    private static class ThreadPoolReflectionUtils {
        
        /**
         * 获取 ConfigService 中的 multiTaskExecutor Map
         */
        @SuppressWarnings("unchecked")
        public static Map<String, ExecutorService> getMultiTaskExecutor(ConfigService configService) {
            try {
                // 获取 NacosConfigService 中的 worker 字段
                Field workerField = configService.getClass().getDeclaredField("worker");
                workerField.setAccessible(true);
                Object worker = workerField.get(configService);
                
                // 获取 ClientWorker 中的 agent 字段
                Field agentField = worker.getClass().getDeclaredField("agent");
                agentField.setAccessible(true);
                Object agent = agentField.get(worker);
                
                // 获取 ConfigRpcTransportClient 中的 multiTaskExecutor 字段
                Field multiTaskExecutorField = agent.getClass().getDeclaredField("multiTaskExecutor");
                multiTaskExecutorField.setAccessible(true);
                return (Map<String, ExecutorService>) multiTaskExecutorField.get(agent);
                
            } catch (Exception e) {
                System.err.println("获取 multiTaskExecutor 失败: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
        
        /**
         * 获取线程池的详细状态信息
         */
        public static ThreadPoolStatus getThreadPoolStatus(ExecutorService executor) {
            if (executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
                return new ThreadPoolStatus(
                    tpe.getCorePoolSize(),
                    tpe.getMaximumPoolSize(),
                    tpe.getActiveCount(),
                    tpe.getPoolSize(),
                    tpe.getTaskCount(),
                    tpe.getCompletedTaskCount(),
                    tpe.isShutdown(),
                    tpe.isTerminated()
                );
            }
            return new ThreadPoolStatus(0, 0, 0, 0, 0, 0,
                executor.isShutdown(), executor.isTerminated());
        }
        
        /**
         * 线程池状态信息类
         */
        public static class ThreadPoolStatus {
            public final int corePoolSize;
            public final int maximumPoolSize;
            public final int activeCount;
            public final int poolSize;
            public final long taskCount;
            public final long completedTaskCount;
            public final boolean isShutdown;
            public final boolean isTerminated;
            
            public ThreadPoolStatus(int corePoolSize, int maximumPoolSize, int activeCount,
                                  int poolSize, long taskCount, long completedTaskCount,
                                  boolean isShutdown, boolean isTerminated) {
                this.corePoolSize = corePoolSize;
                this.maximumPoolSize = maximumPoolSize;
                this.activeCount = activeCount;
                this.poolSize = poolSize;
                this.taskCount = taskCount;
                this.completedTaskCount = completedTaskCount;
                this.isShutdown = isShutdown;
                this.isTerminated = isTerminated;
            }
            
            @Override
            public String toString() {
                return String.format(
                    "ThreadPool[core=%d, max=%d, active=%d, pool=%d, task=%d, completed=%d, shutdown=%s, terminated=%s]",
                    corePoolSize, maximumPoolSize, activeCount, poolSize,
                    taskCount, completedTaskCount, isShutdown, isTerminated
                );
            }
        }
    }

    /**
     * 线程池状态分析器
     */
    private static class ThreadPoolAnalyzer {
        private Map<String, WeakReference<ExecutorService>> threadPoolRefs = new HashMap<>();
        private Map<String, ThreadPoolReflectionUtils.ThreadPoolStatus> lastSnapshot = new HashMap<>();
        
        /**
         * 记录线程池快照
         */
        public void takeThreadPoolSnapshot(String phase, ConfigService configService) {
            System.out.printf("=== %s - 线程池状态 ===\n", phase);
            
            Map<String, ExecutorService> multiTaskExecutor = ThreadPoolReflectionUtils.getMultiTaskExecutor(configService);
            if (multiTaskExecutor == null) {
                System.out.println("⚠ 无法获取 multiTaskExecutor");
                return;
            }
            
            System.out.printf("multiTaskExecutor 包含 %d 个线程池:\n", multiTaskExecutor.size());
            
            for (Map.Entry<String, ExecutorService> entry : multiTaskExecutor.entrySet()) {
                String taskId = entry.getKey();
                ExecutorService executor = entry.getValue();
                
                // 创建弱引用用于后续检测回收
                threadPoolRefs.put(taskId, new WeakReference<>(executor));
                
                ThreadPoolReflectionUtils.ThreadPoolStatus status =
                    ThreadPoolReflectionUtils.getThreadPoolStatus(executor);
                
                System.out.printf("  TaskId[%s]: %s\n", taskId, status);
                
                // 与上次快照对比
                ThreadPoolReflectionUtils.ThreadPoolStatus lastStatus = lastSnapshot.get(taskId);
                if (lastStatus != null) {
                    printStatusDiff(taskId, lastStatus, status);
                }
                
                lastSnapshot.put(taskId, status);
            }
            
            System.out.println();
        }
        
        /**
         * 检查线程池是否被回收
         */
        public void checkThreadPoolRecycling() {
            System.out.println("=== 检查线程池回收状态 ===");
            
            int totalPools = threadPoolRefs.size();
            int recycledPools = 0;
            
            for (Map.Entry<String, WeakReference<ExecutorService>> entry : threadPoolRefs.entrySet()) {
                String taskId = entry.getKey();
                WeakReference<ExecutorService> ref = entry.getValue();
                
                if (ref.get() == null) {
                    System.out.printf("✓ TaskId[%s] 的线程池已被回收\n", taskId);
                    recycledPools++;
                } else {
                    ExecutorService executor = ref.get();
                    ThreadPoolReflectionUtils.ThreadPoolStatus status =
                        ThreadPoolReflectionUtils.getThreadPoolStatus(executor);
                    System.out.printf("⚠ TaskId[%s] 的线程池仍然存在: %s\n", taskId, status);
                }
            }
            
            System.out.printf("回收统计: %d/%d 个线程池已被回收 (%.2f%%)\n",
                recycledPools, totalPools,
                totalPools > 0 ? (double) recycledPools / totalPools * 100 : 0);
            
            if (recycledPools < totalPools) {
                System.out.println("⚠ 检测到可能的线程池资源泄漏！");
            } else {
                System.out.println("✓ 所有线程池都已正确回收");
            }
            
            System.out.println();
        }
        
        /**
         * 打印状态变化
         */
        private void printStatusDiff(String taskId, ThreadPoolReflectionUtils.ThreadPoolStatus before,
                                   ThreadPoolReflectionUtils.ThreadPoolStatus after) {
            if (before.isShutdown != after.isShutdown) {
                System.out.printf("    状态变化: shutdown %s -> %s\n", before.isShutdown, after.isShutdown);
            }
            if (before.isTerminated != after.isTerminated) {
                System.out.printf("    状态变化: terminated %s -> %s\n", before.isTerminated, after.isTerminated);
            }
            if (before.activeCount != after.activeCount) {
                System.out.printf("    状态变化: activeCount %d -> %d\n", before.activeCount, after.activeCount);
            }
        }
        
        /**
         * 生成分析报告
         */
        public void printAnalysisReport() {
            System.out.println("=== 线程池分析报告 ===");
            
            for (Map.Entry<String, ThreadPoolReflectionUtils.ThreadPoolStatus> entry : lastSnapshot.entrySet()) {
                String taskId = entry.getKey();
                ThreadPoolReflectionUtils.ThreadPoolStatus status = entry.getValue();
                
                System.out.printf("TaskId[%s] 最终状态:\n", taskId);
                System.out.printf("  - 是否关闭: %s\n", status.isShutdown);
                System.out.printf("  - 是否终止: %s\n", status.isTerminated);
                System.out.printf("  - 活跃线程数: %d\n", status.activeCount);
                System.out.printf("  - 总任务数: %d\n", status.taskCount);
                System.out.printf("  - 已完成任务数: %d\n", status.completedTaskCount);
                
                // 分析潜在问题
                if (!status.isShutdown) {
                    System.out.printf("  ⚠ 警告: 线程池未正确关闭\n");
                }
                if (status.isShutdown && !status.isTerminated && status.activeCount > 0) {
                    System.out.printf("  ⚠ 警告: 线程池已关闭但仍有活跃线程\n");
                }
                
                System.out.println();
            }
        }
    }

    /**
     * 格式化字节数
     */
    private static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "-" + formatBytes(-bytes);
        }

        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }
}
