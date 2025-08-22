
- 启动类：com.alibaba.nacos.bootstrap.NacosBootstrap

### 单机启动

1. 添加 VM 参数：-Dnacos.standalone=true
2. 创建表：distribution/conf/mysql-schema.sql
3. 生效的日志配置文件：core/src/main/resources/META-INF/logback/nacos.xml

### 集群启动

控制台执行以下命令：

```shell
ifconfig | grep "inet " | grep -v 127.0.0.1
```

获取本机真实的 IP 地址，这个 IP 地址用于配置 Nacos 的集群的 IP:PORT。

接下来在 IDEA 中添加三个 nacos 实例的启动配置，并添加上以下参数：

![img.png](img.png)

- VM: `-Dnacos.home=/Users/nacos/8852 -Dnacos.server.ip=10.254.77.99`

其中 server ip 即为执行命令时获取到的 IP

- nacos.server.main.port: Nacos Server 端口
- nacos.console.port: Nacos 控制台端口

配置上以上内容后，可以先尝试启动一下（会启动失败），目的是能够在配置的 `nacos.home` 目录下生成 `conf` 和 `logs` 目录，在下面创建名称为 `cluster.conf` 的文件，内容如下：

```text
10.254.77.99:8848
10.254.77.99:8850
10.254.77.99:8852
```

这样 Nacos Server 会和其他的 Server 创建 gRPC 连接，文件都创建好之后再次启动，集群就部署成功了。为了启动方便，可以在 IDEA 启动配置中添加一个 Compound，将三个启动配置都添加到这个配置中，这样就可以同时启动三个 Nacos Server 了。

---

### module

- nacos-console: 控制台相关内容

---


### 源码流程日志关键字

- `[notifyConfig]`: 通知配置变更
- `[clientConnection]`: 客户端连接
- `[registerInstance]`: 注册实例

---

### Nacos 对 gRPC 的使用

接下来我们以 Nacos 源码为例，解析它在创建连接时是如何使用 gRPC 的。

#### protobuf

以下是 Nacos 中定义的 protobuf 文件：

```protobuf
syntax = "proto3";

import "google/protobuf/any.proto";
import "google/protobuf/timestamp.proto";

option java_multiple_files = true;
option java_package = "com.alibaba.nacos.api.grpc.auto";

message Metadata {
  string type = 3;
  string clientIp = 8;
  map<string, string> headers = 7;
}

message Payload {
  Metadata metadata = 2;
  google.protobuf.Any body = 3;
}

service Request {
  // Sends a commonRequest
  rpc request (Payload) returns (Payload) {
  }
}

service BiRequestStream {
  // Sends a biStreamRequest
  rpc requestBiStream (stream Payload) returns (stream Payload) {
  }
}
```

首先我们先看一下 `Metadata` 的定义，它是传递请求 `Payload` 的元数据，其中的三个字段的字段号并不是连续的；`Payload` 载荷消息是实际的传输对象，它除了包含 `Metadata` 外，还定义了 `google.protobuf.Any body` 请求体字段，`Any` 类型表示它可以包装任意类型的消息体。

接下来我们分析下它的服务（service）定义：

- `Request` 服务定义了一个 `request` 方法，它接收一个 `Payload` 类型的参数，返回一个 `Payload` 类型的结果，适用于简单的“请求-响应”场景。
- `BiRequestStream` 服务定义了 `requestBiStream` 方法，它接收一个 `Payload` 类型的流参数，返回一个 `Payload` 类型的流结果，是 **双向流式RPC服务**，客户端可以同时发送多个请求，服务端也可以同时发送多个响应，支持全双工通信。

#### 服务端



---
Raft 算法学习指南

Raft 是一种设计用于解决分布式系统中一致性问题的共识算法，它被设计为比 Paxos 更易于理解和实现。从你分享的代码可以看到，Nacos 使用了基于 JRaft 的实现来保证集群配置的一致性。

Raft 算法核心概念
Raft 将分布式一致性问题分解为三个相对独立的子问题：

领导人选举：当现有领导人失效时，选举新的领导人
日志复制：领导人接收客户端请求并将其作为日志条目复制到集群中所有节点
安全性：确保所有节点以相同的顺序应用相同的命令
学习 Raft 的方法
1. 理论基础学习
   阅读原论文：《In Search of an Understandable Consensus Algorithm》(寻找一种可理解的共识算法)

中文翻译版本也有，可以在网上搜索到
视频教程：

MIT 6.824 分布式系统课程中的 Raft 部分

2. 可视化工具
   The Secret Lives of Data http://thesecretlivesofdata.com/raft/ ：交互式可视化演示
   Raft Visualization https://raft.github.io/ ：官方推荐的可视化工具
3. 代码学习
   JRaft：阿里巴巴开源的 Java 版 Raft 实现（你分享的 Nacos 中使用的就是这个）

源码：https://github.com/sofastack/sofa-jraft
etcd/raft：Go 语言实现的 Raft，在 etcd 中使用

源码：https://github.com/etcd-io/etcd/tree/main/raft
4. 动手实践
   构建简单实现：尝试自己实现一个简化版的 Raft
   基于 JRaft 开发：可以尝试在你自己的项目中集成 JRaft
   分析 Nacos 源码：深入理解 Nacos 如何使用 JRaft 实现配置一致性
5. 进阶主题
   成员变更：学习如何安全地改变集群配置
   日志压缩：了解 Raft 中快照机制如何工作
   性能优化：研究提高 Raft 性能的各种技术
   学习资源推荐
   官方资源：

Raft 官网：https://raft.github.io/
Raft 论文：https://raft.github.io/raft.pdf
书籍：

《分布式一致性算法开发实战》
《数据密集型应用系统设计》(设计数据密集型应用) 第9章
博客和教程：

Raft 算法详解 https://zhuanlan.zhihu.com/p/32052223
JRaft 实现原理 https://www.sofastack.tech/projects/sofa-jraft/overview/

Raft 算法是分布式系统领域的重要知识点，通过理论结合实践的方式学习，你会对分布式一致性有更深入的理解，也能更好地应用到实际工作中。