
- 启动类：com.alibaba.nacos.bootstrap.NacosBootstrap

### 单机启动

1. 添加 VM 参数：-Dnacos.standalone=true
2. 创建表：distribution/conf/mysql-schema.sql
3. 生效的日志配置文件：core/src/main/resources/META-INF/logback/nacos.xml

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



