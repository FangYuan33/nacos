
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

并且需要在 `resources/application.properties` 添加配置：

```properties
nacos.core.auth.server.identity.key=nacos_fy
nacos.core.auth.server.identity.value=nacos_fy
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

```sql
-- 查询 derby 数据库中所有的表
SELECT t.TABLENAME FROM SYS.SYSTABLES t, SYS.SYSSCHEMAS s WHERE s.SCHEMAID = t.SCHEMAID
```

