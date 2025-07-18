
- 启动类：com.alibaba.nacos.bootstrap.NacosBootstrap

### 单机启动

1. 添加 VM 参数：-Dnacos.standalone=true
2. 创建表：distribution/conf/mysql-schema.sql
3. 生效的日志配置文件：core/src/main/resources/META-INF/logback/nacos.xml

---

### module

- nacos-console: 控制台相关内容

