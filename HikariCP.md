# HikariCP

### 重点类及说明

| 类名                 | 说明                               |
|--------------------|----------------------------------|
| `HikariDataSource` | 对外数据源接口 负责对外暴露连接获取功能 管理连接池生命周期   |
| `HikariConfig`     | 加载配置、初始化参数                       |
| `HikariPool`       | 核心连接池逻辑（创建、借出、回收）                |
| `PoolEntry`        | 连接池中的连接包装 包装 JDBC 连接对象           |
| `ConcurrentBag`    | 并发连接管理容器                         |
| `ProxyConnection`  | 动态代理 `Connection` 对象，拦截方法以管理连接状态 |
| `ProxyStatement`   | 动态代理 `Connection` 对象，拦截方法以管理连接状态 |
| `ConcurrentBag<T>` | 自研高性能连接容器 支持并发借还连接               |
| `HouseKeeper`      | 周期性维护池状态 回收过期连接 补充新连接            |

### 配置

#### 基础配置

| 配置项               | 说明       | 建议      |
|-------------------|----------|---------|
| `jdbcUrl`         | 数据库连接URL | 必填      |
| `username`        | 数据库用户名   | 必填      |
| `password`        | 数据库密码    | 必填      |
| `driverClassName` | JDBC驱动类名 | 可选 自动推导 |

#### 连接池大小相关

| 配置项               | 说明                         | 建议                                    |
|-------------------|----------------------------|---------------------------------------|
| `maximumPoolSize` | 最大连接数 默认10                 | 根据并发情况设置 一般为 CPU 核数 × 2               |
| `minimumIdle`     | 最小空闲连接数 默认=maximumPoolSize | 可设为0节省资源 或设为相同保持连接稳定                  |
| `idleTimeout`     | 空闲连接的最大存活时间 默认600000ms     | 如果 minimumIdle 小于 maximumPoolSize 可调低 |
| `maxLifetime`     | 连接最大生命周期 默认1800000ms       | 设置略小于数据库自身连接超时                        |
| `keepaliveTime`   | 保持连接活跃的ping间隔 默认0禁用        | 设置小于数据库空闲超时时间 如600000ms               |

#### 超时设置

| 配置项                 | 说明                  | 建议                 |
|---------------------|---------------------|--------------------|
| `connectionTimeout` | 等待连接的最大时间 默认30000ms | 视业务情况设为1000~5000ms |
| `validationTimeout` | 校验连接的超时时间 默认5000ms  | 可适当调小              |

#### SQL执行相关

| 配置项                    | 说明       | 建议               |
|------------------------|----------|------------------|
| `autoCommit`           | 是否自动提交事务 | 需要手动控制事务时关闭      |
| `readOnly`             | 是否只读连接   | 读写分离 只读操作可设为true |
| `transactionIsolation` | 事务隔离级别   | 可选配置             |

#### 连接检测与泄漏排查

| 配置项                      | 说明                  | 建议                  |
|--------------------------|---------------------|---------------------|
| `connectionTestQuery`    | 连接校验SQL（如 SELECT 1） | 不推荐设置 用 isValid 更高效 |
| `leakDetectionThreshold` | 连接泄漏检测阈值 默认0(关闭)    | 设为业务超时时间如3000ms进行排查 |

---

#### 示例配置代码

``` java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://localhost:3306/test");
config.setUsername("root");
config.setPassword("123456");

config.setMaximumPoolSize(20);
config.setMinimumIdle(5);
config.setIdleTimeout(600000);
config.setMaxLifetime(1800000);
config.setKeepaliveTime(300000);
config.setConnectionTimeout(5000);

config.setLeakDetectionThreshold(3000);

HikariDataSource ds = new HikariDataSource(config);
Connection conn = ds.getConnection();
// ... 使用连接
conn.close(); // 自动归还
```

### 总结

HikariCP 是一个高性能 JDBC 连接池框架 它通过一个池类(HikariPool)统一管理连接对象(PoolEntry)并对原生连接封装为代理连接(
ProxyConnection)实现连接复用 事务控制 泄漏检测等功能 框架通过配置类(HikariConfig)集中配置连接参数 采用 Javassist
动态生成代理类提升性能 同时利用后台线程(HouseKeeper)定期清理无效连接 保证连接池健康稳定

#### 常用类

- HikariPool：连接池核心，维护连接的创建、回收、驱逐和调度
- PoolEntry：连接池中的连接包装，保存连接状态和元数据和定时任务和Statement列表
- ProxyStatement / ProxyResultSet：对 JDBC 连接、语句和结果集的代理，增强功能（如泄漏检测、状态管理）
- ProxyConnection 代理连接 JavassistProxyFactory会生成实现类 进行增强
- HikariConfig：配置类，定义各种连接池参数
- ProxyFactory（通过Javassist生成实现）：生成各种代理实例，注入增强代码
- ConcurrentBag 存PoolEntry的地方 高性能容器 共享列表 ThreadLocal SynchronousQueue FastList
- FastList 没有边界检测的高性能List
- SynchronousQueue 没有缓冲区的 避免加锁和上下文切换 减少内存开销
- ProxyLeakTask 连接泄漏检测任务 超过一定时间没归还认为是泄漏
- HouseKeeper 后台维护线程 定期检查连接是否过期 检查idleTimeout超过的空闲连接 检查连接池小于minimumIdle则补充
- SuspendResumeLock 连接池关闭或重配置期间阻止线程获取连接的工具类
- JavassistProxyFactory 比动态代理快 减少反射开销 功能增强 避免冗长代码
- MetricsTrackerDelegate 监控

#### 总结的总结

- 新建HikariConfig -> 新建HikariDataSource -> getConnection
- 这个Connection 是HikariCP的代理连接 close不是真的关 是归还连接池
- 连接泄漏检测
