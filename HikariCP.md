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

### 总结

HikariCP主要做的事是高效管理数据库连接池 使用代理包装连接以实现性能优化和资源管理 并内置连接泄漏检测 设计轻量且易用

- HikariPool：连接池核心，维护连接的创建、回收、驱逐和调度
- PoolEntry：连接池中的连接包装，保存连接状态和元数据
- ProxyConnection / ProxyStatement / ProxyResultSet：对 JDBC 连接、语句和结果集的代理，增强功能（如泄漏检测、状态管理）
- HikariConfig：配置类，定义各种连接池参数
- ProxyFactory（通过Javassist生成实现）：生成各种代理实例，注入增强代码

。
