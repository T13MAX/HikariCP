# HikariCP


### 重点类及说明

| 类名                 | 说明                               |
|--------------------|----------------------------------|
| `HikariDataSource` | 对外暴露的数据源，用户调用入口                  |
| `HikariConfig`     | 加载配置、初始化参数                       |
| `HikariPool`       | 核心连接池逻辑（创建、借出、回收）                |
| `PoolEntry`        | 包装 JDBC 连接对象                     |
| `ConcurrentBag`    | 并发连接管理容器                         |
| `ProxyConnection`  | 动态代理 `Connection` 对象，拦截方法以管理连接状态 |
| `ProxyStatement`   | 动态代理 `Connection` 对象，拦截方法以管理连接状态 |
| `ConcurrentBag<T>` | 自研高性能连接容器 支持并发借还连接               |
| `HouseKeeper`      | 周期性维护池状态 回收过期连接 补充新连接            |

