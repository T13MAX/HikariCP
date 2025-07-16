# HikariCP

## 🔷 HikariCP 源码结构与推荐阅读

### 📦 核心模块

- 全部在同一个模块中（`com.zaxxer.hikari` 包）

### 🔑 重点类及说明

| 类名                 | 说明                               |
|--------------------|----------------------------------|
| `HikariDataSource` | 对外暴露的数据源，用户调用入口                  |
| `HikariConfig`     | 加载配置、初始化参数                       |
| `HikariPool`       | 核心连接池逻辑（创建、借出、回收）                |
| `PoolEntry`        | 包装 JDBC 连接对象                     |
| `ConcurrentBag`    | 并发连接管理容器                         |
| `HouseKeeper`      | 定时清理线程                           |
| `ProxyConnection`  | 动态代理 `Connection` 对象，拦截方法以管理连接状态 |
| `ProxyStatement`   | 动态代理 `Connection` 对象，拦截方法以管理连接状态 |
| `ConcurrentBag<T>` | 自研高性能连接容器 支持并发借还连接               |
| `HouseKeeper`      | 周期性维护池状态 回收过期连接 补充新连接            |

### 📘 推荐阅读顺序

1. `HikariDataSource` → 创建数据源入口
2. `HikariConfig` → 读取和解析配置
3. `HikariPool` → 核心连接管理逻辑
4. `PoolEntry` → 连接包装对象
5. `ConcurrentBag` → 高效并发连接容器
6. `HouseKeeper` → 定时器与维护逻辑