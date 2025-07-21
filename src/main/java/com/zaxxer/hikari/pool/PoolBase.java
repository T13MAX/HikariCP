/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.SQLExceptionOverride;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import com.zaxxer.hikari.util.DriverDataSource;
import com.zaxxer.hikari.util.PropertyElf;
import com.zaxxer.hikari.util.UtilityElf;
import com.zaxxer.hikari.util.UtilityElf.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.StringJoiner;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.zaxxer.hikari.pool.ProxyConnection.*;
import static com.zaxxer.hikari.util.ClockSource.*;
import static com.zaxxer.hikari.util.UtilityElf.createInstance;
import static java.util.concurrent.TimeUnit.*;

/**
 * 连接池基类
 *
 * @Author t13max
 * @Date 10:56 2025/7/17
 */
abstract class PoolBase {

   // 日志记录器 用于打印PoolBase相关日志
   private final Logger logger = LoggerFactory.getLogger(PoolBase.class);
   // 连接池配置对象，包含各种连接池参数
   public final HikariConfig config;
   // 指标追踪委托 用于收集连接池指标
   IMetricsTrackerDelegate metricsTracker;
   // 连接池名称
   protected final String poolName;
   // 当前数据库catalog（默认数据库）
   volatile String catalog;
   // 最近一次连接失败的异常 原子引用保证线程安全
   final AtomicReference<Throwable> lastConnectionFailure;
   // 最近一次连接失败时间戳（毫秒）
   final AtomicLong connectionFailureTimestamp;
   // 获取连接的超时时间（毫秒） 超过该时间没拿到连接会报错
   long connectionTimeout;
   // 连接验证的超时时间（毫秒） 验证连接是否有效的等待时长
   long validationTimeout;
   // 连接异常覆盖策略 用于自定义异常处理
   SQLExceptionOverride exceptionOverride;

   // 需要重置的连接状态字段列表
   private static final String[] RESET_STATES = {"readOnly", "autoCommit", "isolation", "catalog", "netTimeout", "schema"};
   // 未初始化状态常量
   private static final int UNINITIALIZED = -1;
   // 布尔值常量 true为1 false为0 避免自动拆箱开销
   private static final int TRUE = 1;
   private static final int FALSE = 0;
   // 最小登录超时 秒，默认从系统属性读取，默认1秒
   private static final int MINIMUM_LOGIN_TIMEOUT = Integer.getInteger("com.zaxxer.hikari.minimumLoginTimeoutSecs", 1);

   // 网络超时时间 毫秒
   private int networkTimeout;
   // 是否支持网络超时 0未初始化 1支持 0不支持
   private volatile int isNetworkTimeoutSupported;
   // 是否支持查询超时
   private int isQueryTimeoutSupported;
   // 默认事务隔离级别
   private int defaultTransactionIsolation;
   // 当前事务隔离级别
   private int transactionIsolation;
   // 用于网络超时的执行器(线程池)
   private Executor netTimeoutExecutor;
   // 底层真实数据源
   private DataSource dataSource;
   // 连接默认的schema
   private final String schema;
   // 是否只读连接
   private final boolean isReadOnly;
   // 是否自动提交事务
   private final boolean isAutoCommit;
   // 是否使用 JDBC4 的验证方式
   private final boolean isUseJdbc4Validation;
   // 是否隔离内部查询，避免污染业务事务
   private final boolean isIsolateInternalQueries;
   // 是否已经校验连接有效性
   private volatile boolean isValidChecked;

   PoolBase(final HikariConfig config) {
      this.config = config;

      this.networkTimeout = UNINITIALIZED;
      this.catalog = config.getCatalog();
      this.schema = config.getSchema();
      this.isReadOnly = config.isReadOnly();
      this.isAutoCommit = config.isAutoCommit();
      this.exceptionOverride = config.getExceptionOverride();
      this.transactionIsolation = UtilityElf.getTransactionIsolation(config.getTransactionIsolation());

      this.isQueryTimeoutSupported = UNINITIALIZED;
      this.isNetworkTimeoutSupported = UNINITIALIZED;
      this.isUseJdbc4Validation = config.getConnectionTestQuery() == null;
      this.isIsolateInternalQueries = config.isIsolateInternalQueries();

      this.poolName = config.getPoolName();
      this.connectionTimeout = config.getConnectionTimeout();
      this.validationTimeout = config.getValidationTimeout();
      this.lastConnectionFailure = new AtomicReference<>();
      this.connectionFailureTimestamp = new AtomicLong();

      initializeDataSource();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      return poolName;
   }

   //把连接放回连接池 下次用
   abstract void recycle(final PoolEntry poolEntry);

   // ***********************************************************************
   //                           JDBC methods
   // ***********************************************************************

   // 安全地关闭连接 忽略异常并记录日志
   void quietlyCloseConnection(final Connection connection, final String closureReason) {
      if (connection != null) {
         try {
            // 打印关闭连接的debug日志
            logger.debug("{} - Closing connection {}: {}", poolName, connection, closureReason);
            // 使用try-with-resources关闭连接 即使设置网络超时失败也继续关闭
            try (connection) {
               // 如果连接未关闭 则设置网络超时时间为15秒
               if (!connection.isClosed())
                  setNetworkTimeout(connection, SECONDS.toMillis(15));
            } catch (SQLException e) {
               // 忽略设置超时时抛出的异常
            }
         } catch (Exception e) {
            // 捕获关闭连接过程中的其他异常并记录日志
            logger.debug("{} - Closing connection {} failed", poolName, connection, e);
         }
      }
   }

   // 判断连接是否已失效
   boolean isConnectionDead(final Connection connection) {
      try {
         // 设置临时的网络超时时间 用于接下来的连接校验
         setNetworkTimeout(connection, validationTimeout);

         try {
            final var validationSeconds = (int) Math.max(1000L, validationTimeout) / 1000;

            if (isUseJdbc4Validation) {
               // 使用 JDBC4 的 isValid 方法校验连接是否有效
               return !connection.isValid(validationSeconds);
            }

            // 否则使用自定义的测试语句校验连接
            try (var statement = connection.createStatement()) {
               if (isNetworkTimeoutSupported != TRUE) {
                  // 设置语句超时时间
                  setQueryTimeout(statement, validationSeconds);
               }
               // 执行测试语句
               statement.execute(config.getConnectionTestQuery());
            }
         } finally {
            // 还原之前的网络超时时间
            setNetworkTimeout(connection, networkTimeout);

            // 如果开启了内部查询隔离 且不是自动提交 则回滚当前事务
            if (isIsolateInternalQueries && !isAutoCommit) {
               connection.rollback();
            }
         }
         return false; // 校验通过 连接正常
      } catch (Exception e) {
         // 校验失败 记录异常 返回连接失效
         lastConnectionFailure.set(e);
         logger.warn("{} - Failed to validate connection {} ({}). Possibly consider using a shorter maxLifetime value.", poolName, connection, e.getMessage());
         return true;
      }
   }

   Throwable getLastConnectionFailure() {
      return lastConnectionFailure.get();
   }

   public DataSource getUnwrappedDataSource() {
      return dataSource;
   }

   // ***********************************************************************
   //                         PoolEntry methods
   // ***********************************************************************

   PoolEntry newPoolEntry(final boolean isEmptyPool) throws Exception {
      return new PoolEntry(newConnection(isEmptyPool), this, isReadOnly, isAutoCommit);
   }

   // 重置连接状态 将连接恢复到初始配置状态
   void resetConnectionState(final Connection connection, final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException {
      int resetBits = 0; // 用于记录实际被重置的状态

      // 如果只读状态被修改过且当前与初始值不一致 则重置
      if ((dirtyBits & DIRTY_BIT_READONLY) != 0 && proxyConnection.getReadOnlyState() != isReadOnly) {
         connection.setReadOnly(isReadOnly);
         resetBits |= DIRTY_BIT_READONLY;
      }

      // 如果自动提交状态被修改过且当前与初始值不一致 则重置
      if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0 && proxyConnection.getAutoCommitState() != isAutoCommit) {
         connection.setAutoCommit(isAutoCommit);
         resetBits |= DIRTY_BIT_AUTOCOMMIT;
      }

      // 如果事务隔离级别被修改过且当前与初始值不一致 则重置
      if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0 && proxyConnection.getTransactionIsolationState() != transactionIsolation) {
         connection.setTransactionIsolation(transactionIsolation);
         resetBits |= DIRTY_BIT_ISOLATION;
      }

      // 如果 catalog 被修改过且当前与初始值不一致 则重置
      if ((dirtyBits & DIRTY_BIT_CATALOG) != 0 && catalog != null && !catalog.equals(proxyConnection.getCatalogState())) {
         connection.setCatalog(catalog);
         resetBits |= DIRTY_BIT_CATALOG;
      }

      // 如果网络超时被修改过且当前与初始值不一致 则重置
      if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0 && proxyConnection.getNetworkTimeoutState() != networkTimeout) {
         setNetworkTimeout(connection, networkTimeout);
         resetBits |= DIRTY_BIT_NETTIMEOUT;
      }

      // 如果 schema 被修改过且当前与初始值不一致 则重置
      if ((dirtyBits & DIRTY_BIT_SCHEMA) != 0 && schema != null && !schema.equals(proxyConnection.getSchemaState())) {
         connection.setSchema(schema);
         resetBits |= DIRTY_BIT_SCHEMA;
      }

      // 如果有字段被重置 打日志
      if (resetBits != 0 && logger.isDebugEnabled()) {
         logger.debug("{} - Reset ({}) on connection {}", poolName, stringFromResetBits(resetBits), connection);
      }
   }

   void shutdownNetworkTimeoutExecutor() {
      isNetworkTimeoutSupported = UNINITIALIZED;
      if (netTimeoutExecutor instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) netTimeoutExecutor).shutdownNow();
      }
   }

   long getLoginTimeout() {
      try {
         return (dataSource != null) ? dataSource.getLoginTimeout() : SECONDS.toSeconds(5);
      } catch (SQLException e) {
         return SECONDS.toSeconds(5);
      }
   }

   // ***********************************************************************
   //                       JMX methods
   // ***********************************************************************

   /**
    * Register MBeans for HikariConfig and HikariPool.
    * 处理JMX MBean的注册和注销
    *
    * @param hikariPool a HikariPool instance
    */
   void handleMBeans(final HikariPool hikariPool, final boolean register) {
      // 如果配置不允许注册MBean 直接返回
      if (!config.isRegisterMbeans()) {
         return;
      }
      try {
         // 获取平台MBean服务器实例
         final var mBeanServer = ManagementFactory.getPlatformMBeanServer();

         ObjectName beanConfigName, beanPoolName;

         // 判断是否使用JMX 2.0规范格式命名
         if ("true".equals(System.getProperty("hikaricp.jmx.register2.0"))) {
            beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig,name=" + poolName);
            beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool,name=" + poolName);
         } else {
            beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (" + poolName + ")");
            beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + poolName + ")");
         }

         if (register) {
            // 注册MBean 如果未注册过
            if (!mBeanServer.isRegistered(beanConfigName)) {
               mBeanServer.registerMBean(config, beanConfigName);
               mBeanServer.registerMBean(hikariPool, beanPoolName);
            } else {
               // 已经注册过日志报错
               logger.error("{} - JMX name ({}) is already registered.", poolName, poolName);
            }
         } else if (mBeanServer.isRegistered(beanConfigName)) {
            // 注销MBean
            mBeanServer.unregisterMBean(beanConfigName);
            mBeanServer.unregisterMBean(beanPoolName);
         }
      } catch (Exception e) {
         // 注册或注销失败 打警告日志
         logger.warn("{} - Failed to {} management beans.", poolName, (register ? "register" : "unregister"), e);
      }
   }

   // ***********************************************************************
   //                          Private methods
   // ***********************************************************************

   /**
    * Create/initialize the underlying DataSource.
    * 初始化数据源
    */
   private void initializeDataSource() {
      // 获取配置项
      final var jdbcUrl = config.getJdbcUrl();
      final var credentials = config.getCredentials();
      final var dsClassName = config.getDataSourceClassName();
      final var driverClassName = config.getDriverClassName();
      final var dataSourceJNDI = config.getDataSourceJNDI();
      final var dataSourceProperties = config.getDataSourceProperties();
      // 获取已有的数据源实例
      var ds = config.getDataSource();
      // 如果指定了DataSource类名但未提供实例 通过反射创建并设置属性
      if (dsClassName != null && ds == null) {
         ds = UtilityElf.createInstance(dsClassName, DataSource.class);
         PropertyElf.setTargetFromProperties(ds, dataSourceProperties);
      }
      // 如果提供了JDBC URL但未提供DataSource实例 使用DriverDataSource包装
      else if (jdbcUrl != null && ds == null) {
         ds = new DriverDataSource(jdbcUrl, driverClassName, dataSourceProperties, credentials.getUsername(), credentials.getPassword());
      }
      // 如果配置了JNDI名称且未提供实例 从JNDI中查找
      else if (dataSourceJNDI != null && ds == null) {
         try {
            var ic = new InitialContext();
            ds = (DataSource) ic.lookup(dataSourceJNDI);
         } catch (NamingException e) {
            throw new PoolInitializationException(e);
         }
      }
      // 如果成功创建了DataSource 设置登录超时和网络超时执行器
      if (ds != null) {
         setLoginTimeout(ds);
         createNetworkTimeoutExecutor(ds, dsClassName, jdbcUrl);
      }
      this.dataSource = ds;
   }

   /**
    * Obtain connection from data source.
    * 创建新的数据库连接
    *
    * @return a Connection
    */
   private Connection newConnection(final boolean isEmptyPool) throws Exception {
      // 记录开始时间和连接唯一ID
      final var start = currentTime();
      final var id = java.util.UUID.randomUUID();

      Connection connection = null;
      try {
         // 获取用户名密码
         final var credentials = config.getCredentials();
         final var username = credentials.getUsername();
         final var password = credentials.getPassword();

         logger.debug("{} - Attempting to create/setup new connection ({})", poolName, id);

         // 获取连接 如果未指定用户名则调用无参方法
         connection = (username == null) ? dataSource.getConnection() : dataSource.getConnection(username, password);
         if (connection == null) {
            throw new SQLTransientConnectionException("DataSource returned null unexpectedly");
         }

         // 设置连接属性 如只读 事务隔离等
         setupConnection(connection);

         // 清除连接失败记录
         lastConnectionFailure.set(null);
         connectionFailureTimestamp.set(0);

         logger.debug("{} - Established new connection ({})", poolName, id);
         return connection;
      } catch (Throwable t) {
         logger.debug("{} - Failed to create/setup connection ({}): {}", poolName, id, t.getMessage());

         // 记录失败时间
         connectionFailureTimestamp.compareAndSet(0, start);
         // 如果连接池为空且失败超过1分钟 打日志
         if (isEmptyPool && elapsedMillis(connectionFailureTimestamp.get()) > MINUTES.toMillis(1)) {
            logger.warn("{} - Pool is empty, failed to create/setup connection ({})", poolName, id, t);
            connectionFailureTimestamp.set(0);
         }

         // 如果已获取到连接 尝试关闭
         if (connection != null) {
            quietlyCloseConnection(connection, "(Failed to create/setup connection (".concat(id.toString()).concat(")"));
         }

         // 记录最后一次异常
         lastConnectionFailure.set(t);
         throw t;
      } finally {
         // 记录连接创建耗时
         if (metricsTracker != null) {
            metricsTracker.recordConnectionCreated(elapsedMillis(start));
         }
      }
   }

   /**
    * Set up a connection initial state.
    * 设置连接属性
    *
    * @param connection a Connection
    * @throws ConnectionSetupException thrown if any exception is encountered
    */
   private void setupConnection(final Connection connection) throws ConnectionSetupException {
      try {
         if (networkTimeout == UNINITIALIZED) {
            networkTimeout = getAndSetNetworkTimeout(connection, validationTimeout);
         } else {
            setNetworkTimeout(connection, validationTimeout);
         }

         if (connection.isReadOnly() != isReadOnly) {
            connection.setReadOnly(isReadOnly);
         }

         if (connection.getAutoCommit() != isAutoCommit) {
            connection.setAutoCommit(isAutoCommit);
         }

         checkDriverSupport(connection);

         if (transactionIsolation != defaultTransactionIsolation) {
            //noinspection MagicConstant
            connection.setTransactionIsolation(transactionIsolation);
         }

         if (catalog != null) {
            connection.setCatalog(catalog);
         }

         if (schema != null) {
            connection.setSchema(schema);
         }

         executeSql(connection, config.getConnectionInitSql(), true);

         setNetworkTimeout(connection, networkTimeout);
      } catch (SQLException e) {
         throw new ConnectionSetupException(e);
      }
   }

   /**
    * Execute isValid() or connection test query.
    *
    * @param connection a Connection to check
    */
   private void checkDriverSupport(final Connection connection) throws SQLException {
      if (!isValidChecked) {
         //是否支持isValid
         checkValidationSupport(connection);
         //检查默认隔离级别
         checkDefaultIsolation(connection);
         isValidChecked = true;
      }
   }

   /**
    * Check whether Connection.isValid() is supported, or that the user has test query configured.
    *
    * @param connection a Connection to check
    * @throws SQLException rethrown from the driver
    */
   private void checkValidationSupport(final Connection connection) throws SQLException {
      try {
         if (isUseJdbc4Validation) {
            connection.isValid(Math.max(1, (int) MILLISECONDS.toSeconds(validationTimeout)));
         } else {
            executeSql(connection, config.getConnectionTestQuery(), false);
         }
      } catch (Exception | AbstractMethodError e) {
         logger.error("{} - Failed to execute{} connection test query ({}).", poolName, (isUseJdbc4Validation ? " isValid() for connection, configure" : ""), e.getMessage());
         throw e;
      }
   }

   /**
    * Check the default transaction isolation of the Connection.
    *
    * @param connection a Connection to check
    * @throws SQLException rethrown from the driver
    */
   private void checkDefaultIsolation(final Connection connection) throws SQLException {
      try {
         defaultTransactionIsolation = connection.getTransactionIsolation();
         if (transactionIsolation == -1) {
            transactionIsolation = defaultTransactionIsolation;
         }
      } catch (SQLException e) {
         logger.warn("{} - Default transaction isolation level detection failed ({}).", poolName, e.getMessage());
         if (e.getSQLState() != null && !e.getSQLState().startsWith("08")) {
            throw e;
         }
      }
   }

   /**
    * Set the query timeout, if it is supported by the driver.
    * 设置查询超时
    *
    * @param statement  a statement to set the query timeout on
    * @param timeoutSec the number of seconds before timeout
    */
   private void setQueryTimeout(final Statement statement, final int timeoutSec) {
      if (isQueryTimeoutSupported != FALSE) {
         try {
            statement.setQueryTimeout(timeoutSec);
            isQueryTimeoutSupported = TRUE;
         } catch (Exception e) {
            if (isQueryTimeoutSupported == UNINITIALIZED) {
               isQueryTimeoutSupported = FALSE;
               logger.info("{} - Failed to set query timeout for statement. ({})", poolName, e.getMessage());
            }
         }
      }
   }

   /**
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.  Return the pre-existing value of the network timeout.
    * 设置网络超时
    *
    * @param connection the connection to set the network timeout on
    * @param timeoutMs  the number of milliseconds before timeout
    * @return the pre-existing network timeout value
    */
   private int getAndSetNetworkTimeout(final Connection connection, final long timeoutMs) {
      if (isNetworkTimeoutSupported != FALSE) {
         try {
            final var originalTimeout = connection.getNetworkTimeout();
            connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
            isNetworkTimeoutSupported = TRUE;
            return originalTimeout;
         } catch (Exception | AbstractMethodError e) {
            if (isNetworkTimeoutSupported == UNINITIALIZED) {
               isNetworkTimeoutSupported = FALSE;

               logger.info("{} - Driver does not support get/set network timeout for connections. ({})", poolName, e.getMessage());
               if (validationTimeout < SECONDS.toMillis(1)) {
                  logger.warn("{} - A validationTimeout of less than 1 second cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               } else if (validationTimeout % SECONDS.toMillis(1) != 0) {
                  logger.warn("{} - A validationTimeout with fractional second granularity cannot be honored on drivers without setNetworkTimeout() support.", poolName);
               }
            }
         }
      }

      return 0;
   }

   /**
    * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
    * driver supports it.
    * 设置数据库连接的网络读写超时时间
    * 超过这个时间未完成操作 JDBC 驱动会抛出异常 避免连接长时间卡死在网络IO上
    *
    * @param connection the connection to set the network timeout on
    * @param timeoutMs  the number of milliseconds before timeout
    * @throws SQLException throw if the connection.setNetworkTimeout() call throws
    */
   private void setNetworkTimeout(final Connection connection, final long timeoutMs) throws SQLException {
      if (isNetworkTimeoutSupported == TRUE) {
         connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
      }
   }

   /**
    * Execute the user-specified init SQL.
    * 执行指定的sql
    *
    * @param connection the connection to initialize
    * @param sql        the SQL to execute
    * @param isCommit   whether to commit the SQL after execution or not
    * @throws SQLException throws if the init SQL execution fails
    */
   private void executeSql(final Connection connection, final String sql, final boolean isCommit) throws SQLException {
      if (sql != null) {
         try (var statement = connection.createStatement()) {
            // connection was created a few milliseconds before, so set query timeout is omitted (we assume it will succeed)
            statement.execute(sql);
         }

         if (isIsolateInternalQueries && !isAutoCommit) {
            if (isCommit) {
               connection.commit();
            } else {
               connection.rollback();
            }
         }
      }
   }

   /**
    * 创建设置networkTimeout时用的Executor
    * 如果DataSource支持setNetworkTimeout方法 则需要传入一个Executor参数
    *
    * @Author t13max
    * @Date 11:47 2025/7/17
    */
   private void createNetworkTimeoutExecutor(final DataSource dataSource, final String dsClassName, final String jdbcUrl) {
      // Temporary hack for MySQL issue: http://bugs.mysql.com/bug.php?id=75615
      if ((dsClassName != null && dsClassName.contains("Mysql")) ||
         (jdbcUrl != null && jdbcUrl.contains("mysql")) ||
         (dataSource != null && dataSource.getClass().getName().contains("Mysql"))) {
         netTimeoutExecutor = new SynchronousExecutor();
      } else {
         ThreadFactory threadFactory = config.getThreadFactory();
         threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory(poolName + ":network-timeout-executor");
         ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
         executor.setKeepAliveTime(15, SECONDS);
         executor.allowCoreThreadTimeOut(true);
         netTimeoutExecutor = executor;
      }
   }

   /**
    * Set the loginTimeout on the specified DataSource.
    * 在指定的数据源上设置超时时间
    *
    * @param dataSource the DataSource
    */
   private void setLoginTimeout(final DataSource dataSource) {
      if (connectionTimeout != Integer.MAX_VALUE) {
         try {
            dataSource.setLoginTimeout(Math.max(MINIMUM_LOGIN_TIMEOUT, (int) MILLISECONDS.toSeconds(500L + connectionTimeout)));
         } catch (Exception e) {
            logger.info("{} - Failed to set login timeout for data source. ({})", poolName, e.getMessage());
         }
      }
   }

   /**
    * This will create a string for debug logging. Given a set of "reset bits", this
    * method will return a concatenated string, for example:
    * <p
    * Input : 0b00110
    * Output: "autoCommit, isolation"
    * 根据bits返回字符串
    *
    * @param bits a set of "reset bits"
    * @return a string of which states were reset
    */
   private String stringFromResetBits(final int bits) {

      //高效拼接字符串
      final var sb = new StringJoiner(", ");
      for (int ndx = 0; ndx < RESET_STATES.length; ndx++) {
         if ((bits & (0b1 << ndx)) != 0) {
            sb.add(RESET_STATES[ndx]);
         }
      }

      return sb.toString();
   }

   // ***********************************************************************
   //                      Private Static Classes
   // ***********************************************************************

   //连接初始化或配置失败
   static class ConnectionSetupException extends Exception {
      private static final long serialVersionUID = 929872118275916521L;

      ConnectionSetupException(Throwable t) {
         super(t);
      }
   }

   /**
    * Special executor used only to work around a MySQL issue that has not been addressed.
    * MySQL issue: <a href="http://bugs.mysql.com/bug.php?id=75615">...</a>
    * 同步线程池 在用户线程直接执行
    */
   private static class SynchronousExecutor implements Executor {
      /**
       * {@inheritDoc}
       */
      @Override
      @SuppressWarnings("NullableProblems")
      public void execute(Runnable command) {
         try {
            command.run();
         } catch (Exception t) {
            LoggerFactory.getLogger(PoolBase.class).debug("Failed to execute: {}", command, t);
         }
      }
   }

   //自定义监控指标采集的接口
   interface IMetricsTrackerDelegate extends AutoCloseable {

      //记录连接被使用
      default void recordConnectionUsage(PoolEntry poolEntry) {
      }

      //记录连接创建
      default void recordConnectionCreated(long connectionCreatedMillis) {
      }

      //记录borrow超时
      default void recordBorrowTimeoutStats(long startTime) {
      }

      //记录borrow状态
      default void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {
      }

      //记录连接超时
      default void recordConnectionTimeout() {
      }

      //关闭
      @Override
      default void close() {
      }
   }

   /**
    * A class that delegates to a MetricsTracker implementation.  The use of a delegate
    * allows us to use the NopMetricsTrackerDelegate when metrics are disabled, which in
    * turn allows the JIT to completely optimize away to callsites to record metrics.
    * 适配器 对外统一接口
    * 内部可以接第三方监控系统实现
    */
   static class MetricsTrackerDelegate implements IMetricsTrackerDelegate {
      final IMetricsTracker tracker;

      MetricsTrackerDelegate(IMetricsTracker tracker) {
         this.tracker = tracker;
      }

      @Override
      public void recordConnectionUsage(final PoolEntry poolEntry) {
         tracker.recordConnectionUsageMillis(poolEntry.getMillisSinceBorrowed());
      }

      @Override
      public void recordConnectionCreated(long connectionCreatedMillis) {
         tracker.recordConnectionCreatedMillis(connectionCreatedMillis);
      }

      @Override
      public void recordBorrowTimeoutStats(long startTime) {
         tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime));
      }

      @Override
      public void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {
         final var now = currentTime();
         poolEntry.lastBorrowed = now;
         tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime, now));
      }

      @Override
      public void recordConnectionTimeout() {
         tracker.recordConnectionTimeout();
      }

      @Override
      public void close() {
         tracker.close();
      }
   }

   /**
    * A no-op implementation of the IMetricsTrackerDelegate that is used when metrics capture is
    * disabled.
    */
   static final class NopMetricsTrackerDelegate implements IMetricsTrackerDelegate {
   }
}
