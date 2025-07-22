/*
 * Copyright (C) 2013,2014 Brett Wooldridge
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleHealthChecker;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleMetricsTrackerFactory;
import com.zaxxer.hikari.metrics.dropwizard.Dropwizard5MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import com.zaxxer.hikari.util.ConcurrentBag;
import com.zaxxer.hikari.util.ConcurrentBag.IBagStateListener;
import com.zaxxer.hikari.util.SuspendResumeLock;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.zaxxer.hikari.util.ClockSource.*;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.UtilityElf.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This is the primary connection pool class that provides the basic
 * pooling behavior for HikariCP.
 * Hikari连接池
 *
 * @author Brett Wooldridge
 */
public final class HikariPool extends PoolBase implements HikariPoolMXBean, IBagStateListener {

   // 日志记录器 用于打印HikariPool相关日志
   private final Logger logger = LoggerFactory.getLogger(HikariPool.class);

   // 连接池状态常量 0正常 1挂起 2关闭
   public static final int POOL_NORMAL = 0;
   public static final int POOL_SUSPENDED = 1;
   public static final int POOL_SHUTDOWN = 2;

   // 当前连接池状态 volatile保证线程安全可见
   public volatile int poolState;
   // 连接存活时间旁路窗口毫秒数（默认500ms）用于优化连接验证频率
   private final long aliveBypassWindowMs = Long.getLong("com.zaxxer.hikari.aliveBypassWindowMs", MILLISECONDS.toMillis(500));
   // 定时任务执行周期(默认30秒)
   private final long housekeepingPeriodMs = Long.getLong("com.zaxxer.hikari.housekeeping.periodMs", SECONDS.toMillis(30));
   // 连接寿命时间方差因子 用于连接回收时间随机化 防止同时过期(范围2~40 默认4)
   private final long lifeTimeVarianceFactor = Math.min(40, Math.max(2, Long.getLong("com.zaxxer.hikari.lifeTimeVarianceFactor", 4)));
   // 是否启用请求边界功能(默认关闭)
   private final boolean isRequestBoundariesEnabled = Boolean.getBoolean("com.zaxxer.hikari.enableRequestBoundaries");

   // 驱逐连接时日志信息
   private static final String EVICTED_CONNECTION_MESSAGE = "(connection was evicted)";
   private static final String DEAD_CONNECTION_MESSAGE = "(connection is dead)";

   // 连接池连接创建器 实例化新的连接对象
   private final PoolEntryCreator poolEntryCreator = new PoolEntryCreator();
   // 连接池补充连接时用的创建器(带前缀日志)
   private final PoolEntryCreator postFillPoolEntryCreator = new PoolEntryCreator("After adding ");
   // 创建连接线程池 执行连接创建任务
   private final ThreadPoolExecutor addConnectionExecutor;
   // 关闭连接线程池 执行连接关闭任务
   private final ThreadPoolExecutor closeConnectionExecutor;
   // 连接池中所有连接的容器(线程安全)
   private final ConcurrentBag<PoolEntry> connectionBag;
   // 代理连接泄漏检测任务工厂
   private final ProxyLeakTaskFactory leakTaskFactory;
   // 挂起恢复锁 控制连接池暂停和恢复状态
   private final SuspendResumeLock suspendResumeLock;
   // 定时任务执行器 用于执行维护任务(清理 健康检查等)
   private final ScheduledExecutorService houseKeepingExecutorService;
   // 维护任务(清理 健康检查等) 的 定时任务句柄
   private ScheduledFuture<?> houseKeeperTask;

   /**
    * Construct a HikariPool with the specified configuration.
    *
    * @param config a HikariConfig instance
    */
   public HikariPool(final HikariConfig config) {
      super(config);

      //连接池容器
      this.connectionBag = new ConcurrentBag<>(this);

      this.suspendResumeLock = config.isAllowPoolSuspension() ? new SuspendResumeLock() : SuspendResumeLock.FAUX_LOCK;

      this.houseKeepingExecutorService = initializeHouseKeepingExecutorService();

      checkFailFast();

      if (config.getMetricsTrackerFactory() != null) {
         setMetricsTrackerFactory(config.getMetricsTrackerFactory());
      } else {
         setMetricRegistry(config.getMetricRegistry());
      }

      setHealthCheckRegistry(config.getHealthCheckRegistry());

      handleMBeans(this, true);

      //线程工厂
      ThreadFactory threadFactory = config.getThreadFactory();
      //最大池大小
      final int maxPoolSize = config.getMaximumPoolSize();
      //创建连接线程池 执行创建任务的
      this.addConnectionExecutor = createThreadPoolExecutor(maxPoolSize, poolName + ":connection-adder", threadFactory, new CustomDiscardPolicy());
      //关闭
      this.closeConnectionExecutor = createThreadPoolExecutor(maxPoolSize, poolName + ":connection-closer", threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
      //泄漏检测任务工厂
      this.leakTaskFactory = new ProxyLeakTaskFactory(config.getLeakDetectionThreshold(), houseKeepingExecutorService);
      //清理任务
      this.houseKeeperTask = houseKeepingExecutorService.scheduleWithFixedDelay(new HouseKeeper(), 100L, housekeepingPeriodMs, MILLISECONDS);

      //阻塞直到失败
      if (Boolean.getBoolean("com.zaxxer.hikari.blockUntilFilled") && config.getInitializationFailTimeout() > 1) {

         //设置最大和核心数量
         addConnectionExecutor.setMaximumPoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));
         addConnectionExecutor.setCorePoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));

         final long startTime = currentTime();
         //等待
         while (elapsedMillis(startTime) < config.getInitializationFailTimeout() && getTotalConnections() < config.getMinimumIdle()) {
            quietlySleep(MILLISECONDS.toMillis(100));
         }

         //修改回去
         addConnectionExecutor.setCorePoolSize(1);
         addConnectionExecutor.setMaximumPoolSize(1);
      }
   }

   /**
    * Get a connection from the pool, or timeout after connectionTimeout milliseconds.
    * 获取一个连接 默认超时
    *
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   public Connection getConnection() throws SQLException {
      return getConnection(connectionTimeout);
   }

   /**
    * Get a connection from the pool, or timeout after the specified number of milliseconds.
    * 获取一个连接 带超时时间
    *
    * @param hardTimeout the maximum time to wait for a connection from the pool
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   public Connection getConnection(final long hardTimeout) throws SQLException {
      suspendResumeLock.acquire();
      final var startTime = currentTime();

      try {
         var timeout = hardTimeout;
         do {
            //从Bag里borrow一个连接
            var poolEntry = connectionBag.borrow(timeout, MILLISECONDS);

            //超时没拿到 break
            if (poolEntry == null) {
               break; // We timed out... break and throw exception
            }

            final var now = currentTime();
            //链接已经标记为驱逐 大于链接存活时间 连接寄了
            if (poolEntry.isMarkedEvicted() || (elapsedMillis(poolEntry.lastAccessed, now) > aliveBypassWindowMs && isConnectionDead(poolEntry.connection))) {
               //关闭
               closeConnection(poolEntry, poolEntry.isMarkedEvicted() ? EVICTED_CONNECTION_MESSAGE : DEAD_CONNECTION_MESSAGE);
               //重新设置超时时间
               timeout = hardTimeout - elapsedMillis(startTime);
            } else {
               //记录成功borrow状态
               metricsTracker.recordBorrowStats(poolEntry, startTime);
               //请求边界
               if (isRequestBoundariesEnabled) {
                  try {
                     //通知 开始request
                     poolEntry.connection.beginRequest();
                  } catch (SQLException e) {
                     logger.warn("beginRequest Failed for: {}, ({})", poolEntry.connection, e.getMessage());
                  }
               }
               //创建代理连接
               return poolEntry.createProxyConnection(leakTaskFactory.schedule(poolEntry));
            }
         } while (timeout > 0L);

         //记录borrow超时
         metricsTracker.recordBorrowTimeoutStats(startTime);
         throw createTimeoutException(startTime);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new SQLException(poolName + " - Interrupted during connection acquisition", e);
      } finally {
         suspendResumeLock.release();
      }
   }

   /**
    * Shutdown the pool, closing all idle connections and aborting or closing
    * active connections.
    * 关闭连接池 关闭所有空闲链接 中止或关闭活跃连接
    *
    * @throws InterruptedException thrown if the thread is interrupted during shutdown
    */
   public synchronized void shutdown() throws InterruptedException {

      try {
         poolState = POOL_SHUTDOWN;

         if (addConnectionExecutor == null) { // pool never started
            return;
         }

         logPoolState("Before shutdown ");

         if (houseKeeperTask != null) {
            houseKeeperTask.cancel(false);
            houseKeeperTask = null;
         }

         softEvictConnections();

         addConnectionExecutor.shutdown();
         if (!addConnectionExecutor.awaitTermination(getLoginTimeout(), SECONDS)) {
            logger.warn("Timed-out waiting for add connection executor to shutdown");
         }

         destroyHouseKeepingExecutorService();

         connectionBag.close();

         final var assassinExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(), poolName + ":connection-assassinator",
            config.getThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
         try {
            final var start = currentTime();
            do {
               //强制终端活动链接
               abortActiveConnections(assassinExecutor);
               //软驱逐链接
               softEvictConnections();
            } while (getTotalConnections() > 0 && elapsedMillis(start) < SECONDS.toMillis(10));
         } finally {
            assassinExecutor.shutdown();
            if (!assassinExecutor.awaitTermination(10L, SECONDS)) {
               logger.warn("Timed-out waiting for connection assassin to shutdown");
            }
         }

         shutdownNetworkTimeoutExecutor();
         closeConnectionExecutor.shutdown();
         if (!closeConnectionExecutor.awaitTermination(10L, SECONDS)) {
            logger.warn("Timed-out waiting for close connection executor to shutdown");
         }
      } finally {
         logPoolState("After  shutdown ");
         handleMBeans(this, false);
         metricsTracker.close();
      }
   }

   /**
    * Evict a Connection from the pool.
    *
    * @param connection the Connection to evict (actually a {@link ProxyConnection})
    */
   public void evictConnection(Connection connection) {

      var proxyConnection = (ProxyConnection) connection;
      //取消泄漏检测
      proxyConnection.cancelLeakTask();

      try {
         //软驱逐
         softEvictConnection(proxyConnection.getPoolEntry(), "(connection evicted by user)", !connection.isClosed() /* owner */);
      } catch (SQLException e) {
         // unreachable in HikariCP, but we're still forced to catch it
      }
   }

   /**
    * Set a metrics registry to be used when registering metrics collectors.  The HikariDataSource prevents this
    * method from being called more than once.
    *
    * @param metricRegistry the metrics registry instance to use
    */
   @SuppressWarnings("PackageAccessibility")
   public void setMetricRegistry(Object metricRegistry) {
      if (metricRegistry != null && safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")) {
         setMetricsTrackerFactory(new CodahaleMetricsTrackerFactory((MetricRegistry) metricRegistry));
      } else if (metricRegistry != null && safeIsAssignableFrom(metricRegistry, "io.dropwizard.metrics5.MetricRegistry")) {
         setMetricsTrackerFactory(new Dropwizard5MetricsTrackerFactory((io.dropwizard.metrics5.MetricRegistry) metricRegistry));
      } else if (metricRegistry != null && safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry")) {
         setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory((MeterRegistry) metricRegistry));
      } else {
         setMetricsTrackerFactory(null);
      }
   }

   /**
    * Set the MetricsTrackerFactory to be used to create the IMetricsTracker instance used by the pool.
    *
    * @param metricsTrackerFactory an instance of a class that subclasses MetricsTrackerFactory
    */
   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
      if (metricsTrackerFactory != null) {
         this.metricsTracker = new MetricsTrackerDelegate(metricsTrackerFactory.create(config.getPoolName(), getPoolStats()));
      } else {
         this.metricsTracker = new NopMetricsTrackerDelegate();
      }
   }

   /**
    * Set the health check registry to be used when registering health checks.  Currently only Codahale health
    * checks are supported.
    *
    * @param healthCheckRegistry the health check registry instance to use
    */
   public void setHealthCheckRegistry(Object healthCheckRegistry) {
      if (healthCheckRegistry != null) {
         CodahaleHealthChecker.registerHealthChecks(this, config, (HealthCheckRegistry) healthCheckRegistry);
      }
   }

   // ***********************************************************************
   //                        IBagStateListener callback
   // ***********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public void addBagItem(final int waiting) {
      if (waiting > addConnectionExecutor.getQueue().size())
         addConnectionExecutor.submit(poolEntryCreator);
   }

   // ***********************************************************************
   //                        HikariPoolMBean methods
   // ***********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public int getActiveConnections() {
      return connectionBag.getCount(STATE_IN_USE);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getIdleConnections() {
      return connectionBag.getCount(STATE_NOT_IN_USE);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getTotalConnections() {
      return connectionBag.size();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getThreadsAwaitingConnection() {
      return connectionBag.getWaitingThreadCount();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void softEvictConnections() {
      connectionBag.values().forEach(poolEntry -> softEvictConnection(poolEntry, "(connection evicted)", false /* not owner */));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public synchronized void suspendPool() {
      if (suspendResumeLock == SuspendResumeLock.FAUX_LOCK) {
         throw new IllegalStateException(poolName + " - is not suspendable");
      } else if (poolState != POOL_SUSPENDED) {
         suspendResumeLock.suspend();
         poolState = POOL_SUSPENDED;
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public synchronized void resumePool() {
      if (poolState == POOL_SUSPENDED) {
         poolState = POOL_NORMAL;
         fillPool(false);
         suspendResumeLock.resume();
      }
   }

   // ***********************************************************************
   //                           Package methods
   // ***********************************************************************

   /**
    * Log the current pool state at debug level.
    * 打印当前池子状态
    *
    * @param prefix an optional prefix to prepend the log message
    */
   void logPoolState(String... prefix) {
      if (logger.isDebugEnabled()) {
         logger.debug("{} - {}stats (total={}/{}, idle={}/{}, active={}, waiting={})",
            poolName, (prefix.length > 0 ? prefix[0] : ""),
            getTotalConnections(), config.getMaximumPoolSize(), getIdleConnections(), config.getMinimumIdle(), getActiveConnections(), getThreadsAwaitingConnection());
      }
   }

   /**
    * Recycle PoolEntry (add back to the pool)
    * 回收一个连接 返还给池子
    *
    * @param poolEntry the PoolEntry to recycle
    */
   @Override
   void recycle(final PoolEntry poolEntry) {
      //记录链接被使用
      metricsTracker.recordConnectionUsage(poolEntry);
      //已经被驱逐 关闭链接
      if (poolEntry.isMarkedEvicted()) {
         closeConnection(poolEntry, EVICTED_CONNECTION_MESSAGE);
      } else {
         //处理边界
         if (isRequestBoundariesEnabled) {
            try {
               poolEntry.connection.endRequest();
            } catch (SQLException e) {
               logger.warn("endRequest Failed for: {},({})", poolEntry.connection, e.getMessage());
            }
         }
         connectionBag.requite(poolEntry);
      }
   }

   /**
    * Permanently close the real (underlying) connection (eat any exception).
    * 关闭底层链接
    *
    * @param poolEntry     poolEntry having the connection to close
    * @param closureReason reason to close
    */
   void closeConnection(final PoolEntry poolEntry, final String closureReason) {

      if (connectionBag.remove(poolEntry)) {
         final var connection = poolEntry.close();
         closeConnectionExecutor.execute(() -> {
            quietlyCloseConnection(connection, closureReason);
            if (poolState == POOL_NORMAL) {
               fillPool(false);
            }
         });
      }
   }

   @SuppressWarnings("unused")
   int[] getPoolStateCounts() {
      return connectionBag.getStateCounts();
   }


   // ***********************************************************************
   //                           Private methods
   // ***********************************************************************

   /**
    * Creating new poolEntry. If maxLifetime is configured, create a future End-of-life task with variance from
    * the maxLifetime time to ensure there is no massive die-off of Connections in the pool.
    * 创建新的 poolEntry。如果配置了 maxLifetime，创建一个与 maxLifetime 时间不同的未来生命周期结束任务 防止大规模死亡
    */
   private PoolEntry createPoolEntry() {
      try {
         final var poolEntry = newPoolEntry(getTotalConnections() == 0);

         final var maxLifetime = config.getMaxLifetime();
         if (maxLifetime > 0) {
            // default variance upto 25% of the maxLifetime (random)
            final var variance = maxLifetime > 10_000L ? ThreadLocalRandom.current().nextLong(maxLifetime / lifeTimeVarianceFactor) : 0L;
            final var lifetime = maxLifetime - variance;
            poolEntry.setFutureEol(houseKeepingExecutorService.schedule(new MaxLifetimeTask(poolEntry), lifetime, MILLISECONDS));
         }

         final long keepaliveTime = config.getKeepaliveTime();
         if (keepaliveTime > 0) {
            // variance up to 20% of the heartbeat time
            final var variance = ThreadLocalRandom.current().nextLong(keepaliveTime / 5);
            final var heartbeatTime = keepaliveTime - variance;
            poolEntry.setKeepalive(houseKeepingExecutorService.scheduleWithFixedDelay(new KeepaliveTask(poolEntry), heartbeatTime, heartbeatTime, MILLISECONDS));
         }

         return poolEntry;
      } catch (ConnectionSetupException e) {
         if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            logger.debug("{} - Error thrown while acquiring connection from data source", poolName, e.getCause());
         }
      } catch (Throwable e) {
         if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            logger.debug("{} - Cannot acquire connection from data source", poolName, e);
         }
      }

      return null;
   }

   /**
    * Fill pool up from current idle connections (as they are perceived at the point of execution) to minimumIdle connections.
    * 补足空闲连接数
    */
   private synchronized void fillPool(final boolean isAfterAdd) {
      final var idle = getIdleConnections();
      final var shouldAdd = getTotalConnections() < config.getMaximumPoolSize() && idle < config.getMinimumIdle();

      if (shouldAdd) {
         final var countToAdd = config.getMinimumIdle() - idle;
         for (int i = 0; i < countToAdd; i++)
            addConnectionExecutor.submit(isAfterAdd ? postFillPoolEntryCreator : poolEntryCreator);
      } else if (isAfterAdd) {
         logger.debug("{} - Fill pool skipped, pool has sufficient level or currently being filled.", poolName);
      }
   }

   /**
    * Attempt to abort or close active connections.
    *
    * @param assassinExecutor the ExecutorService to pass to Connection.abort()
    */
   private void abortActiveConnections(final ExecutorService assassinExecutor) {
      for (var poolEntry : connectionBag.values(STATE_IN_USE)) {
         Connection connection = poolEntry.close();
         try {
            connection.abort(assassinExecutor);
         } catch (Throwable e) {
            quietlyCloseConnection(connection, "(connection aborted during shutdown)");
         } finally {
            connectionBag.remove(poolEntry);
         }
      }
   }

   /**
    * If {@code initializationFailTimeout} is configured, check that we have DB connectivity.
    * 快速检查数据库连接是否可用 连不上立即失败
    *
    * @throws PoolInitializationException if fails to create or validate connection
    * @see HikariConfig#setInitializationFailTimeout(long)
    */
   private void checkFailFast() {

      //初始化超时时间
      final var initializationFailTimeout = config.getInitializationFailTimeout();
      if (initializationFailTimeout < 0) {
         return;
      }

      final var startTime = currentTime();

      do {
         final var poolEntry = createPoolEntry();
         if (poolEntry != null) {

            if (config.getMinimumIdle() > 0) {
               //添加到Bag
               connectionBag.add(poolEntry);
               logger.info("{} - Added connection {}", poolName, poolEntry.connection);
            } else {
               //关闭连接
               quietlyCloseConnection(poolEntry.close(), "(initialization check complete and minimumIdle is zero)");
            }

            return;
         }

         if (getLastConnectionFailure() instanceof ConnectionSetupException) {
            throwPoolInitializationException(getLastConnectionFailure().getCause());
         }

         //忽略中断sleep
         quietlySleep(SECONDS.toMillis(1));
         //在时间内尽最大努力创建连接
      } while (elapsedMillis(startTime) < initializationFailTimeout);

      if (initializationFailTimeout > 0) {
         throwPoolInitializationException(getLastConnectionFailure());
      }
   }

   /**
    * Log the Throwable that caused pool initialization to fail, and then throw a PoolInitializationException with
    * that cause attached.
    *
    * @param t the Throwable that caused the pool to fail to initialize (possibly null)
    */
   private void throwPoolInitializationException(Throwable t) {
      destroyHouseKeepingExecutorService();
      throw new PoolInitializationException(t);
   }

   /**
    * "Soft" evict a Connection (/PoolEntry) from the pool.  If this method is being called by the user directly
    * through {@link com.zaxxer.hikari.HikariDataSource#evictConnection(Connection)} then {@code owner} is {@code true}.
    * <p>
    * If the caller is the owner, or if the Connection is idle (i.e. can be "reserved" in the {@link ConcurrentBag}),
    * then we can close the connection immediately.  Otherwise, we leave it "marked" for eviction so that it is evicted
    * the next time someone tries to acquire it from the pool.
    * <p>
    * 软驱逐连接
    *
    * @param poolEntry the PoolEntry (/Connection) to "soft" evict from the pool
    * @param reason    the reason that the connection is being evicted
    * @param owner     true if the caller is the owner of the connection, false otherwise
    * @return true if the connection was evicted (closed), false if it was merely marked for eviction
    */
   private boolean softEvictConnection(final PoolEntry poolEntry, final String reason, final boolean owner) {
      //标记驱逐
      poolEntry.markEvicted();
      //当前线程是连接的owner 并且未在使用
      if (owner || connectionBag.reserve(poolEntry)) {
         //立即关闭
         closeConnection(poolEntry, reason);
         return true;
      }

      return false;
   }

   /**
    * Create/initialize the Housekeeping service {@link ScheduledExecutorService}.  If the user specified an Executor
    * to be used in the {@link HikariConfig}, then we use that.  If no Executor was specified (typical), then create
    * an Executor and configure it.
    * 初始化houseKeeping线程池
    *
    * @return either the user specified {@link ScheduledExecutorService}, or the one we created
    */
   private ScheduledExecutorService initializeHouseKeepingExecutorService() {
      if (config.getScheduledExecutor() == null) {
         final var threadFactory = Optional.ofNullable(config.getThreadFactory()).orElseGet(() -> new DefaultThreadFactory(poolName + ":housekeeper"));
         final var executor = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
         executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
         executor.setRemoveOnCancelPolicy(true);
         return executor;
      } else {
         return config.getScheduledExecutor();
      }
   }

   /**
    * Destroy (/shutdown) the Housekeeping service Executor, if it was the one that we created.
    * 如果是Hikari创建的 终止
    */
   private void destroyHouseKeepingExecutorService() {
      if (config.getScheduledExecutor() == null) {
         houseKeepingExecutorService.shutdownNow();
      }
   }

   /**
    * Create a PoolStats instance that will be used by metrics tracking, with a pollable resolution of 1 second.
    *
    * @return a PoolStats instance
    */
   private PoolStats getPoolStats() {
      return new PoolStats(SECONDS.toMillis(1)) {
         @Override
         protected void update() {
            this.pendingThreads = HikariPool.this.getThreadsAwaitingConnection();
            this.idleConnections = HikariPool.this.getIdleConnections();
            this.totalConnections = HikariPool.this.getTotalConnections();
            this.activeConnections = HikariPool.this.getActiveConnections();
            this.maxConnections = config.getMaximumPoolSize();
            this.minConnections = config.getMinimumIdle();
         }
      };
   }

   /**
    * Create a timeout exception (specifically, {@link SQLTransientConnectionException}) to be thrown, because a
    * timeout occurred when trying to acquire a Connection from the pool.  If there was an underlying cause for the
    * timeout, e.g. a SQLException thrown by the driver while trying to create a new Connection, then use the
    * SQL State from that exception as our own and additionally set that exception as the "next" SQLException inside
    * our exception.
    * <p>
    * As a side effect, log the timeout failure at DEBUG, and record the timeout failure in the metrics tracker.
    *
    * @param startTime the start time (timestamp) of the acquisition attempt
    * @return a SQLException to be thrown from {@link #getConnection()}
    */
   private SQLException createTimeoutException(long startTime) {

      //打印状态
      logPoolState("Timeout failure ");
      //记录连接超时
      metricsTracker.recordConnectionTimeout();

      String sqlState = null;
      int errorCode = 0;
      //拿到上一个异常
      final var originalException = getLastConnectionFailure();
      if (originalException instanceof SQLException) {
         sqlState = ((SQLException) originalException).getSQLState();
         errorCode = ((SQLException) originalException).getErrorCode();
      }
      final var connectionException = new SQLTransientConnectionException(
         poolName + " - Connection is not available, request timed out after " + elapsedMillis(startTime) + "ms " +
            "(total=" + getTotalConnections() + ", active=" + getActiveConnections() + ", idle=" + getIdleConnections() + ", waiting=" + getThreadsAwaitingConnection() + ")",
         sqlState, errorCode, originalException);
      if (originalException instanceof SQLException) {
         connectionException.setNextException((SQLException) originalException);
      }

      return connectionException;
   }


   // ***********************************************************************
   //                      Non-anonymous Inner-classes
   // ***********************************************************************

   /**
    * Creating and adding poolEntries (connections) to the pool.
    * 执行实际的创建连接逻辑
    */
   private final class PoolEntryCreator implements Callable<Boolean> {

      private final String loggingPrefix;

      PoolEntryCreator() {
         this(null);
      }

      PoolEntryCreator(String loggingPrefix) {
         this.loggingPrefix = loggingPrefix;
      }

      @Override
      public Boolean call() {
         var backoffMs = 10L;
         var added = false;
         try {
            //是否需要继续创建
            while (shouldContinueCreating()) {
               final var poolEntry = createPoolEntry();
               if (poolEntry != null) {
                  added = true;
                  connectionBag.add(poolEntry);
                  logger.debug("{} - Added connection {}", poolName, poolEntry.connection);
                  quietlySleep(30L);
                  break;
               } else {  // failed to get connection from db, sleep and retry
                  if (loggingPrefix != null && backoffMs % 50 == 0)
                     logger.debug("{} - Connection add failed, sleeping with backoff: {}ms", poolName, backoffMs);
                  quietlySleep(backoffMs);
                  backoffMs = Math.min(SECONDS.toMillis(5), backoffMs * 2);
               }
            }
         } finally {
            if (added && loggingPrefix != null)
               logPoolState(loggingPrefix);
            else if (!added)
               logPoolState("Connection not added, ");
         }

         // Pool is suspended, shutdown, or at max size
         return Boolean.FALSE;
      }

      /**
       * We only create connections if we need another idle connection or have threads still waiting
       * for a new connection.  Otherwise we bail out of the request to create.
       *
       * @return true if we should create a connection, false if the need has disappeared
       */
      private synchronized boolean shouldContinueCreating() {
         return poolState == POOL_NORMAL && getTotalConnections() < config.getMaximumPoolSize() &&
            (getIdleConnections() < config.getMinimumIdle() || connectionBag.getWaitingThreadCount() > getIdleConnections());
      }
   }

   /**
    * The housekeeping task to retire and maintain minimum idle connections.
    * 定期维护连接池状态
    * 清理超时空闲链接 调整连接池大小 检测连接泄漏 更新时间基准
    */
   private final class HouseKeeper implements Runnable {

      private volatile long previous = plusMillis(currentTime(), -housekeepingPeriodMs);

      @SuppressWarnings("AtomicFieldUpdaterNotStaticFinal")
      private final AtomicReferenceFieldUpdater<PoolBase, String> catalogUpdater = AtomicReferenceFieldUpdater.newUpdater(PoolBase.class, String.class, "catalog");

      @Override
      public void run() {
         try {
            // refresh values in case they changed via MBean
            connectionTimeout = config.getConnectionTimeout();
            validationTimeout = config.getValidationTimeout();
            leakTaskFactory.updateLeakDetectionThreshold(config.getLeakDetectionThreshold());

            if (config.getCatalog() != null && !config.getCatalog().equals(catalog)) {
               catalogUpdater.set(HikariPool.this, config.getCatalog());
            }

            final var idleTimeout = config.getIdleTimeout();
            final var now = currentTime();

            // Detect retrograde time, allowing +128ms as per NTP spec.
            if (plusMillis(now, 128) < plusMillis(previous, housekeepingPeriodMs)) {
               logger.warn("{} - Retrograde clock change detected (housekeeper delta={}), soft-evicting connections from pool.", poolName, elapsedDisplayString(previous, now));
               previous = now;
               softEvictConnections();
               return;
            } else if (now > plusMillis(previous, (3 * housekeepingPeriodMs) / 2)) {
               // No point evicting for forward clock motion, this merely accelerates connection retirement anyway
               logger.warn("{} - Thread starvation or clock leap detected (housekeeper delta={}).", poolName, elapsedDisplayString(previous, now));
            }

            previous = now;

            if (idleTimeout > 0L && config.getMinimumIdle() < config.getMaximumPoolSize()) {
               logPoolState("Before cleanup ");
               final var notInUse = connectionBag.values(STATE_NOT_IN_USE);
               var maxToRemove = notInUse.size() - config.getMinimumIdle();
               for (PoolEntry entry : notInUse) {
                  if (maxToRemove > 0 && elapsedMillis(entry.lastAccessed, now) > idleTimeout && connectionBag.reserve(entry)) {
                     closeConnection(entry, "(connection has passed idleTimeout)");
                     maxToRemove--;
                  }
               }
               logPoolState("After  cleanup ");
            } else
               logPoolState("Pool ");

            fillPool(true); // Try to maintain minimum connections
         } catch (Exception e) {
            logger.error("Unexpected exception in housekeeping task", e);
         }
      }
   }

   //到达最大存活时间后驱逐连接的任务
   private final class MaxLifetimeTask implements Runnable {

      private final PoolEntry poolEntry;

      MaxLifetimeTask(final PoolEntry poolEntry) {
         this.poolEntry = poolEntry;
      }

      public void run() {
         if (softEvictConnection(poolEntry, "(connection has passed maxLifetime)", false /* not owner */)) {
            addBagItem(connectionBag.getWaitingThreadCount());
         }
      }
   }

   //定时保持连接活性的任务
   private final class KeepaliveTask implements Runnable {
      private final PoolEntry poolEntry;

      KeepaliveTask(final PoolEntry poolEntry) {
         this.poolEntry = poolEntry;
      }

      public void run() {
         if (connectionBag.reserve(poolEntry)) {
            if (isConnectionDead(poolEntry.connection)) {
               //重新弄一个
               softEvictConnection(poolEntry, DEAD_CONNECTION_MESSAGE, true);
               addBagItem(connectionBag.getWaitingThreadCount());
            } else {
               connectionBag.unreserve(poolEntry);
               logger.debug("{} - keepalive: connection {} is alive", poolName, poolEntry.connection);
            }
         }
      }
   }

   public static class PoolInitializationException extends RuntimeException {
      private static final long serialVersionUID = 929872118275916520L;

      /**
       * Construct an exception, possibly wrapping the provided Throwable as the cause.
       *
       * @param t the Throwable to wrap
       */
      public PoolInitializationException(Throwable t) {
         super("Failed to initialize pool: " + t.getMessage(), t);
      }
   }
}
