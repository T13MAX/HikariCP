/*
 * Copyright (C) 2013 Brett Wooldridge
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

package com.zaxxer.hikari;

import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.pool.HikariPool;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.zaxxer.hikari.pool.HikariPool.POOL_NORMAL;

/**
 * The HikariCP pooled DataSource.
 * 数据源
 * 继承HikariConfig 方便设置配置项
 *
 * @author Brett Wooldridge
 */
public class HikariDataSource extends HikariConfig implements DataSource, Closeable {

   private static final Logger LOGGER = LoggerFactory.getLogger(HikariDataSource.class);

   private final AtomicBoolean isShutdown = new AtomicBoolean();

   //快速访问
   private final HikariPool fastPathPool;

   //懒加载
   private volatile HikariPool pool;

   /**
    * Default constructor.  Setters are used to configure the pool.  Using
    * this constructor vs. {@link #HikariDataSource(HikariConfig)} will
    * result in {@link #getConnection()} performance that is slightly lower
    * due to lazy initialization checks.
    * <p>
    * The first call to {@link #getConnection()} starts the pool.  Once the pool
    * is started, the configuration is "sealed" and no further configuration
    * changes are possible -- except via {@link HikariConfigMXBean} methods.
    */
   public HikariDataSource() {
      fastPathPool = null;
   }

   /**
    * Construct a HikariDataSource with the specified configuration.  The
    * {@link HikariConfig} is copied and the pool is started by invoking this
    * constructor.
    * <p>
    * The {@link HikariConfig} can be modified without affecting the HikariDataSource
    * and used to initialize another HikariDataSource instance.
    *
    * @param configuration a HikariConfig instance
    */
   public HikariDataSource(HikariConfig configuration) {

      //校验HikariConfig参数是否有效和完整
      configuration.validate();
      //把参数复制到数据源
      configuration.copyStateTo(this);

      LOGGER.info("{} - Starting...", configuration.getPoolName());
      pool = fastPathPool = new HikariPool(this);
      LOGGER.info("{} - Start completed.", configuration.getPoolName());

      //封印 后续配置禁止修改
      this.seal();
   }

   // ***********************************************************************
   //                          DataSource methods
   // ***********************************************************************

   /**
    * 拿连接
    * {@inheritDoc}
    */
   @Override
   public Connection getConnection() throws SQLException {

      // 如果连接池已关闭 抛出异常
      if (isClosed()) {
         throw new SQLException("HikariDataSource " + this + " has been closed.");
      }

      // 如果 fastPathPool 已初始化 直接从中获取连接
      if (fastPathPool != null) {
         return fastPathPool.getConnection();
      }

      // 临时变量 result 指向当前连接池
      HikariPool result = pool;

      // 如果连接池尚未初始化
      if (result == null) {
         synchronized (this) {
            // 再次检查连接池是否初始化 （双重检查锁）
            result = pool;
            if (result == null) {
               // 校验当前配置是否合法
               validate();

               // 打日志：连接池开始初始化
               LOGGER.info("{} - Starting...", getPoolName());

               try {
                  // 创建新的连接池实例并赋值
                  pool = result = new HikariPool(this);

                  // 封印配置 不允许再修改
                  this.seal();
               } catch (PoolInitializationException pie) {
                  // 如果初始化失败 且原因是 SQLException 则直接抛出
                  if (pie.getCause() instanceof SQLException) {
                     throw (SQLException) pie.getCause();
                  } else {
                     throw pie;
                  }
               }

               // 打日志：连接池初始化完成
               LOGGER.info("{} - Start completed.", getPoolName());
            }
         }
      }

      // 返回连接池中获取的连接
      return result.getConnection();
   }

   /**
    * 不允许使用用户名密码拿连接
    * {@inheritDoc}
    */
   @Override
   public Connection getConnection(String username, String password) throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   /**
    * 获取日志打印器
    * {@inheritDoc}
    */
   @Override
   public PrintWriter getLogWriter() throws SQLException {

      HikariPool p = pool;
      //拿到底层数据源 (真正创建屋里连接的对象)
      return (p != null ? p.getUnwrappedDataSource().getLogWriter() : null);
   }

   /**
    * 设置日志打印器
    * {@inheritDoc}
    */
   @Override
   public void setLogWriter(PrintWriter out) throws SQLException {
      var p = pool;
      if (p != null) {
         //底层数据源
         p.getUnwrappedDataSource().setLogWriter(out);
      }
   }

   /**
    * 设置登录超时时间
    * {@inheritDoc}
    */
   @Override
   public void setLoginTimeout(int seconds) throws SQLException {
      var p = pool;
      if (p != null) {
         p.getUnwrappedDataSource().setLoginTimeout(seconds);
      }
   }

   /**
    * 获取登录超时时间
    * {@inheritDoc}
    */
   @Override
   public int getLoginTimeout() throws SQLException {
      var p = pool;
      return (p != null ? p.getUnwrappedDataSource().getLoginTimeout() : 0);
   }

   /**
    * 返回父级日志记录器 继承Java的日志系统
    * {@inheritDoc}
    */
   @Override
   public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
   }

   /**
    * 获取被包装的真实对象
    * 需要调用底层数据源或连接的特有方法 而这些方法不在标准接口中时 就会用到 unwrap
    * {@inheritDoc}
    */
   @Override
   @SuppressWarnings("unchecked")
   public <T> T unwrap(Class<T> iface) throws SQLException {

      // 如果传入接口类型是当前对象的类型 则直接返回当前对象
      if (iface.isInstance(this)) {
         return (T) this;
      }

      // 获取当前连接池引用
      var p = pool;
      if (p != null) {
         // 获取底层真实数据源
         final var unwrappedDataSource = p.getUnwrappedDataSource();

         // 如果底层数据源是目标类型 直接返回
         if (iface.isInstance(unwrappedDataSource)) {
            return (T) unwrappedDataSource;
         }

         // 如果底层数据源不为空 递归调用其unwrap方法
         if (unwrappedDataSource != null) {
            return unwrappedDataSource.unwrap(iface);
         }
      }

      // 如果都没找到 抛出异常 表示不能转换为目标接口
      throw new SQLException("Wrapped DataSource is not an instance of " + iface);
   }

   /**
    * 判断是否包装了指定类型的对象
    * {@inheritDoc}
    */
   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      if (iface.isInstance(this)) {
         return true;
      }

      var p = pool;
      if (p != null) {
         final var unwrappedDataSource = p.getUnwrappedDataSource();
         if (iface.isInstance(unwrappedDataSource)) {
            return true;
         }

         if (unwrappedDataSource != null) {
            return unwrappedDataSource.isWrapperFor(iface);
         }
      }

      return false;
   }

   // ***********************************************************************
   //                        HikariConfigMXBean methods
   // ***********************************************************************

   /**
    * 设置指标监控用的 MetricRegistry
    * 一般配合 Dropwizard Metrics 记录连接池状态 如活动连接数 等待时间等
    * {@inheritDoc}
    */
   @Override
   public void setMetricRegistry(Object metricRegistry) {

      // 判断是否已经设置过 MetricRegistry
      var isAlreadySet = getMetricRegistry() != null;

      // 设置到父类 HikariConfig 中
      super.setMetricRegistry(metricRegistry);

      // 如果连接池已初始化
      var p = pool;
      if (p != null) {
         // 如果已经设置过 抛出异常 不允许重复设置
         if (isAlreadySet) {
            throw new IllegalStateException("MetricRegistry can only be set one time");
         } else {
            // 否则设置到连接池里
            p.setMetricRegistry(super.getMetricRegistry());
         }
      }
   }

   /**
    * 设置MetricsTrackerFactory
    * 用于自定义指标收集逻辑
    * {@inheritDoc}
    */
   @Override
   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {

      //是否设置过
      var isAlreadySet = getMetricsTrackerFactory() != null;
      //调用HikariConfig的setMetricsTrackerFactory
      super.setMetricsTrackerFactory(metricsTrackerFactory);

      var p = pool;
      if (p != null) {
         //已经设置过抛出异常
         if (isAlreadySet) {
            throw new IllegalStateException("MetricsTrackerFactory can only be set one time");
         } else {
            //设置
            p.setMetricsTrackerFactory(super.getMetricsTrackerFactory());
         }
      }
   }

   /**
    * 设置HealthCheckRegistry
    * 用于健康检查的注册表
    * {@inheritDoc}
    */
   @Override
   public void setHealthCheckRegistry(Object healthCheckRegistry) {
      var isAlreadySet = getHealthCheckRegistry() != null;
      super.setHealthCheckRegistry(healthCheckRegistry);

      var p = pool;
      if (p != null) {
         if (isAlreadySet) {
            throw new IllegalStateException("HealthCheckRegistry can only be set one time");
         } else {
            p.setHealthCheckRegistry(super.getHealthCheckRegistry());
         }
      }
   }

   // ***********************************************************************
   //                        HikariCP-specific methods
   // ***********************************************************************

   /**
    * Returns {@code true} if the pool as been started and is not suspended or shutdown.
    *
    * @return {@code true} if the pool as been started and is not suspended or shutdown.
    */
   public boolean isRunning() {
      return pool != null && pool.poolState == POOL_NORMAL;
   }

   /**
    * Get the {@code HikariPoolMXBean} for this HikariDataSource instance.  If this method is called on
    * a {@code HikariDataSource} that has been constructed without a {@code HikariConfig} instance,
    * and before an initial call to {@code #getConnection()}, the return value will be {@code null}.
    *
    * @return the {@code HikariPoolMXBean} instance, or {@code null}.
    */
   public HikariPoolMXBean getHikariPoolMXBean() {
      return pool;
   }

   /**
    * Get the {@code HikariConfigMXBean} for this HikariDataSource instance.
    *
    * @return the {@code HikariConfigMXBean} instance.
    */
   public HikariConfigMXBean getHikariConfigMXBean() {
      return this;
   }

   /**
    * Evict a connection from the pool.  If the connection has already been closed (returned to the pool)
    * this may result in a "soft" eviction; the connection will be evicted sometime in the future if it is
    * currently in use.  If the connection has not been closed, the eviction is immediate.
    * 将特定连接从连接池中移除(驱逐) 移除并关闭
    * 用于处理异常连接或失效连接 防止被重用
    *
    * @param connection the connection to evict from the pool
    */
   public void evictConnection(Connection connection) {
      HikariPool p;
      if (!isClosed() && (p = pool) != null && connection.getClass().getName().startsWith("com.zaxxer.hikari")) {
         p.evictConnection(connection);
      }
   }

   /**
    * Shutdown the DataSource and its associated pool.
    */
   @Override
   public void close() {
      if (isShutdown.getAndSet(true)) {
         return;
      }

      var p = pool;
      if (p != null) {
         try {
            LOGGER.info("{} - Shutdown initiated...", getPoolName());
            //关闭池子
            p.shutdown();
            LOGGER.info("{} - Shutdown completed.", getPoolName());
         } catch (InterruptedException e) {
            LOGGER.warn("{} - Interrupted during closing", getPoolName(), e);
            Thread.currentThread().interrupt();
         }
      }
   }

   /**
    * Determine whether the HikariDataSource has been closed.
    *
    * @return true if the HikariDataSource has been closed, false otherwise
    */
   public boolean isClosed() {
      return isShutdown.get();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      return "HikariDataSource (" + pool + ")";
   }
}
