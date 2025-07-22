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

import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.zaxxer.hikari.SQLExceptionOverride.Override.*;

/**
 * This is the proxy class for {@link Connection}.
 * 代理连接 拦截 跟踪连接的状态变化(如只读 自动提交 隔离级别等)
 *
 * @author Brett Wooldridge
 */
public abstract class ProxyConnection implements Connection {

   // 各种连接属性的“脏位”标志 用于标记哪些属性被修改过
   static final int DIRTY_BIT_READONLY = 0b000001;       // 只读属性被修改
   static final int DIRTY_BIT_AUTOCOMMIT = 0b000010;     // 自动提交属性被修改
   static final int DIRTY_BIT_ISOLATION = 0b000100;      // 事务隔离级别被修改
   static final int DIRTY_BIT_CATALOG = 0b001000;        // catalog被修改
   static final int DIRTY_BIT_NETTIMEOUT = 0b010000;     // 网络超时被修改
   static final int DIRTY_BIT_SCHEMA = 0b100000;         // schema被修改

   // 日志记录器
   private static final Logger LOGGER;
   // 数据库错误状态集合 用于判断连接是否应被抛弃
   private static final Set<String> ERROR_STATES;
   // 数据库错误码集合 同上
   private static final Set<Integer> ERROR_CODES;

   // 实际的JDBC连接
   @SuppressWarnings("WeakerAccess")
   protected Connection delegate;

   // 当前连接对应的池条目
   private final PoolEntry poolEntry;
   // 用于检测连接泄漏的任务
   private final ProxyLeakTask leakTask;
   // 当前连接打开的语句列表
   private final FastList<Statement> openStatements;

   // 属性修改位标志
   private int dirtyBits;
   // 标记是否手动更改了事务提交状态
   private boolean isCommitStateDirty;
   // 当前连接是否处于只读模式
   private boolean isReadOnly;
   // 当前连接是否自动提交
   private boolean isAutoCommit;
   // 当前网络超时时间
   private int networkTimeout;
   // 当前事务隔离级别
   private int transactionIsolation;
   // 当前连接的catalog
   private String dbcatalog;
   // 当前连接的schema
   private String dbschema;

   // static initializer
   static {
      LOGGER = LoggerFactory.getLogger(ProxyConnection.class);

      ERROR_STATES = new HashSet<>();
      ERROR_STATES.add("0A000"); // FEATURE UNSUPPORTED
      ERROR_STATES.add("57P01"); // ADMIN SHUTDOWN
      ERROR_STATES.add("57P02"); // CRASH SHUTDOWN
      ERROR_STATES.add("57P03"); // CANNOT CONNECT NOW
      ERROR_STATES.add("01002"); // SQL92 disconnect error
      ERROR_STATES.add("JZ0C0"); // Sybase disconnect error
      ERROR_STATES.add("JZ0C1"); // Sybase disconnect error

      ERROR_CODES = new HashSet<>();
      ERROR_CODES.add(500150);
      ERROR_CODES.add(2399);
      ERROR_CODES.add(1105);
   }

   protected ProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<Statement> openStatements, final ProxyLeakTask leakTask, final boolean isReadOnly, final boolean isAutoCommit) {
      this.poolEntry = poolEntry;
      this.delegate = connection;
      this.openStatements = openStatements;
      this.leakTask = leakTask;
      this.isReadOnly = isReadOnly;
      this.isAutoCommit = isAutoCommit;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final String toString() {
      return this.getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegate;
   }

   // ***********************************************************************
   //                     Connection State Accessors
   // ***********************************************************************

   final boolean getAutoCommitState() {
      return isAutoCommit;
   }

   final String getCatalogState() {
      return dbcatalog;
   }

   final String getSchemaState() {
      return dbschema;
   }

   final int getTransactionIsolationState() {
      return transactionIsolation;
   }

   final boolean getReadOnlyState() {
      return isReadOnly;
   }

   final int getNetworkTimeoutState() {
      return networkTimeout;
   }

   // ***********************************************************************
   //                          Internal methods
   // ***********************************************************************

   final PoolEntry getPoolEntry() {
      return poolEntry;
   }

   @SuppressWarnings("ConstantConditions") // 忽略常量条件的警告
   final SQLException checkException(SQLException sqle) {
      // 是否需要驱逐连接
      var evict = false;
      SQLException nse = sqle;
      final var exceptionOverride = poolEntry.getPoolBase().exceptionOverride;

      // 遍历最多10层异常链 检查是否是致命异常
      for (int depth = 0; delegate != ClosedConnection.CLOSED_CONNECTION && nse != null && depth < 10; depth++) {
         final var sqlState = nse.getSQLState();
         final var shouldEvict = exceptionOverride != null ? exceptionOverride.adjudicate(nse) : CONTINUE_EVICT;

         if (shouldEvict == DO_NOT_EVICT) {
            break; // 不需要驱逐
         } else if (sqlState != null && sqlState.startsWith("08") // 连接异常类SQLState
            || ERROR_STATES.contains(sqlState) // 命中配置的错误状态
            || ERROR_CODES.contains(nse.getErrorCode()) // 命中配置的错误码
            || shouldEvict == MUST_EVICT) { // 明确要求驱逐

            evict = true;
            break;
         } else {
            nse = nse.getNextException(); // 查看下一个异常
         }
      }

      // 如果需要驱逐 记录日志 取消泄漏检测 关闭连接
      if (evict) {
         var exception = (nse != null) ? nse : sqle;
         LOGGER.warn("{} - Connection {} marked as broken because of SQLSTATE({}), ErrorCode({})", poolEntry.getPoolName(), delegate, exception.getSQLState(), exception.getErrorCode(), exception);
         leakTask.cancel();
         poolEntry.evict("(connection is broken)");
         delegate = ClosedConnection.CLOSED_CONNECTION;
      }

      return sqle;
   }

   final synchronized void untrackStatement(final Statement statement) {
      openStatements.remove(statement);
   }

   /**
    * 标记事务提交状态被手动修改 归还的时候做一些处理
    * 如果 dirty 且是自动提交关闭状态下 会跳过清理(默认认为用户处理了)
    * 如果不是自动提交 且 dirty 会执行 rollback() 保证下个使用者拿到干净连接
    * 还要重置一些属性
    *
    * @Author t13max
    * @Date 17:00 2025/7/22
    */
   final void markCommitStateDirty() {
      if (!isAutoCommit) {
         isCommitStateDirty = true;
      }
   }

   //取消泄漏检测
   void cancelLeakTask() {
      leakTask.cancel();
   }

   private synchronized <T extends Statement> T trackStatement(final T statement) {
      openStatements.add(statement);

      return statement;
   }

   @SuppressWarnings("EmptyTryBlock")
   private synchronized void closeStatements() {
      final var size = openStatements.size();
      if (size > 0) {
         for (int i = 0; i < size && delegate != ClosedConnection.CLOSED_CONNECTION; i++) {
            try (Statement ignored = openStatements.get(i)) {
               // automatic resource cleanup
            } catch (SQLException e) {
               LOGGER.warn("{} - Connection {} marked as broken because of an exception closing open statements during Connection.close()", poolEntry.getPoolName(), delegate);
               leakTask.cancel();
               poolEntry.evict("(exception closing Statements during Connection.close())");
               delegate = ClosedConnection.CLOSED_CONNECTION;
            }
         }

         openStatements.clear();
      }
   }

   // **********************************************************************
   //              "Overridden" java.sql.Connection Methods
   // **********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public final void close() throws SQLException {

      // Closing statements can cause connection eviction, so this must run before the conditional below
      closeStatements();

      if (delegate != ClosedConnection.CLOSED_CONNECTION) {

         //取消泄漏检测任务
         leakTask.cancel();

         try {
            //脏了 并且不是自动提交 说明执行了sql没提交 需要回滚
            if (isCommitStateDirty && !isAutoCommit) {
               //回滚
               delegate.rollback();
               LOGGER.debug("{} - Executed rollback on connection {} due to dirty commit state on close().", poolEntry.getPoolName(), delegate);
            }

            if (dirtyBits != 0) {
               poolEntry.resetConnectionState(this, dirtyBits);
            }

            delegate.clearWarnings();
         } catch (SQLException e) {
            // when connections are aborted, exceptions are often thrown that should not reach the application
            if (!poolEntry.isMarkedEvicted()) {
               throw checkException(e);
            }
         } finally {
            //置为关闭连接
            delegate = ClosedConnection.CLOSED_CONNECTION;
            //回收
            poolEntry.recycle();
         }
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @SuppressWarnings("RedundantThrows")
   public boolean isClosed() throws SQLException {
      return (delegate == ClosedConnection.CLOSED_CONNECTION);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Statement createStatement() throws SQLException {
      return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement()));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Statement createStatement(int resultSetType, int concurrency) throws SQLException {
      return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement(resultSetType, concurrency)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Statement createStatement(int resultSetType, int concurrency, int holdability) throws SQLException {
      return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement(resultSetType, concurrency, holdability)));
   }


   /**
    * {@inheritDoc}
    */
   @Override
   public CallableStatement prepareCall(String sql) throws SQLException {
      return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency) throws SQLException {
      return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql, resultSetType, concurrency)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency, int holdability) throws SQLException {
      return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql, resultSetType, concurrency, holdability)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql) throws SQLException {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, autoGeneratedKeys)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency) throws SQLException {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency, int holdability) throws SQLException {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency, holdability)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, columnIndexes)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, columnNames)));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public DatabaseMetaData getMetaData() throws SQLException {
      markCommitStateDirty();
      return ProxyFactory.getProxyDatabaseMetaData(this, delegate.getMetaData());
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void commit() throws SQLException {
      delegate.commit();
      isCommitStateDirty = false;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void rollback() throws SQLException {
      delegate.rollback();
      isCommitStateDirty = false;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void rollback(Savepoint savepoint) throws SQLException {
      delegate.rollback(savepoint);
      isCommitStateDirty = true;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean getAutoCommit() throws SQLException {
      if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0) {
         return isAutoCommit;
      }
      return delegate.getAutoCommit();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setAutoCommit(boolean autoCommit) throws SQLException {
      delegate.setAutoCommit(autoCommit);
      isAutoCommit = autoCommit;
      dirtyBits |= DIRTY_BIT_AUTOCOMMIT;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isReadOnly() throws SQLException {
      if ((dirtyBits & DIRTY_BIT_READONLY) != 0) {
         return isReadOnly;
      }
      return delegate.isReadOnly();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setReadOnly(boolean readOnly) throws SQLException {
      delegate.setReadOnly(readOnly);
      isReadOnly = readOnly;
      dirtyBits |= DIRTY_BIT_READONLY;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getTransactionIsolation() throws SQLException {
      if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0) {
         return transactionIsolation;
      }
      return delegate.getTransactionIsolation();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setTransactionIsolation(int level) throws SQLException {
      delegate.setTransactionIsolation(level);
      transactionIsolation = level;
      dirtyBits |= DIRTY_BIT_ISOLATION;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String getCatalog() throws SQLException {
      if ((dirtyBits & DIRTY_BIT_CATALOG) != 0) {
         return dbcatalog;
      }
      return delegate.getCatalog();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setCatalog(String catalog) throws SQLException {
      delegate.setCatalog(catalog);
      dbcatalog = catalog;
      dirtyBits |= DIRTY_BIT_CATALOG;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getNetworkTimeout() throws SQLException {
      if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0) {
         return networkTimeout;
      }
      return delegate.getNetworkTimeout();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      delegate.setNetworkTimeout(executor, milliseconds);
      networkTimeout = milliseconds;
      dirtyBits |= DIRTY_BIT_NETTIMEOUT;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String getSchema() throws SQLException {
      if ((dirtyBits & DIRTY_BIT_SCHEMA) != 0) {
         return dbschema;
      }
      return delegate.getSchema();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setSchema(String schema) throws SQLException {
      delegate.setSchema(schema);
      dbschema = schema;
      dirtyBits |= DIRTY_BIT_SCHEMA;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final boolean isWrapperFor(Class<?> iface) throws SQLException {
      return iface.isInstance(delegate) || (delegate != null && delegate.isWrapperFor(iface));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @SuppressWarnings("unchecked")
   public final <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(delegate)) {
         return (T) delegate;
      } else if (delegate != null) {
         return delegate.unwrap(iface);
      }

      throw new SQLException("Wrapped connection is not an instance of " + iface);
   }

   // **********************************************************************
   //                         Private classes
   // **********************************************************************

   private static final class ClosedConnection {
      static final Connection CLOSED_CONNECTION = getClosedConnection();

      private static Connection getClosedConnection() {
         InvocationHandler handler = (proxy, method, args) -> {
            final String methodName = method.getName();
            switch (methodName) {
               case "isClosed":
                  return Boolean.TRUE;
               case "isValid":
                  return Boolean.FALSE;
               case "abort":
                  return Void.TYPE;
               case "close":
                  return Void.TYPE;
               case "toString":
                  return ClosedConnection.class.getCanonicalName();
            }

            throw new SQLException("Connection is closed");
         };

         return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class[]{Connection.class}, handler);
      }
   }
}
