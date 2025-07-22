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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Runnable that is scheduled in the future to report leaks.  The ScheduledFuture is
 * cancelled if the connection is closed before the leak time expires.
 * 检测泄漏的任务
 * 超过一定时间没有归还连接池 则认定泄漏 打印警告日志
 *
 * @author Brett Wooldridge
 */
class ProxyLeakTask implements Runnable {

   private static final Logger LOGGER = LoggerFactory.getLogger(ProxyLeakTask.class);

   //空的任务
   static final ProxyLeakTask NO_LEAK;

   //定时任务结果 用于cancel
   private ScheduledFuture<?> scheduledFuture;
   //连接名
   private String connectionName;
   //用于找到创建任务的时候的堆栈
   private Exception exception;
   //线程名
   private String threadName;
   //是否泄漏
   private boolean isLeaked;

   static {
      NO_LEAK = new ProxyLeakTask() {
         @Override
         void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold) {
         }

         @Override
         public void run() {
         }

         @Override
         public void cancel() {
         }
      };
   }

   ProxyLeakTask(final PoolEntry poolEntry) {
      this.exception = new Exception("Apparent connection leak detected");
      this.threadName = Thread.currentThread().getName();
      this.connectionName = poolEntry.connection.toString();
   }

   private ProxyLeakTask() {
   }

   void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold) {
      scheduledFuture = executorService.schedule(this, leakDetectionThreshold, TimeUnit.MILLISECONDS);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void run() {

      isLeaked = true;

      final var stackTrace = exception.getStackTrace();
      final var trace = new StackTraceElement[stackTrace.length - 5];

      //去掉内部调用的无用堆栈
      System.arraycopy(stackTrace, 5, trace, 0, trace.length);

      exception.setStackTrace(trace);
      LOGGER.warn("Connection leak detection triggered for {} on thread {}, stack trace follows", connectionName, threadName, exception);
   }

   //取消任务
   void cancel() {
      scheduledFuture.cancel(false);
      if (isLeaked) {
         LOGGER.info("Previously reported leaked connection {} on thread {} was returned to the pool (unleaked)", connectionName, threadName);
      }
   }
}
