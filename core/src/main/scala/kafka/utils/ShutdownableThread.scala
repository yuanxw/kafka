/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch

abstract class ShutdownableThread(val name: String, val isInterruptible: Boolean = true)
        extends Thread(name) with Logging {
  this.setDaemon(false)
  this.logIdent = "[" + name + "], "
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutdownLatch = new CountDownLatch(1)

  def shutdown() = {
    initiateShutdown()
    awaitShutdown()
  }

  def initiateShutdown(): Boolean = {
    if(isRunning.compareAndSet(true, false)) {
      info("Shutting down")
      isRunning.set(false)
      if (isInterruptible)
        interrupt()
      true
    } else
      false
  }

    /**
   * After calling initiateShutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = {
    shutdownLatch.await()
    info("Shutdown completed")
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   * 该方法会被重复调用，直到线程关闭或该方法抛出异常为止。
   * 这是一个抽象方法，由子类实现具体的业务逻辑（如拉取数据、处理任务等），
   * 体现了模板方法设计模式：父类定义线程执行框架，子类填充具体工作内容。
   */
  def doWork(): Unit

  override def run(): Unit = {
    info("Starting ")
    try{
      // 线程主循环：只要isRunning状态为true（通过原子变量控制，线程安全），就持续调用doWork()
      // isRunning通常由外部调用shutdown()方法置为false，触发线程退出循环
      while(isRunning.get()){
        // 调用抽象方法执行具体工作（由子类实现）
        doWork()
      }
    } catch{
      // 捕获所有异常（包括Error），防止线程因未处理的异常意外终止
      case e: Throwable =>
        // 仅当线程仍处于运行状态时记录错误（避免处理已主动停止的线程异常）
        if(isRunning.get())
          error("Error due to ", e)
    }
    shutdownLatch.countDown()
    info("Stopped ")
  }
}