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

package kafka.log

import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.utils._

import scala.collection._
import scala.collection.JavaConverters._
import kafka.common.{KafkaException, KafkaStorageException}
import kafka.server.{BrokerState, OffsetCheckpoint, RecoveringFromUncleanShutdown}
import java.util.concurrent.{ExecutionException, ExecutorService, Executors, Future}

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
 *
 * Kafka日志管理子系统的入口点。LogManager负责日志的创建、检索和清理。
 * 所有读写操作都委托给各个日志实例。
 *
 * LogManager在一个或多个目录中维护日志。新的日志会在包含最少日志的数据目录中创建。
 * 不会在事后移动分区或基于大小或I/O速率进行平衡。
 *
 * 后台线程通过定期截断多余的日志段来处理日志保留。
 */
@threadsafe
class LogManager(val logDirs: Array[File],  // 日志目录数组
                 val topicConfigs: Map[String, LogConfig], // 主题特定的配置映射
                 val defaultConfig: LogConfig,      // 默认日志配置
                 val cleanerConfig: CleanerConfig,  // 日志清理器配置
                 ioThreads: Int,              // 后台IO线程数
                 val flushCheckMs: Long,      // 日志刷新检查间隔（毫秒）
                 val flushCheckpointMs: Long, // 检查点刷新间隔（毫秒）
                 val retentionCheckMs: Long,  // 日志保留检查间隔（毫秒）
                 scheduler: Scheduler,        // 调度器
                 val brokerState: BrokerState, // 代理状态
                 time: Time) extends Logging { // 时间工具

  // 恢复点检查点文件名
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  // 目录锁文件名
  val LockFile = ".lock"
  // 初始任务延迟时间（30秒）
  val InitialTaskDelayMs = 30*1000

  // 日志创建/删除操作的同步锁
  private val logCreationOrDeletionLock = new Object
  // 存储所有日志的线程安全池，Key为TopicPartition
  private val logs = new Pool[TopicPartition, Log]()
  // 待删除日志的阻塞队列
  private val logsToBeDeleted = new LinkedBlockingQueue[Log]()

  // 创建并验证日志目录
  createAndValidateLogDirs(logDirs)
  // 锁定日志目录防止多个LogManager实例同时访问
  private val dirLocks = lockLogDirs(logDirs)
  // 为每个日志目录创建恢复点检查点
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap
  // 加载所有日志
  loadLogs()

  // public, so we can access this from kafka.admin.DeleteTopicTest
  // 日志清理器（公开可见，供kafka.admin.DeleteTopicTest访问）
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner) // 根据配置决定是否启用日志清理器
      new LogCleaner(cleanerConfig, logDirs, logs, time = time)
    else
      // 不启用时为null
      null
  
  /**
   * Create and check validity of the given directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory 
   * </ol>
   *
   * 创建并验证给定目录的有效性，具体包括：
   * <ol>
   * <li> 确保目录列表中没有重复项
   * <li> 如果目录不存在则创建它
   * <li> 检查每个路径是否是可读的目录
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File]) {
    // 检查目录列表中是否有重复路径（通过规范化路径进行比较）
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))

    // 遍历所有目录进行处理
    for(dir <- dirs) {
      // 检查目录是否存在，不存在则创建
      if(!dir.exists) {
        info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
        // 创建目录（包括必要的父目录）
        val created = dir.mkdirs()
        if(!created)
          throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
      }
      // 验证路径确实是目录且可读
      if(!dir.isDirectory || !dir.canRead)
        throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
    }
  }
  
  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.map { dir =>
      val lock = new FileLock(new File(dir, LockFile))
      if(!lock.tryLock())
        throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath + 
                               ". A Kafka instance in another process or thread is using this directory.")
      lock
    }
  }
  
  /**
   * Recover and load all logs in the given data directories
   * 恢复并加载给定数据目录中的所有日志
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")
    // 记录开始时间，用于性能统计
    val startMs = time.milliseconds
    // 线程池集合
    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    // 存储每个目录的任务Future
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // 遍历所有日志目录
    for (dir <- this.logDirs) {
      // 为每个目录创建固定大小的线程池
      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      // 干净关闭文件：用于标记broker是否正常关闭（正常关闭时会创建该文件）
      val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)

      if (cleanShutdownFile.exists) {
        // 存在干净关闭文件，说明上次正常关闭，无需进行日志恢复，直接加载即可
        debug(
          "Found clean shutdown file. " +
          "Skipping recovery for all logs in data directory: " +
          dir.getAbsolutePath)
      } else {
        // log recovery itself is being performed by `Log` class during initialization
        // 不存在干净关闭文件，说明上次可能是非正常关闭（如崩溃），需要进行日志恢复
        // 更新broker状态为"从非正常关闭中恢复"
        brokerState.newState(RecoveringFromUncleanShutdown)
      }

      // 读取当前目录的恢复点检查点（记录每个分区的日志恢复点偏移量）
      var recoveryPoints = Map[TopicPartition, Long]()
      try {
        recoveryPoints = this.recoveryPointCheckpoints(dir).read
      } catch {
        case e: Exception =>
          warn("Error occured while reading recovery-point-offset-checkpoint file of directory " + dir, e)
          warn("Resetting the recovery checkpoint to 0")
      }

      // 生成当前目录的日志加载任务：遍历目录下的子目录（每个子目录对应一个分区日志）
      val jobsForDir = for {
        // 获取目录内容，转为List Option避免空指针
        dirContent <- Option(dir.listFiles).toList
        // 筛选出子目录（日志目录）
        logDir <- dirContent if logDir.isDirectory
      } yield {
        // 创建Runnable任务
        CoreUtils.runnable {
          debug("Loading log '" + logDir.getName + "'")

          // 从目录名解析出主题分区信息
          val topicPartition = Log.parseTopicPartitionName(logDir)
          // 获取该主题的配置，如果没有则使用默认配置
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          // 获取该分区的恢复点，如果没有则使用0
          val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)

          // 创建Log实例（这会触发日志恢复过程）
          val current = new Log(logDir, config, logRecoveryPoint, scheduler, time)
          // 检查是否是待删除的日志目录（以删除后缀结尾）
          if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
            this.logsToBeDeleted.add(current)
          } else {
            // 将日志添加到全局日志池中
            val previous = this.logs.put(topicPartition, current)
            if (previous != null) {
              // 如果已经存在相同分区的日志，抛出异常（重复目录）
              throw new IllegalArgumentException(
                "Duplicate log directories found: %s, %s!".format(
                  current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
            }
          }
        }
      }
      // 提交所有任务到线程池，并存储Future引用
      jobs(cleanShutdownFile) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      // 等待所有目录的加载任务完成
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)     // 阻塞等待所有任务完成
        cleanShutdownFile.delete() // 删除干净关闭文件（如果有）
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
      }
    } finally {
      // 关闭所有线程池
      threadPools.foreach(_.shutdown())
    }

    // 输出加载完成的耗时信息
    info(s"Logs loading complete in ${time.milliseconds - startMs} ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   *  启动后台线程来刷新日志和执行日志清理
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    /* 调度清理任务来删除旧日志 */
    if(scheduler != null) {
      // 调度日志保留清理任务，定期删除过期的日志段
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)
      // 调度日志刷新任务，定期将内存中的日志数据刷写到磁盘
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs, 
                         delay = InitialTaskDelayMs, 
                         period = flushCheckMs, 
                         TimeUnit.MILLISECONDS)
      // 调度恢复点检查点任务，定期将恢复点偏移量持久化到磁盘
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointRecoveryPointOffsets,
                         delay = InitialTaskDelayMs,
                         period = flushCheckpointMs,
                         TimeUnit.MILLISECONDS)
      // 调度日志删除任务，定期清理标记为待删除的日志
      scheduler.schedule("kafka-delete-logs",
                         deleteLogs,
                         delay = InitialTaskDelayMs,
                         period = defaultConfig.fileDeleteDelayMs,
                         TimeUnit.MILLISECONDS)
    }
    // 如果配置启用了日志清理器，则启动清理器
    if(cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    for (dir <- this.logDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile())
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }


  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long]) {
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = logs.get(topicPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner: Boolean = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && cleaner != null)
          cleaner.abortAndPauseCleaning(topicPartition)
        log.truncateTo(truncateOffset)
        if (needToStopCleaner && cleaner != null) {
          cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
          cleaner.resumeCleaning(topicPartition)
        }
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   *  Delete all data in a partition and start the log at the new offset
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long) {
    val log = logs.get(topicPartition)
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null)
        cleaner.abortAndPauseCleaning(topicPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicPartition)
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory 
   * to avoid recovering the whole log on startup.
   * 将所有日志的当前恢复点写入日志目录中的文本文件，以避免在启动时恢复整个日志。
   */
  def checkpointRecoveryPointOffsets() {
    // 遍历所有日志目录，对每个目录中的日志执行检查点操作
    this.logDirs.foreach(checkpointLogsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   * 为指定目录中的所有日志创建检查点。
   */
  private def checkpointLogsInDir(dir: File): Unit = {
    // 获取该目录下所有日志的恢复点信息
    val recoveryPoints = this.logsByDir.get(dir.toString)
    // 如果目录中存在日志，则写入检查点文件
    if (recoveryPoints.isDefined) {
      // 将恢复点映射（主题分区 -> 恢复点偏移量）写入检查点文件
      this.recoveryPointCheckpoints(dir).write(recoveryPoints.get.mapValues(_.recoveryPoint))
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   */
  def getLog(topicPartition: TopicPartition): Option[Log] = Option(logs.get(topicPartition))

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
   */
  def createLog(topicPartition: TopicPartition, config: LogConfig): Log = {
    logCreationOrDeletionLock synchronized {
      // create the log if it has not already been created in another thread
      getLog(topicPartition).getOrElse {
        val dataDir = nextLogDir()
        val dir = new File(dataDir, topicPartition.topic + "-" + topicPartition.partition)
        dir.mkdirs()
        val log = new Log(dir, config, recoveryPoint = 0L, scheduler, time)
        logs.put(topicPartition, log)
        info("Created log for partition [%s,%d] in %s with properties {%s}."
          .format(topicPartition.topic,
            topicPartition.partition,
            dataDir.getAbsolutePath,
            config.originals.asScala.mkString(", ")))
        log
      }
    }
  }

  /**
   *  Delete logs marked for deletion.
   */
  private def deleteLogs(): Unit = {
    try {
      var failed = 0
      while (!logsToBeDeleted.isEmpty && failed < logsToBeDeleted.size()) {
        val removedLog = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: Throwable =>
              error(s"Exception in deleting $removedLog. Moving it to the end of the queue.", e)
              failed = failed + 1
              logsToBeDeleted.put(removedLog)
          }
        }
      }
    } catch {
      case e: Throwable => 
        error(s"Exception in kafka-delete-logs thread.", e)
    }
}

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and 
    * add it in the queue for deletion. 
    * @param topicPartition TopicPartition that needs to be deleted
    */
  def asyncDelete(topicPartition: TopicPartition) = {
    val removedLog: Log = logCreationOrDeletionLock synchronized {
        logs.remove(topicPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null) {
        cleaner.abortCleaning(topicPartition)
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      val dirName = Log.logDeleteDirName(removedLog.name)
      removedLog.close()
      val renamedDir = new File(removedLog.dir.getParent, dirName)
      val renameSuccessful = removedLog.dir.renameTo(renamedDir)
      if (renameSuccessful) {
        removedLog.dir = renamedDir
        // change the file pointers for log and index file
        for (logSegment <- removedLog.logSegments) {
          logSegment.log.setFile(new File(renamedDir, logSegment.log.file.getName))
          logSegment.index.file = new File(renamedDir, logSegment.index.file.getName)
        }

        logsToBeDeleted.add(removedLog)
        removedLog.removeLogMetrics()
        info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
      } else {
        throw new KafkaStorageException("Failed to rename log directory from " + removedLog.dir.getAbsolutePath + " to " + renamedDir.getAbsolutePath)
      }
    }
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(logDirs.size == 1) {
      logDirs(0)
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap
      var dirCounts = (zeros ++ logCounts).toBuffer
    
      // choose the directory with the least logs in it
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   *
   * 删除所有符合条件的日志。返回删除的日志段数量。
   * 仅处理非压缩（non-compacted）类型的日志。
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    // 记录本次清理中删除的日志段总数
    var total = 0
    // 记录清理开始时间，用于统计耗时
    val startMs = time.milliseconds
    // 遍历所有日志，仅处理非压缩类型的日志（压缩日志由LogCleaner单独处理）
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      // 调用日志实例的deleteOldSegments()方法，删除该日志中符合清理条件的旧段
      // 该方法返回本次删除的段数量，累加到total中
      total += log.deleteOldSegments()
    }
    // 输出清理完成的调试日志，包含删除总数和耗时（转换为秒）
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs(): Iterable[Log] = logs.values

  /**
   * Get a map of TopicPartition => Log
   */
  def logsByTopicPartition: Map[TopicPartition, Log] = logs.toMap

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir = {
    this.logsByTopicPartition.groupBy {
      case (_, log) => log.dir.getParent
    }
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   * 刷新任何超过刷新间隔且有未写入消息的日志。
   */
  private def flushDirtyLogs() = {
    debug("Checking for dirty logs to flush...")
    // 遍历所有日志（键为TopicPartition，值为对应的Log实例）
    for ((topicPartition, log) <- logs) {
      try {
        // 计算从上一次刷盘到现在的时间间隔（毫秒）
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        // 若距上次刷盘的时间已超过配置的刷新间隔，则执行刷盘操作。默认log.config.flushMs为Long.MaxValue，表示不进行刷盘操作
        if(timeSinceLastFlush >= log.config.flushMs)
          // 执行日志刷新操作
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicPartition.topic, e)
      }
    }
  }
}
