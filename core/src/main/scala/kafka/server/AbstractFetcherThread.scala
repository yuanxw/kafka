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

package kafka.server

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.consumer.PartitionTopicInfo
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import kafka.common.{ClientIdAndBroker, KafkaException}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.protocol.Errors
import AbstractFetcherThread._

import scala.collection.{Map, Set, mutable}
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.PartitionStates
import org.apache.kafka.common.record.MemoryRecords

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest
  type PD <: PartitionData

  private val partitionStates = new PartitionStates[PartitionFetchState]
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PD)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition])

  protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): REQ

  protected def fetch(fetchRequest: REQ): Seq[(TopicPartition, PD)]

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  /**
   * 重写抽象方法，实现线程的核心工作逻辑。
   * 该方法主要负责构建拉取请求（fetch request），并在请求有效时进行处理，
   * 是副本从leader分区拉取数据的核心执行流程。
   */
  override def doWork() {
    // 在分区映射锁（partitionMapLock）的同步块中执行，确保操作分区状态时的线程安全
    val fetchRequest = inLock(partitionMapLock) {
      // 构建抓取请求，基于当前所有活跃分区的状态
      val fetchRequest = buildFetchRequest(partitionStates.partitionStates.asScala.map { state =>
        // 将分区状态转换为主题分区 -> 分区状态的映射
        state.topicPartition -> state.value
      })
      // 检查是否有活跃的分区需要抓取
      if (fetchRequest.isEmpty) {
        // 如果没有活跃分区，记录跟踪日志并在条件变量上等待指定的等待时间
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      // 返回构建的抓取请求（可能为空）
      fetchRequest
    }
    // 如果抓取请求不为空，则处理该请求
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest)
  }

  /**
   * 处理拉取请求（fetch request）的核心方法。
   * 该方法负责从leader broker获取数据响应、解析处理响应内容、更新本地副本状态，
   * 并处理拉取过程中出现的各种错误（如偏移量越界、数据损坏等），是副本同步数据的关键环节。
   *
   * @param fetchRequest 已构建的拉取请求对象（包含需要拉取的分区及偏移量信息）
   */
  private def processFetchRequest(fetchRequest: REQ) {
    // 存储处理过程中出现错误的分区集合
    val partitionsWithError = mutable.Set[TopicPartition]()

    /**
     * 将出错的分区添加到错误集合，并将其移至分区状态列表的末尾（延迟处理，优先处理正常分区）
     * @param partition 出现错误的主题分区
     */
    def updatePartitionsWithError(partition: TopicPartition): Unit = {
      partitionsWithError += partition
      partitionStates.moveToEnd(partition)
    }
    // 初始化响应数据为空序列
    var responseData: Seq[(TopicPartition, PD)] = Seq.empty

    try {
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      // 发送拉取请求并获取响应数据（实际网络交互逻辑由fetch方法实现）
      responseData = fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        // 捕获拉取过程中的所有异常（如网络错误、broker不可用等）
        if (isRunning.get) {
          warn(s"Error in fetch $fetchRequest", t)
          // 在分区映射锁的同步块中处理所有分区的错误
          inLock(partitionMapLock) {
            // 将所有当前跟踪的分区标记为出错
            partitionStates.partitionSet.asScala.foreach(updatePartitionsWithError)
            // there is an error occurred while fetching partitions, sleep a while
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
            // partition with error effectively doubling the delay. It would be good to improve this.

            // 拉取出错时，等待指定时间后再重试（避免频繁失败重试导致的资源浪费）
            // 注意：ReplicaFetcherThread.handlePartitionsWithError也会为每个错误分区引入相同延迟，可能导致总延迟翻倍（待优化）
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    // 更新拉取请求的速率统计（用于监控和配额控制）
    fetcherStats.requestRate.mark()

    // 若响应数据非空，则处理拉取到的分区数据
    if (responseData.nonEmpty) {
      // process fetched data
      // 在分区映射锁的同步块中处理数据，确保分区状态操作的线程安全
      inLock(partitionMapLock) {
        // 遍历每个分区的响应数据
        responseData.foreach { case (topicPartition, partitionData) =>
          val topic = topicPartition.topic
          val partitionId = topicPartition.partition

          // 获取当前分区的拉取状态（包含期望拉取的偏移量等信息）
          Option(partitionStates.stateValue(topicPartition)).foreach(currentPartitionFetchState =>

            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
            // 只有当前偏移量与抓取请求中请求的偏移量相同时，我们才追加到日志
            if (fetchRequest.offset(topicPartition) == currentPartitionFetchState.offset) {

              // 根据分区数据的错误代码进行不同的处理
              Errors.forCode(partitionData.errorCode) match {
                case Errors.NONE =>
                  try {
                    // 将分区数据转换为记录集
                    val records = partitionData.toRecords
                    // 计算新的偏移量：取最后一条记录的nextOffset，如果没有记录则使用当前偏移量
                    val newOffset = records.shallowEntries.asScala.lastOption.map(_.nextOffset).getOrElse(
                      currentPartitionFetchState.offset)

                    // 更新滞后统计信息：高水位标记与新偏移量之间的差值
                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)

                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    // 将分区数据交给子类处理，此后在本线程中不能再修改它
                    processPartitionData(topicPartition, currentPartitionFetchState.offset, partitionData)

                    // 获取有效字节数
                    val validBytes = records.validBytes
                    if (validBytes > 0) {
                      // Update partitionStates only if there is no exception during processPartitionData
                      // 只有在processPartitionData过程中没有异常时才更新分区状态
                      partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))

                      // 更新字节速率统计
                      fetcherStats.byteRate.mark(validBytes)
                    }
                  } catch {
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches

                      // 处理数据损坏异常：仅记录错误并标记分区为出错，不终止线程
                      // 这样做的目的：
                      // 1. 单个分区的数据损坏不会导致整个拉取线程崩溃，避免影响其他分区
                      // 2. 若损坏是暂时的（如日志截断、部分写入），后续拉取可能恢复正常
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                      updatePartitionsWithError(topicPartition);
                    case e: Throwable =>
                      // 处理其他异常
                      throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                        .format(topic, partitionId, currentPartitionFetchState.offset), e)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  // 处理偏移量超出范围错误
                  try {
                    // 调用处理方法获取新的偏移量
                    val newOffset = handleOffsetOutOfRange(topicPartition)
                    // 更新分区状态为新的偏移量
                    partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                    error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                      .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                  } catch {
                    case e: Throwable => // 处理获取新偏移量时的异常
                      error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      updatePartitionsWithError(topicPartition)
                  }
                case _ => // 处理其他错误
                  if (isRunning.get) {
                    error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                      partitionData.exception.get))
                    updatePartitionsWithError(topicPartition)
                  }
              }
            })
        }
      }
    }

    if (partitionsWithError.nonEmpty) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  def addPartitions(partitionAndOffsets: Map[TopicPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      // If the partitionMap already has the topic/partition, then do not update the map with the old offset
      val newPartitionToState = partitionAndOffsets.filter { case (tp, _) =>
        !partitionStates.contains(tp)
      }.map { case (tp, offset) =>
        val fetchState =
          if (PartitionTopicInfo.isOffsetInvalid(offset)) new PartitionFetchState(handleOffsetOutOfRange(tp))
          else new PartitionFetchState(offset)
        tp -> fetchState
      }
      val existingPartitionToState = partitionStates.partitionStates.asScala.map { state =>
        state.topicPartition -> state.value
      }.toMap
      partitionStates.set((existingPartitionToState ++ newPartitionToState).asJava)
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long) {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach (currentPartitionFetchState =>
          if (currentPartitionFetchState.isActive)
            partitionStates.updateAndMoveToEnd(partition, new PartitionFetchState(currentPartitionFetchState.offset, new DelayedItem(delay)))
        )
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition)
        fetcherLagStats.unregister(topicPartition.topic, topicPartition.partition)
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.size
    finally partitionMapLock.unlock()
  }

}

object AbstractFetcherThread {

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicPartition: TopicPartition): Long
  }

  trait PartitionData {
    def errorCode: Short
    def exception: Option[Throwable]
    def toRecords: MemoryRecords
    def highWatermark: Long
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  def isReplicaInSync(topic: String, partitionId: Int): Boolean = {
    val fetcherLagMetrics = stats.get(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (fetcherLagMetrics != null)
      fetcherLagMetrics.lag <= 0
    else
      false
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(active, inactive)
  */
case class PartitionFetchState(offset: Long, delay: DelayedItem) {

  def this(offset: Long) = this(offset, new DelayedItem(0))

  def isActive: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) == 0

  override def toString = "%d-%b".format(offset, isActive)
}
