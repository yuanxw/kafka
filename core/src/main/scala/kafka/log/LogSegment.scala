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

import java.io.{File, IOException}
import java.util.concurrent.TimeUnit

import kafka.common._
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.LogEntryPosition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.math._

 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The message set containing log entries
 * @param index The offset index
 * @param timeIndex The timestamp index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment(val log: FileRecords,
                 val index: OffsetIndex,
                 val timeIndex: TimeIndex,
                 val baseOffset: Long,
                 val indexIntervalBytes: Int,
                 val rollJitterMs: Long,
                 time: Time) extends Logging {

  private var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  private var bytesSinceLastIndexEntry = 0

  /* The timestamp we used for time based log rolling */
  private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp we see so far */
  @volatile private var maxTimestampSoFar = timeIndex.lastEntry.timestamp
  @volatile private var offsetOfMaxTimestamp = timeIndex.lastEntry.offset

  def this(dir: File, startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int, rollJitterMs: Long, time: Time, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false) =
    this(FileRecords.open(Log.logFile(dir, startOffset), fileAlreadyExists, initFileSize, preallocate),
         new OffsetIndex(Log.indexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
         new TimeIndex(Log.timeIndexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
         startOffset,
         indexIntervalBytes,
         rollJitterMs,
         time)

  /* Return the size in bytes of this log segment */
  def size: Long = log.sizeInBytes()

   /**
     * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
     */
   def canConvertToRelativeOffset(offset: Long): Boolean = {
     (offset - baseOffset) <= Integer.MAX_VALUE
   }

   /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * 从指定偏移量开始追加给定消息。必要时向索引文件添加条目（加速后续消息查询）。
   * 注意：该方法假设在同步锁中调用（非线程安全，需外部保证并发安全）。
   *
   * @param firstOffset The first offset in the message set.
   * @param largestTimestamp The largest timestamp in the message set.
   * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
   * @param records The log entries to append.
   */
  @nonthreadsafe // 注解：非线程安全，需外部同步控制
  def append(firstOffset: Long, largestOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords) {
    // 仅当消息集大小大于0时才执行追加操作（过滤空消息）
    if (records.sizeInBytes > 0) {
      trace("Inserting %d bytes at offset %d at position %d with largest timestamp %d at shallow offset %d"
          .format(records.sizeInBytes, firstOffset, log.sizeInBytes(), largestTimestamp, shallowOffsetOfMaxTimestamp))
      // 获取当前日志文件的大小（即新消息将要写入的起始物理位置，单位：字节）
      val physicalPosition = log.sizeInBytes()

      // 若日志文件为空（首次写入消息），记录该日志段的初始时间戳（用于后续日志滚动判断）
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)
      // append the messages
      // 验证：最大偏移量能否安全转换为相对偏移量（Kafka索引中存储的是相对于段起始偏移的差值，防止溢出）
      require(canConvertToRelativeOffset(largestOffset), "largest offset in message set can not be safely converted to relative offset.")

      // 将消息实际写入日志文件（.log文件），返回写入的字节数
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file()} at offset $firstOffset")

      // Update the in memory max timestamp and corresponding offset.
      // 更新内存中的最大时间戳及对应偏移量（用于时间索引更新）
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp
      }
      // append an entry to the index (if needed)
      // 若距离上次更新索引的字节数已超过配置的索引间隔（index.interval.bytes，默认4096字节），则更新索引
      // 默认条件参数：log.index.interval.bytes = 4096字节，即每4096字节更新一次索引
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        // 向偏移量索引（.index文件）添加条目：[起始偏移量 -> 物理位置]，用于快速定位消息在日志文件中的位置
        index.append(firstOffset, physicalPosition)
        // 向时间索引（.timeindex文件）添加条目：[最大时间戳 -> 对应偏移量]，用于按时间范围查询消息
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
        // 重置“上次索引后累计字节数”计数器
        bytesSinceLastIndexEntry = 0
      }
      // 累加“上次索引后累计字节数”（用于判断下次是否需要更新索引）
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * 查找第一个偏移量 >= 请求偏移量的消息的物理文件位置。
   *
   * startingFilePosition 参数是一个优化，如果我们已经知道文件中一个有效的起始位置
   * 高于索引中的最大下界，则可以使用它。
   *
   * @param offset The offset we want to translate  要转换的偏移量
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * 开始搜索的文件位置的下界。这纯粹是一个优化参数，当省略时，搜索将从偏移量索引中的位置开始。
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
   *        message or null if no message meets this criteria.
   *         存储具有最小偏移量 >= 请求偏移量的消息在日志中的位置以及消息的大小，如果没有消息满足此条件，则返回 null。
   */
  @threadsafe // 标注此方法是线程安全的
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogEntryPosition = {
    // 1. 使用索引查找目标偏移量对应的近似位置
    val mapping = index.lookup(offset)
    // 2. 在日志文件中搜索确切的偏移量位置，从索引返回的位置和起始文件位置中的较大值开始搜索
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * 从本日志段读取消息集，从第一个偏移量 >= startOffset 的消息开始。
   * 消息集将包含不超过 maxSize 字节的数据，并且如果指定了 maxOffset，将在 maxOffset 之前结束。
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read 包含在读取消息集中的第一个偏移量的下限
   * @param maxSize The maximum number of bytes to include in the message set we read 读取消息集中包含的最大字节数
   * @param maxOffset An optional maximum offset for the message set we read 读取消息集的最大偏移量限制
   * @param maxPosition The maximum position in the log segment that should be exposed for read 日志段中允许读取的最大位置（用于控制并发写入时的可见性）
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists) 若为true，即使第一条消息超过maxSize，也会返回该消息（如果存在）
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   *
   *         获取的数据和第一个偏移量 >= startOffset 的消息的偏移量元数据，
   *         如果 startOffset 大于此日志中的最大偏移量，则返回 null
   *
   */
  @threadsafe // 标注此方法是线程安全的
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    // 检查maxSize参数的有效性
    if (maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    // 获取日志的当前大小（这可能发生变化，需要保存一个一致的副本）
    val logSize = log.sizeInBytes // this may change, need to save a consistent copy

    // 将起始偏移量转换为文件中的位置和消息大小信息
    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    // 如果起始位置已经超出日志末尾，返回null
    if (startOffsetAndSize == null)
      return null

    // 获取起始位置（转换为整数）
    val startPosition = startOffsetAndSize.position.toInt

    // 创建偏移量元数据对象，包含起始偏移量、基础偏移量和起始位置
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    // 调整最大读取大小：若需要至少一条消息，确保最大大小不小于第一条消息的大小（即使突破原maxSize）
    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    // 若调整后的最大大小为0，返回空消息集（但包含正确的偏移量元数据）
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // 根据是否指定maxOffset，计算要读取的消息集长度
    val length = maxOffset match {
      case None =>
        // no max offset, just read until the max position
        // 未指定maxOffset：读取到maxPosition（日志段可见位置上限）或adjustedMaxSize的较小值
        min((maxPosition - startPosition).toInt, adjustedMaxSize)
      case Some(offset) =>
        // there is a max offset, translate it to a file position and use that to calculate the max read size;
        // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the
        // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an
        // offset between new leader's high watermark and the log end offset, we want to return an empty response.

        // 提供了maxOffset，将其转换为文件位置并用其计算最大读取大小；
        // 当分区的领导者变更时，新领导者的高水位标记可能短暂低于
        // 先前领导者的真实高水位标记。在此时间窗口内，如果消费者获取的偏移量
        // 位于新领导者的高水位标记和日志末端偏移量之间，我们希望返回空响应。
        if (offset < startOffset)
          return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false)

        // 将maxOffset转换为文件位置（从startPosition开始查找）
        val mapping = translateOffset(offset, startPosition)

        // 计算结束位置：若maxOffset超出日志段范围，使用日志段大小；否则使用映射得到的位置
        val endPosition =
          if (mapping == null)
            logSize // the max offset is off the end of the log, use the end of the file maxOffset超出日志段，结束位置为日志段末尾
          else
            mapping.position
        // 最终读取长度：取（maxPosition和endPosition的较小值 - 起始位置）与adjustedMaxSize的较小值
        min(min(maxPosition, endPosition) - startPosition, adjustedMaxSize).toInt
    }

    // 从日志文件的startPosition位置读取length字节的消息，构建并返回FetchDataInfo
    // 同时标记第一条消息是否不完整（当adjustedMaxSize小于第一条消息大小时）
    FetchDataInfo(offsetMetadata, log.read(startPosition, length),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
   *
   * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
   * is corrupt.
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(maxMessageSize: Int): Int = {
    index.truncate()
    index.resize(index.maxIndexSize)
    timeIndex.truncate()
    timeIndex.resize(timeIndex.maxIndexSize)
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampSoFar = Record.NO_TIMESTAMP
    try {
      for (entry <- log.shallowEntries(maxMessageSize).asScala) {
        val record = entry.record
        record.ensureValid()

        // The max timestamp should have been put in the outer message, so we don't need to iterate over the inner messages.
        if (record.timestamp > maxTimestampSoFar) {
          maxTimestampSoFar = record.timestamp
          offsetOfMaxTimestamp = entry.offset
        }

        // Build offset index
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          val startOffset = entry.firstOffset
          index.append(startOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
          lastIndexEntry = validBytes
        }
        validBytes += entry.sizeInBytes()
      }
    } catch {
      case e: CorruptRecordException =>
        logger.warn("Found invalid messages in log segment %s at byte offset %d: %s."
          .format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    val truncated = log.sizeInBytes - validBytes
    log.truncateTo(validBytes)
    index.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
    timeIndex.trimToValidSize()
    truncated
  }

  def loadLargestTimestamp(readToLogEnd: Boolean = false) {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampSoFar = lastTimeIndexEntry.timestamp
    offsetOfMaxTimestamp = lastTimeIndexEntry.offset
    if (readToLogEnd) {
      val offsetPosition = index.lookup(lastTimeIndexEntry.offset)
      // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
      val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
      if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
        maxTimestampSoFar = maxTimestampOffsetAfterLastEntry.timestamp
        offsetOfMaxTimestamp = maxTimestampOffsetAfterLastEntry.offset
      }
    }
  }


  override def toString = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    val mapping = translateOffset(offset)
    if (mapping == null)
      return 0
    index.truncateTo(offset)
    timeIndex.truncateTo(offset)
    // after truncation, reset and allocate more space for the (new currently  active) index
    index.resize(index.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)
    val bytesTruncated = log.truncateTo(mapping.position.toInt)
    if(log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }
    bytesSinceLastIndexEntry = 0
    // We may need to reload the max timestamp after truncation.
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp(readToLogEnd = true)
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def nextOffset(): Long = {
    val ms = read(index.lastOffset, None, log.sizeInBytes)
    if (ms == null) {
      baseOffset
    } else {
      ms.records.shallowEntries.asScala.lastOption match {
        case None => baseOffset
        case Some(last) => last.nextOffset
      }
    }
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      index.flush()
      timeIndex.flush()
    }
  }

  /**
   * Change the suffix for the index and log file for this log segment
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {

    def kafkaStorageException(fileType: String, e: IOException) =
      new KafkaStorageException(s"Failed to change the $fileType file suffix from $oldSuffix to $newSuffix for log segment $baseOffset", e)

    try log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("log", e)
    }
    try index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("index", e)
    }
    try timeIndex.renameTo(new File(CoreUtils.replaceSuffix(timeIndex.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("timeindex", e)
    }
  }

  /**
   * Append the largest time index entry to the time index when this log segment become inactive segment.
   * This entry will be used to decide when to delete the segment.
   */
  def onBecomeInactiveSegment() {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message has a timestamp we use the message timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new message's timestamp and the first message's timestamp exceeds the
   * segment rolling time.
   * If the first message does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long) : Long = {
    // Load the timestamp of the first message into memory
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.shallowEntries.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().record.timestamp)
    }
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  /**
   * Search the message offset based on timestamp.
   * This method returns an option of TimestampOffset. The offset is the offset of the first message whose timestamp is
   * greater than or equals to the target timestamp.
   *
   * If all the message in the segment have smaller timestamps, the returned offset will be last offset + 1 and the
   * timestamp will be max timestamp in the segment.
   *
   * If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp,
   * the returned the offset will be the base offset of the segment and the timestamp will be Message.NoTimestamp.
   *
   * This methods only returns None when the log is not empty but we did not see any messages when scanning the log
   * from the indexed position. This could happen if the log is truncated after we get the indexed position but
   * before we scan the log from there. In this case we simply return None and the caller will need to check on
   * the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @return the timestamp and offset of the first message whose timestamp is larger than or equal to the
   *         target timestamp. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(timestamp: Long): Option[TimestampOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    val position = index.lookup(timestampOffset.offset).position

    // Search the timestamp
    Option(log.searchForTimestamp(timestamp, position)).map { timestampAndOffset =>
      TimestampOffset(timestampAndOffset.timestamp, timestampAndOffset.offset)
    }
  }

  /**
   * Close this log segment
   */
  def close() {
    CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true))
    CoreUtils.swallow(index.close)
    CoreUtils.swallow(timeIndex.close())
    CoreUtils.swallow(log.close)
  }

  /**
   * Delete this log segment from the filesystem.
   *
   * @throws KafkaStorageException if the delete fails.
   */
  def delete() {
    val deletedLog = log.delete()
    val deletedIndex = index.delete()
    val deletedTimeIndex = timeIndex.delete()
    if(!deletedLog && log.file.exists)
      throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
    if(!deletedIndex && index.file.exists)
      throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
    if(!deletedTimeIndex && timeIndex.file.exists)
      throw new KafkaStorageException("Delete of time index " + timeIndex.file.getName + " failed.")
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    log.file.setLastModified(ms)
    index.file.setLastModified(ms)
    timeIndex.file.setLastModified(ms)
  }
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
