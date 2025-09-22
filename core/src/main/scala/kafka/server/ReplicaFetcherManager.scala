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

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

class ReplicaFetcherManager(brokerConfig: KafkaConfig, replicaMgr: ReplicaManager, metrics: Metrics, time: Time, threadNamePrefix: Option[String] = None, quotaManager: ReplicationQuotaManager)
      extends AbstractFetcherManager("ReplicaFetcherManager on broker " + brokerConfig.brokerId,
        "Replica", brokerConfig.numReplicaFetchers) {

  /**
   * 重写方法，用于创建具体的Fetcher线程（副本拉取线程）。
   * 该方法是Kafka副本同步机制的一部分，负责实例化从源Broker拉取数据的线程，
   * 确保副本与 leader 分区的数据保持一致。
   *
   * @param fetcherId 拉取线程的唯一标识ID，用于区分不同的拉取线程
   * @param sourceBroker 数据来源的Broker节点信息（包含地址、端口等），即leader分区所在的Broker
   * @return 创建的具体拉取线程实例（ReplicaFetcherThread），继承自AbstractFetcherThread抽象类
   */
  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
    // 根据是否有线程名前缀，生成唯一的线程名（便于日志跟踪和监控识别）
    val threadName = threadNamePrefix match {
      case None => // 无前缀时，线程名格式为 "ReplicaFetcherThread-拉取ID-源BrokerID"
        "ReplicaFetcherThread-%d-%d".format(fetcherId, sourceBroker.id)
      case Some(p) => // 有前缀时，在原有格式前添加前缀（如区分不同组件的拉取线程）
        "%s:ReplicaFetcherThread-%d-%d".format(p, fetcherId, sourceBroker.id)
    }
    // 创建并返回副本拉取线程实例，传入必要的配置和管理组件
    new ReplicaFetcherThread(
      threadName, // 线程名
      fetcherId,  // 拉取线程ID
      sourceBroker, // 源broker端点信息，指定从哪个broker抓取数据
      brokerConfig, // broker配置信息
      replicaMgr, // 副本管理器，用于处理抓取到的数据
      metrics, // 指标收集器，用于记录抓取相关的指标
      time,    // 时间服务，提供时间相关功能
      quotaManager // 配额管理器，用于流量控制
    )
  }

  def shutdown() {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
