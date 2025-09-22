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

import java.io.IOException

import org.apache.kafka.clients.{ClientRequest, ClientResponse, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object NetworkClientBlockingOps {
  implicit def networkClientBlockingOps(client: NetworkClient): NetworkClientBlockingOps =
    new NetworkClientBlockingOps(client)
}

/**
 * Provides extension methods for `NetworkClient` that are useful for implementing blocking behaviour. Use with care.
 *
 * Example usage:
 *
 * {{{
 * val networkClient: NetworkClient = ...
 * import NetworkClientBlockingOps._
 * networkClient.blockingReady(...)
 * }}}
 */
class NetworkClientBlockingOps(val client: NetworkClient) extends AnyVal {

  /**
    * Checks whether the node is currently connected, first calling `client.poll` to ensure that any pending
    * disconnects have been processed.
    *
    * This method can be used to check the status of a connection prior to calling `blockingReady` to be able
    * to tell whether the latter completed a new connection.
    */
  def isReady(node: Node)(implicit time: Time): Boolean = {
    val currentTime = time.milliseconds()
    client.poll(0, currentTime)
    client.isReady(node, currentTime)
  }

  /**
   * Invokes `client.poll` to discard pending disconnects, followed by `client.ready` and 0 or more `client.poll`
   * invocations until the connection to `node` is ready, the timeout expires or the connection fails.
   *
   * It returns `true` if the call completes normally or `false` if the timeout expires. If the connection fails,
   * an `IOException` is thrown instead. Note that if the `NetworkClient` has been configured with a positive
   * connection timeout, it is possible for this method to raise an `IOException` for a previous connection which
   * has recently disconnected.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
   * care.
   */
  def blockingReady(node: Node, timeout: Long)(implicit time: Time): Boolean = {
    require(timeout >=0, "timeout should be >= 0")

    val startTime = time.milliseconds()
    val expiryTime = startTime + timeout

    @tailrec
    def awaitReady(iterationStartTime: Long): Boolean = {
      if (client.isReady(node, iterationStartTime))
        true
      else if (client.connectionFailed(node))
        throw new IOException(s"Connection to $node failed")
      else {
        val pollTimeout = expiryTime - iterationStartTime
        client.poll(pollTimeout, iterationStartTime)
        val afterPollTime = time.milliseconds()
        if (afterPollTime < expiryTime) awaitReady(afterPollTime)
        else false
      }
    }

    isReady(node) || client.ready(node, startTime) || awaitReady(startTime)
  }

  /**
   * Invokes `client.send` followed by 1 or more `client.poll` invocations until a response is received or a
   * disconnection happens (which can happen for a number of reasons including a request timeout).
   *
   * In case of a disconnection, an `IOException` is thrown.
   *
   * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
   * care.
   *
   * 先调用`client.send`发送请求，然后通过1次或多次`client.poll`调用轮询，直到收到响应或连接断开
   * （连接断开可能由多种原因导致，包括请求超时）。
   *
   * 若发生连接断开，将抛出`IOException`。
   *
   * 该方法用于在非阻塞的`NetworkClient`基础上实现阻塞行为，使用时需谨慎（避免过度阻塞影响性能）。
   *
   * @param request 待发送的客户端请求对象
   * @param time 时间工具类，用于获取当前时间
   * @return 从服务器接收到的响应对象
   */
  def blockingSendAndReceive(request: ClientRequest)(implicit time: Time): ClientResponse = {
    // 发送请求到目标节点，参数为当前时间戳（用于计算超时）
    client.send(request, time.milliseconds())

    // 持续轮询直到获取目标响应
    pollContinuously { responses =>
      // 在响应集合中查找与请求关联ID匹配的响应
      val response = responses.find { response =>
        response.requestHeader.correlationId == request.correlationId
      }
      // 若找到匹配的响应，检查是否发生断开连接或版本不匹配
      response.foreach { r =>
        // 连接已断开时抛出异常
        if (r.wasDisconnected)
          throw new IOException(s"Connection to ${request.destination} was disconnected before the response was read")
        else if (r.versionMismatch() != null)
          // 协议版本不匹配时抛出异常（通常是客户端与broker版本不兼容）
          throw r.versionMismatch();
      }
      // 返回找到的响应（若未找到则继续轮询）
      response
    }
  }

  /**
    * Invokes `client.poll` until `collect` returns `Some`. The value inside `Some` is returned.
    *
    * Exceptions thrown via `collect` are not handled and will bubble up.
    *
    * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
    * care.
    */
  private def pollContinuously[T](collect: Seq[ClientResponse] => Option[T])(implicit time: Time): T = {

    @tailrec
    def recursivePoll: T = {
      // rely on request timeout to ensure we don't block forever
      val responses = client.poll(Long.MaxValue, time.milliseconds()).asScala
      collect(responses) match {
        case Some(result) => result
        case None => recursivePoll
      }
    }

    recursivePoll
  }

}
