/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.InetSocketAddress

import akka.AkkaException
import akka.actor.{ Actor, ActorLogging, ActorRef, Stash }
import akka.annotation.InternalApi
import akka.io.Tcp._
import akka.io.dns.internal.DnsClient.{ Answer, DnsQuestion, Question4 }
import akka.io.{ IO, Tcp }
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TcpDnsClient(ns: InetSocketAddress, answerRecipient: ActorRef) extends Actor with ActorLogging with Stash {
  import TcpDnsClient._

  import context.system

  override def receive: Receive = idle

  val tcp = IO(Tcp)

  val idle: Receive = {
    case _: Message ⇒
      stash()
      log.debug("Connecting to [{}]", ns)
      tcp ! Tcp.Connect(ns)
      context.become(connecting)
  }

  val connecting: Receive = {
    case CommandFailed(_: Connect) ⇒
      log.warning("Failed to connect to [{}]", ns)
      throw new AkkaException("Connecting failed")
    case _: Tcp.Connected ⇒
      log.debug(s"Connected to TCP address [{}]", ns)
      val connection = sender()
      context.become(ready(connection))
      connection ! Register(self)
      unstashAll()
    case _: Message ⇒
      stash()
  }

  def ready(connection: ActorRef, buffer: ByteString = ByteString.empty): Receive = {
    case msg: Message ⇒
      val bytes = msg.write()
      connection ! Tcp.Write(encodeLength(bytes.length))
      connection ! Tcp.Write(bytes)
    case CommandFailed(_: Write) ⇒
      log.warning("Write failed")
      throw new AkkaException("Write failed")
    case Received(newData) ⇒
      val data = buffer ++ newData
      // TCP DNS responses are prefixed by 2 bytes encoding the length of the response
      val prefixSize = 2
      if (data.length < prefixSize)
        context.become(ready(connection, data))
      else {
        val expectedPayloadLength = decodeLength(data)
        if (data.drop(prefixSize).length < expectedPayloadLength)
          context.become(ready(connection, data))
        else {
          val payload = data.drop(prefixSize).take(expectedPayloadLength)
          answerRecipient ! parseResponse(payload)
          context.become(ready(connection))
          if (data.length > expectedPayloadLength + prefixSize) {
            self ! Received(data.drop(expectedPayloadLength + prefixSize))
          }
        }
      }
    case PeerClosed ⇒
      context.become(idle)
  }

  private def parseResponse(data: ByteString) = {
    val msg = Message.parse(data)
    log.debug(s"Decoded: $msg")
    if (msg.flags.isTruncated) {
      log.warning("TCP DNS response truncated")
    }
    val (recs, additionalRecs) = if (msg.flags.responseCode == ResponseCode.SUCCESS) (msg.answerRecs, msg.additionalRecs) else (Nil, Nil)
    Answer(msg.id, recs, additionalRecs)
  }
}
private[internal] object TcpDnsClient {
  def encodeLength(length: Int): ByteString =
    ByteString((length / 256).toByte, length.toByte)

  def decodeLength(data: ByteString): Int =
    ((data(0).toInt + 256) % 256) * 256 + ((data(1) + 256) % 256)
}
