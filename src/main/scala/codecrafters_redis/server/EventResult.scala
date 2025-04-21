package codecrafters_redis.server

import codecrafters_redis.eventloop.Connection

import java.nio.channels.{SelectionKey, SocketChannel}

sealed trait EventResult
case class ConnectionAccepted(connection: Connection) extends EventResult
case class DataReceived(connection: Connection, data: Array[Byte]) extends EventResult
case class ConnectionClosed(connection: Connection) extends EventResult
case class NoDataReceived(connection: Connection) extends EventResult
case object Completed extends EventResult
case class Failure(error: Throwable) extends EventResult

sealed trait SocketEvent
case class AcceptEvent(key: SelectionKey) extends SocketEvent
case class ReadEvent(key: SelectionKey) extends SocketEvent
case class ConnectEvent(key: SelectionKey) extends SocketEvent
case class WriteEvent(key: SelectionKey) extends SocketEvent



