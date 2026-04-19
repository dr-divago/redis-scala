package codecrafters_redis.server

import codecrafters_redis.Logger
import codecrafters_redis.command.{ConnectionEstablished, DimensionReplication, Event, Psync, RdbDataReceived, ReplConfAck, ReplConfGetAck, Wait}
import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.{Connection, EventLoop, EventSource, NIOEventSource, ProtocolManager, ReplicationState}
import codecrafters_redis.protocol.resp.{Incomplete, ParseState, Parsed, RespBulkString, RespStreamParser, RespString, WaitingForCommand}
import codecrafters_redis.server.processor.{EventProcessor, ResultHandler}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.util.Success

case class ServerContext(connections: TrieMap[SocketChannel, Connection])

trait ServerOperations {
  def handleNewClient(clientChannel: SocketChannel, key: SelectionKey): EventResult
  def handleConnectionReadable(connection: Connection): EventResult
  def handleConnectionEstablished(connection: Connection): EventResult
}

sealed trait ServerMode {
  def resultHandler: ResultHandler
}
case class MasterMode(context: Context) extends ServerMode {
  override def resultHandler: ResultHandler = new MasterResultHandler(context)
}

case class ReplicaMode(context: Context, replicationState: ReplicationState) extends ServerMode {
  override def resultHandler: ResultHandler = new ReplicaResultHandler(context, replicationState)
}

class MasterResultHandler(context: Context) extends ResultHandler {
  override def handle(result: EventResult): Unit = {
    result match {
      case DataReceived(connection, data) =>
        Logger.debug(s"Master received on ${connection.socketChannel}: hex=[${data.take(16).map(b => f"${b & 0xff}%02x").mkString(" ")}] str=${new String(data)}")
        val commands = connection.process(data)
        commands.foreach {
          case ReplConfAck(offset) =>
            context.replicaAckOffsets.put(connection.socketChannel, offset)
          case Psync =>
            val header = Psync.execute(context)
            val rdbBytes = Files.readAllBytes(Paths.get("empty.rdb"))
            val combined = new Array[Byte](header.length + rdbBytes.length)
            System.arraycopy(header, 0, combined, 0, header.length)
            System.arraycopy(rdbBytes, 0, combined, header.length, rdbBytes.length)
            connection.write(combined)
            context.replicaChannels :+= connection.socketChannel
          case Wait(numReplicas, timeoutMs) =>
            handleWait(numReplicas, timeoutMs, connection)
          case cmd =>
            val response = cmd.execute(context)
            if (response.nonEmpty) connection.write(response)
        }
      case NoDataReceived(_) =>
      case ConnectionClosed(connection) =>
        connection.close()
        Logger.info(s"Connection closed: ${connection.socketChannel}")
      case ConnectionAccepted(connection) => Logger.info(s"Connection accepted: ${connection.socketChannel}")
    }
  }

  private def handleWait(numReplicas: Int, timeoutMs: Long, connection: Connection): Unit = {
    if (context.replicaChannels.isEmpty) {
      connection.write(s":0\r\n".getBytes)
      return
    }
    val targetOffset = context.masterReplOffsetLong
    if (targetOffset == 0 || numReplicas == 0) {
      val n = math.min(context.replicaChannels.size, if (numReplicas == 0) Int.MaxValue else numReplicas)
      connection.write(s":$n\r\n".getBytes)
      return
    }
    val alreadySynced = context.syncedReplicaCount(targetOffset)
    if (alreadySynced >= numReplicas) {
      connection.write(s":$alreadySynced\r\n".getBytes)
      return
    }
    val getAck = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".getBytes
    for (ch <- context.replicaChannels) {
      try { ch.write(ByteBuffer.wrap(getAck.clone())) } catch { case _: Exception => }
    }
    val t = targetOffset
    val nr = numReplicas
    val deadline = System.currentTimeMillis() + timeoutMs
    new Thread(() => {
      var done = false
      while (!done && System.currentTimeMillis() < deadline) {
        if (context.syncedReplicaCount(t) >= nr) done = true
        else Thread.sleep(10)
      }
      connection.write(s":${context.syncedReplicaCount(t)}\r\n".getBytes): Unit
    }, "wait-handler").start()
  }
}

class ReplicaResultHandler(context: Context, initialState: ReplicationState) extends ResultHandler {
  var replicationState: ReplicationState = initialState

  override def handle(result: EventResult): Unit = {
    result match {
      case DataReceived(connection, data) =>
        if (replicationState.isMasterConnection(connection.socketChannel)) {
          if (replicationState.isHandshakeDone) {
            val commands = connection.process(data)
            Logger.debug(s"Replica commands from master: $commands")
            val isOnlyGetAck = commands.size == 1 && commands.head == ReplConfGetAck
            if (!isOnlyGetAck) context.replicaOffset += data.length
            commands.foreach {
              case ReplConfGetAck =>
                connection.write(ReplConfGetAck.execute(context))
              case cmd =>
                cmd.execute(context)
            }
          } else {
            val events = parseHandshakeEvents(data)
            Logger.debug(s"Handshake events: $events")
            if (events.nonEmpty) {
              val (newState, actions) = ProtocolManager.processEvent(replicationState, events)
              ProtocolManager.executeAction(actions, newState.context)
              replicationState = newState
            }
          }
        } else {
          val commands = connection.process(data)
          Logger.debug(s"Replica client commands: $commands")
          commands.foreach { cmd =>
            val response = cmd.execute(context)
            if (response.nonEmpty) connection.write(response)
          }
        }
      case _ =>
    }
  }

  private val handshakeAccum = new java.io.ByteArrayOutputStream()

  private def parseHandshakeEvents(data: Array[Byte]): List[Event] = {
    handshakeAccum.write(data)
    val buf = ByteBuffer.wrap(handshakeAccum.toByteArray)
    val events = ListBuffer[Event]()

    var keepParsing = true
    while (keepParsing && buf.hasRemaining) {
      val startPos = buf.position()
      RespStreamParser.parse(buf, WaitingForCommand) match {
        case (Parsed(RespString(s), _), _) =>
          Event.fromArgs(s.split(" ").toVector).foreach(events += _)
        case (Parsed(RespBulkString(bytes), _), _) =>
          events += DimensionReplication(bytes.length)
          events += RdbDataReceived(bytes)
        case (Incomplete, _) =>
          buf.position(startPos)
          tryParseRawRdb(buf) match {
            case Some(rdbBytes) =>
              events += DimensionReplication(rdbBytes.length)
              events += RdbDataReceived(rdbBytes)
            case None =>
              keepParsing = false
          }
        case _ =>
          keepParsing = false
      }
    }

    handshakeAccum.reset()
    if (buf.hasRemaining) {
      val remaining = new Array[Byte](buf.remaining())
      buf.get(remaining)
      handshakeAccum.write(remaining)
    }

    events.toList
  }

  private def tryParseRawRdb(buf: ByteBuffer): Option[Array[Byte]] = {
    if (!buf.hasRemaining || buf.get(buf.position()) != '$') return None
    val startPos = buf.position()
    buf.get()
    val sizeBytes = new StringBuilder()
    var foundCrlf = false
    while (buf.hasRemaining && !foundCrlf) {
      val b = buf.get().toChar
      if (b == '\r' && buf.hasRemaining && buf.get(buf.position()) == '\n') {
        buf.get()
        foundCrlf = true
      } else sizeBytes += b
    }
    if (!foundCrlf) { buf.position(startPos); return None }
    val size = scala.util.Try(sizeBytes.toString.toInt).getOrElse { buf.position(startPos); return None }
    if (buf.remaining() < size) { buf.position(startPos); return None }
    val rdbBytes = new Array[Byte](size)
    buf.get(rdbBytes)
    Some(rdbBytes)
  }
}

case class RedisServer(context: Context, @volatile var mode: ServerMode, eventSource: EventSource, serverContext: ServerContext) extends ServerOperations {
  private val processor = new RedisEventProcessor()
  private val handler = mode match {
    case MasterMode(context) => new MasterResultHandler(context)
    case ReplicaMode(context, state) => new ReplicaResultHandler(context, state)
  }

  private val eventLoop: EventLoop = eventSource.subscribe(processor, handler)

  def start(): Unit = {
    eventLoop.start()
  }

  override def handleNewClient(clientChannel: SocketChannel, key: SelectionKey): EventResult = {
    val connection = Connection(clientChannel, key)
    serverContext.connections.addOne(clientChannel, connection)
    ConnectionAccepted(connection)
  }

  override def handleConnectionReadable(connection: Connection): EventResult = {
    connection.readIntoBuffer() match {
      case Success(bytesRead) if bytesRead > 0 =>
          connection.extractBytesFromBuffer() match {
            case Some(data) => DataReceived(connection, data)
            case None => NoDataReceived(connection)
          }
      case Success(0) => NoDataReceived(connection)
      case Success(-1) => ConnectionClosed(connection)
      case _ => ConnectionClosed(connection)
    }
  }

  override def handleConnectionEstablished(connection: Connection): EventResult = {
    connection.finishConnectOnChannel() match {
      case Success(true) =>
        Logger.info(s"Connected to ${connection.socketChannel.getRemoteAddress}")
        connection.key.interestOps(SelectionKey.OP_READ)
        mode match {
          case ReplicaMode(context, state) if state.context.connection.socketChannel.eq(connection.socketChannel) =>
            val (newState, actions) = ProtocolManager.processEvent(state, List(ConnectionEstablished))
            ProtocolManager.executeAction(actions, newState.context)
            mode = ReplicaMode(context, newState)
            handler match {
              case rh: ReplicaResultHandler => rh.replicationState = newState
              case _ =>
            }
          case _ =>
        }
        ConnectionAccepted(connection)
      case _ =>
        Logger.warn(s"Connection not established for ${connection.socketChannel}")
        ConnectionClosed(connection)
    }
  }


  private class RedisEventProcessor extends EventProcessor {
    def process(event: SocketEvent): EventResult = event match {
      case AcceptEvent(key) =>
        val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
        val clientChannel = serverChannel.accept()
        if (clientChannel != null) {
          clientChannel.configureBlocking(false)
          val newKey = clientChannel.register(key.selector(), SelectionKey.OP_READ)
          handleNewClient(clientChannel, newKey)
        } else {
          Completed
        }
      case ReadEvent(key) =>
        val connection = serverContext.connections.get(key.channel().asInstanceOf[SocketChannel])
        if (connection.isDefined) {
          handleConnectionReadable(connection.get)
        } else {
          Completed
        }
      case ConnectEvent(key) =>
        val connection = serverContext.connections.get(key.channel().asInstanceOf[SocketChannel])
        if (connection.isDefined) {
          handleConnectionEstablished(connection.get)
        } else {
          Completed
        }
    }
  }
}

object RedisServer {
  def apply(context: Context): RedisServer = {

    val serverContext = ServerContext(connections = TrieMap[SocketChannel, Connection]())

    val selector = Selector.open()
    val serverSocket = ServerSocketChannel.open()
    serverSocket.configureBlocking(false)
    serverSocket.bind(new InetSocketAddress("localhost", context.getPort))
    serverSocket.register(selector, SelectionKey.OP_ACCEPT)

    val eventSource = NIOEventSource(selector)
    val initMode: ServerMode = if (context.isReplica) {
      val replicationState = createReplicationState(
        context.config,
        selector,
        serverContext
      )
      ReplicaMode(context, replicationState)
    }
    else {
      MasterMode(context)
    }
    new RedisServer(context, initMode, eventSource, serverContext)
  }

  private def createReplicationState(config: Config, selector: Selector, serverContext: ServerContext) = {
    val masterIpPort = config.replicaof.split(" ")
    val masterChannel = SocketChannel.open()
    masterChannel.configureBlocking(false)

    Logger.info(s"Connecting to master ${masterIpPort(0)}:${masterIpPort(1).toInt}")

    val connected = masterChannel.connect(new InetSocketAddress(masterIpPort(0), masterIpPort(1).toInt))

    val key = if (connected) {
      Logger.info("Connected to master immediately")
      masterChannel.register(selector, SelectionKey.OP_READ)
    } else {
      Logger.info("Waiting for master connection (OP_CONNECT)")
      masterChannel.register(selector, SelectionKey.OP_CONNECT)
    }

    val connectionToMaster = Connection(masterChannel, key)
    serverContext.connections.addOne(masterChannel, connectionToMaster)

    val initialState = ProtocolManager(connectionToMaster)

    if (connected) {
      val (newState, actions) = ProtocolManager.processEvent(initialState, List(ConnectionEstablished))
      ProtocolManager.executeAction(actions, newState.context)
      newState
    } else {
      initialState
    }
  }
}