package codecrafters_redis.eventloop

import codecrafters_redis.command._

import java.nio.channels.SocketChannel

sealed trait State {
  def name :String
  final def handle(event: Event, context: ReplicationContext) : (State, Action) = {
    event match {
      case ConnectionClosed => (Connecting, NoAction)
      case _ => handleEvent(event, context)
    }
  }

  protected def handleEvent(event: Event, context: ReplicationContext): (State, Action)
}


case object Connecting extends State {
  def name : String = "CONNECTING"

  override def handleEvent(event: Event, context: ReplicationContext): (State, Action) = event match {
    case ConnectionEstablished => (PingHandshake, SendPingCommand)
    case _ =>                     (this, NoAction)
  }
}

case object PingHandshake extends State {
  def name : String = "PING_HANDSHAKE"

  override def handleEvent(event: Event, context: ReplicationContext): (State, Action) = event match {
    case PongEvent => (ReplConfListingPortFirst, SendReplConfListeningCommand(context.connection.socketChannel.socket().getPort))
    case _ => (this, NoAction)
  }
}

case object ReplConfListingPortFirst extends State {
  override def name: String = "REPLCONF_LISTENING_PORT"
  override def handleEvent(event: Event, context: ReplicationContext): (State, Action) = event match {
    case OkEvent => (ReplConfCapaSync, SendReplConfCapa)
    case _ => (this, NoAction)
  }
}

case object ReplConfCapaSync extends State {
  override def name: String = "REPLCONF_CAPA_PSYNC2"

  override def handleEvent(event: Event, context: ReplicationContext): (State, Action) = event match {
    case OkEvent => (PSYNC, SendPsyncCommand)
    case _ => (this, NoAction)
  }
}

case object PSYNC extends State {
  override def name: String = "PSYNC"

  override def handleEvent(event: Event, context: ReplicationContext): (State, Action) = event match {
    case FullResync => (ParseRDBFile(), NoAction)
    case _ => (this, NoAction)
  }
}


case class ParseRDBFile(dimension: Option[Int] = None, bytesReceived: Int = 0) extends State {
  override def name: String = "PARSE_RDB_FILE"

  override def handleEvent(event: Event, context: ReplicationContext): (State, Action) = {
    event match {
      case DimensionReplication(dim) =>
        println(s"Parse RDB file dimension $dim")
        (ParseRDBFile(Some(dim)), NoAction)

      case RdbDataReceived(data) =>
        dimension match {
          case Some(dim) =>
            val totalBytesReceived = bytesReceived + data.length
            println(s"RDB data received: $totalBytesReceived/$dim bytes")

            if (totalBytesReceived >= dim) {
              println("RDB file completely received, handshake complete")
              (HandshakeComplete, SkipRdbFileAction(dim))
            } else {
              (ParseRDBFile(Some(dim), totalBytesReceived), NoAction)
            }

          case None =>
            println("Received RDB data but dimension is unknown")
            (this, NoAction)
        }

      case _ => (this, NoAction)
    }
  }
}

case object HandshakeComplete extends State {
  override def name: String = "HANDSHAKE_COMPLETE"

  override def handleEvent(event: Event, context: ReplicationContext): (State, Action) = event match {
    case _ => (this, NoAction)
  }
}

sealed trait Action {
  def execute(connection: Connection) : Unit
}
case object NoAction extends Action {
  override def execute(connection: Connection): Unit = {
    println("No Action to execute")
  }
}
case object SendPingCommand extends Action {
  override def execute(connection: Connection): Unit = {
    println("Sending PING command to master")
    val pingCommand = "*1\r\n$4\r\nPING\r\n"
    connection.write(pingCommand.getBytes)
  }
}

case class SendReplConfListeningCommand(port : Int) extends Action {
  override def execute(connection: Connection): Unit = {
    println(s"Sending REPLCONF listening-port $port command")
    val portStr = port.toString
    val command = s"*3\r\n$$8\r\nREPLCONF\r\n$$14\r\nlistening-port\r\n$$${portStr.length}\r\n$portStr\r\n"
    connection.write(command.getBytes)
  }
}

case object SendReplConfCapa extends Action {
  def execute(connection: Connection): Unit = {
    println("Sending REPLCONF capa command")
    val command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    connection.write(command.getBytes)
  }
}

case object SendPsyncCommand extends Action {
  override def execute(connection: Connection): Unit = {
    val command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    connection.write(command.getBytes)
  }
}

case class SkipRdbFileAction(bytesToSkip : Int) extends Action {
  override def execute(connection: Connection): Unit = connection.skipBytes(bytesToSkip)
}

case class ReplicationContext(connection: Connection)
case class ReplicationState(state: State, context : ReplicationContext) {
  def isHandshakeDone: Boolean = state == HandshakeComplete
  def isMasterConnection(client: SocketChannel) : Boolean = context.connection.socketChannel.eq(client)
}

object ProtocolManager {
  def apply(connection: Connection): ReplicationState = {
    ReplicationState(Connecting, ReplicationContext(connection))
  }

  def processEvent(currentState : ReplicationState, events : List[Event]): (ReplicationState, List[Action]) = {
    if (events.isEmpty) {
       return (currentState, List.empty[Action])
    }

    events.foldLeft(currentState, List.empty[Action]) {
      case ((state, actionList), event) =>
        val (newState, action) = state.state.handle(event, currentState.context)
        println(s"New state ${newState.name} action to execute ${action.toString}")
        (ReplicationState(newState, state.context), actionList :+ action)
    }
  }

  def executeAction(actions: List[Action], context: ReplicationContext) : Unit = {
    println(s"Executing ${actions.toString} with channel ${context.connection}")
    actions.foreach { action =>
      println(s"Executing ${actions.toString} with channel ${context.connection}")
      action.execute(context.connection)}
  }
}
