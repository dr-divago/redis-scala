package codecrafters_redis.eventloop

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

sealed trait State {
  def name :String
  def handle(event: Event, context: ReplicationContext) : (State, Action)
}

case object Connecting extends State {
  def name : String = "CONNECTING"

  override def handle(event: Event, context: ReplicationContext): (State, Action) = event match {
    case ConnectionEstablished => (PingHandshake, SendPingCommand)
    case _ =>                     (this, NoAction)
  }
}

case object PingHandshake extends State {
  def name : String = "PING_HANDSHAKE"

  override def handle(event: Event, context: ReplicationContext): (State, Action) = event match {
    case ResponseReceived(resp) if resp.contains("+PONG") =>
      (ReplConfListingPortFirst, SendReplConfListeningCommand(context.port))
  }
}

case object ReplConfListingPortFirst extends State {
  override def name: String = "REPLCONF_LISTENING_PORT"
  override def handle(event: Event, context: ReplicationContext): (State, Action) = event match {
    case ResponseReceived(response) if response.contains("+OK") =>
      (ReplConfCapaSync, SendReplConfCapa)
  }
}

case object ReplConfCapaSync extends State {
  override def name: String = "REPLCONF_CAPA_PSYNC2"

  override def handle(event: Event, context: ReplicationContext): (State, Action) = event match {
    case ResponseReceived(response) if response.contains("+OK") =>
      (Completed, SendPsyncCommand)
  }
}

case object Completed extends State {
  override def name: String = "PSYNC"

  override def handle(event: Event, context: ReplicationContext): (State, Action) = event match {
    case ResponseReceived(response) if response.contains("+FULLRESYNC") =>
      (this, NoAction)
    case _ => (this, NoAction)
  }
}


sealed trait Event
case object ConnectionEstablished extends Event
case class ResponseReceived(response: String) extends Event
case object ConnectionClosed extends Event

sealed trait Action {
  def execute(channel : SocketChannel) : Unit
}
case object NoAction extends Action {
  override def execute(channel: SocketChannel): Unit = {
    println("No Action to execute")
  }
}
case object SendPingCommand extends Action {
  override def execute(channel: SocketChannel): Unit = {
    println("Sending PING command to master")
    val pingCommand = "*1\r\n$4\r\nPING\r\n"
    val buffer = ByteBuffer.wrap(pingCommand.getBytes)
    channel.write(buffer)
  }
}

case class SendReplConfListeningCommand(port : Int) extends Action {
  override def execute(channel: SocketChannel): Unit = {
    println(s"Sending REPLCONF listening-port $port command")
    val portStr = port.toString
    val command = s"*3\r\n$$8\r\nREPLCONF\r\n$$14\r\nlistening-port\r\n$$${portStr.length}\r\n$portStr\r\n"
    val buffer = ByteBuffer.wrap(command.getBytes)
    channel.write(buffer)
  }
}

case object SendReplConfCapa extends Action {
  def execute(channel: SocketChannel): Unit = {
    println("Sending REPLCONF capa command")
    val command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    val buffer = ByteBuffer.wrap(command.getBytes)
    channel.write(buffer)
  }
}

case object SendPsyncCommand extends Action {
  override def execute(channel: SocketChannel): Unit = {
    val command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    val buffer = ByteBuffer.wrap(command.getBytes)
    channel.write(buffer)
  }
}

case class ReplicationContext(port :Int, masterChannel: Option[SocketChannel] = None)
case class ReplicationState(state: State, context : ReplicationContext)

object ProtocolManager {
  def apply(socketChannel: SocketChannel, port: Int): ReplicationState = {
    ReplicationState(Connecting, ReplicationContext(port, Some(socketChannel)))
  }

  def processEvent(currentState : ReplicationState, event : Event): (ReplicationState, Action) = {
    println(s"Received event $event")

    if (event == ConnectionClosed) {
      println("Connection closed, reset to Connecting state")
      return (ReplicationState(Connecting, currentState.context.copy(masterChannel = None)), NoAction)
    }

    val (newState, action) = currentState.state.handle(event, currentState.context)
    println(s"New state ${newState.name} action to execute ${action.toString}")
    (ReplicationState(newState, currentState.context), action)
  }

  def executeAction(action: Action, context: ReplicationContext) : Unit = {
    println(s"Executing ${action.toString} with channel ${context.masterChannel.get}")
    context.masterChannel.foreach{ channel => action.execute(channel) }
  }

}
