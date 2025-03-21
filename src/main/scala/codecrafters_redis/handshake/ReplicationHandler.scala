package codecrafters_redis.handshake

import codecrafters_redis.command.Event
import codecrafters_redis.eventloop.LineParser

class ReplicationHandler {
  private val lineParser = new LineParser()

  private sealed trait HandshakeState
  private case object WaitingForFullResync extends HandshakeState
  private case object WaitingForDimension extends HandshakeState
  private case class WaitingForRdbData(dim: Int, received : Int) extends HandshakeState

  private var currentState: HandshakeState = WaitingForFullResync

  def process(data: Array[Byte]) : List[Event] = {
    currentState match {
      case WaitingForFullResync | WaitingForDimension =>
        lineParser.append(new String(data))

      case WaitingForRdbData(dim, received) =>
        processRdbData(data, dim, received)
    }
    List.empty
  }

  private def processLines() : List[Event] = {

    List.empty
  }

  private def processRdbData(data: Array[Byte], dim: Int, received: Int) = {

  }

}
