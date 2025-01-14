import codecrafters_redis.{ Continue, ParseState, Parsed, ProtocolParser, WaitingForCommand}
import org.scalatest.funsuite.AnyFunSuite


class ProtocolParserTest extends AnyFunSuite {
  var parser = new ProtocolParser

  test("Parse complex command") {
    //val command = List("*1\r\n", "$4\r\n", "PING\r\n")
    //processCommand(command, WaitingForCommand())
    val commandLong = List("*2\r\n", "$4\r\n", "ECHO", "$3\r\n", "hey")
    processCommand(commandLong, WaitingForCommand())

  }

  def processCommand(command: List[String], currentState : ParseState) : Unit = {
    if (command.nonEmpty) {
      parser.parse(command.head, currentState) match {
        case Parsed(value, nextState) =>
          println(s"Got $value")
          processCommand(command.tail, nextState)
        case Continue(nextState) => processCommand(command.tail, nextState)
      }
    }
  }
}
