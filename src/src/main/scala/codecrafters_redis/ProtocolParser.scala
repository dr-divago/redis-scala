package codecrafters_redis

sealed trait ParserResult
case class Parsed(value: Vector[String], nextState: ParseState) extends ParserResult
case class Continue(nextState: ParseState) extends ParserResult

sealed trait ParseState
case class WaitingForCommand() extends ParseState
case class WaitingForBulkString(currentValue: Vector[String], remaining: Int) extends ParseState
case class ParsingValue(currentValue: Vector[String], remaining : Int, length: Int) extends ParseState
case class Complete(value: Vector[String]) extends ParseState

class ProtocolParser {

  private val CRLF = "\r\n"
  private val initState = WaitingForCommand()


  def process(input: String): Unit = {

  }

  def processCommand(command: String, currentState : ParseState) : Unit = {
    parse(command, currentState) match {
      case Parsed(value, nextState) =>
          println(s"Got $value")
          processCommand(command.tail, nextState)
        case Continue(nextState) => processCommand(command.tail, nextState)
    }
  }

  def parse(input: String, state: ParseState) : ParserResult = {
    state match {
      case WaitingForCommand() =>
        input.head match {
          case '*' => Continue(WaitingForBulkString(Vector(), parseLength(input.tail)))
          case _   => throw new RuntimeException("First command should be a Array")
        }
      case WaitingForBulkString(currentValue, remaining) =>
        input.head match {
          case '$' => Continue(ParsingValue(currentValue, remaining, parseLength(input.tail)))
        }
      case ParsingValue(currentValue, remaining, length) =>
        remaining match {
          case 1 => Parsed(currentValue :+ input, WaitingForCommand())
          case _ => Continue(WaitingForBulkString(currentValue :+ input, remaining - 1))
        }
    }
  }
  private def parseLength(input: String): Int = {
    val indexOfCRLF = input.indexOf(CRLF)
    input.take(indexOfCRLF).toInt
  }
}
