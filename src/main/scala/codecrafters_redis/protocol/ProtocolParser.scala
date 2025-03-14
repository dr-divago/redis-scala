package codecrafters_redis.protocol

sealed trait ParserResult
case class Parsed(value: Vector[String], nextState: ParseState) extends ParserResult
case class Continue(nextState: ParseState) extends ParserResult

sealed trait ParseState
case class WaitingForCommand() extends ParseState
case class WaitingForBulkString(currentValue: Vector[String], remaining: Int) extends ParseState
case class ParsingValue(currentValue: Vector[String], remaining : Int, length: Int) extends ParseState

object ProtocolParser {
  def parse(input: String, state: ParseState) : ParserResult = {
    state match {
      case WaitingForCommand() =>
        input.head match {
          case '*' => Continue(WaitingForBulkString(Vector(), input.tail.toInt))
          case '+' => Parsed(Vector(input.tail), WaitingForCommand())
          case _   => throw new RuntimeException("First command should be a Array")
        }
      case WaitingForBulkString(currentValue, remaining) =>
        input.head match {
          case '$' => Continue(ParsingValue(currentValue, remaining, input.tail.toInt))
          case ':' => Parsed(currentValue :+ input.tail, WaitingForCommand())
        }
      case ParsingValue(currentValue, remaining, _) =>
        remaining match {
          case 1 => Parsed(currentValue :+ input, WaitingForCommand())
          case _ => Continue(WaitingForBulkString(currentValue :+ input, remaining - 1))
        }
    }
  }
}
