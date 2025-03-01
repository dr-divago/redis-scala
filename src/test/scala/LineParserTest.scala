import codecrafters_redis.eventloop.LineParser
import org.scalatest.funsuite.AnyFunSuite

class LineParserTest extends AnyFunSuite {
  test("Parse line") {
    val str = "abcd\r\n"
    val lineParser = new LineParser()
    lineParser.append(str)
    val parsedLine = lineParser.nextLine()
    assert(parsedLine.get.equals("abcd"))
  }

  test("Parse line multiple") {
    val str = "abcd\r\ndefg\r\n"
    val lineParser = new LineParser()
    lineParser.append(str)
    val parsedLine = lineParser.nextLine()
    assert(parsedLine.get.equals("abcd"))
    val parsedLineSecond = lineParser.nextLine()
    assert(parsedLineSecond.get.equals("defg"))
  }

}
