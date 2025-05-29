package protocol

import codecrafters_redis.protocol.resp.{Error, Incomplete, Parsed, ParserResult, RespArrayHeader, RespBulkString, RespError, RespInteger, RespNullArray, RespNullBulkString, RespProtocolParser, RespString}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.ByteBuffer

class RespProtocolParserTests extends AnyFlatSpec with Matchers {

  def createBuffer(s: String): ByteBuffer = {
    ByteBuffer.wrap(s.getBytes("UTF-8"))
  }

  "The Redis parser" should "return Incomplete when buffer is empty" in {
    val result = RespProtocolParser.parse(ByteBuffer.allocate(0))
    result shouldBe Incomplete
  }

  it should "handle partial data properly" in {
    val testPartial = "+OK"
    val buffer = createBuffer(testPartial)
    val result = RespProtocolParser.parse(buffer)
    result shouldBe Incomplete
  }

  it should "parse simple string" in {
    val testSimpleString = "+OK\r\n"
    val buffer = createBuffer(testSimpleString)

    val result = RespProtocolParser.parse(buffer)
    result shouldBe Parsed(RespString("OK"), 5)
  }

  it should "parse error" in {
    val testError = "-ERR\r\n"
    val buffer = createBuffer(testError)

    val result = RespProtocolParser.parse(buffer)
    result shouldBe Parsed(RespError("ERR"), 6)
  }

  it should "parse integer" in {
    val testInteger = ":123\r\n"
    val buffer = createBuffer(testInteger)

    val result = RespProtocolParser.parse(buffer)
    result shouldBe Parsed(RespInteger(123), 6)
  }

  it should "parse array header" in {
    val testArray = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    val buffer = createBuffer(testArray)

    val result = RespProtocolParser.parse(buffer)
    result shouldBe Parsed(RespArrayHeader(2), 4)
  }

  it should "parse error for unknown command" in {
    val testError = "?Unknown command\r\n"
    val buffer = createBuffer(testError)

    val result = RespProtocolParser.parse(buffer)
    result shouldBe Error("Unknown first byte")
  }

  it should "parse bulk string" in {
    val testBulkString = "$5\r\nhello\r\n"
    val buffer = createBuffer(testBulkString)

    val result = RespProtocolParser.parse(buffer)
    val arr = Array[Byte](104, 101, 108, 108, 111)
    val expectedConsumed = 11

    areEqual(result, arr, expectedConsumed)
  }

  private def areEqual(result: ParserResult, arr: Array[Byte], expectedConsumed: Int) = {
    result shouldBe a[Parsed]

    result match {
      case Parsed(respValue, consumed) =>
        consumed shouldBe expectedConsumed
        respValue shouldBe a[RespBulkString]

        respValue match {
          case RespBulkString(value) =>
            value should contain theSameElementsAs arr
          case other => fail(s"Unexpected value $other")
        }
      case other => fail(s"Unexpected result $other")
    }
  }

  it should "handle null bulk string" in {
    val testBulkString = "$-1\r\n"
    val buffer = createBuffer(testBulkString)

    val result = RespProtocolParser.parse(buffer)
    result shouldBe Parsed(RespNullBulkString(), 5)
  }

  it should "parse empty bulk string" in {
    val testBulkString = "$0\r\n\r\n"
    val buffer = createBuffer(testBulkString)

    val result = RespProtocolParser.parse(buffer)
    val expectedConsumed = 6
    val expectedValue = Array.empty[Byte]
    areEqual(result, expectedValue, expectedConsumed)
  }

  it should "handle null arrays" in {
    val testArray = "*-1\r\n"
    val buffer = createBuffer(testArray)

    val result = RespProtocolParser.parse(buffer)
    result shouldBe Parsed(RespNullArray(), 5)
  }

}
