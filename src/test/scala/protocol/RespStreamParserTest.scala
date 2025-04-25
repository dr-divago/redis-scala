package protocol

import codecrafters_redis.protocol.resp.{Error, Incomplete, Parsed, ParserResult, RespArrayHeader, RespBulkString, RespError, RespInteger, RespNullArray, RespNullBulkString, RespProtocolParser, RespStreamParser, RespString, WaitingForCommand, WaitingForNextElement}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.ByteBuffer

class RespStreamParserTest extends AnyFlatSpec with Matchers {

  def createBuffer(s: String): ByteBuffer = {
    ByteBuffer.wrap(s.getBytes("UTF-8"))
  }

  "The Redis parser" should "parse a simple string" in {
    val test = "+OK\r\n"
    val result = RespStreamParser.parse(ByteBuffer.wrap(test.getBytes), WaitingForCommand)
    result shouldBe (Parsed(RespString("OK"), 5), WaitingForCommand)
  }

  "The Redis parser" should "parse an array" in {
    val test = "*3\r\n:1\r\n:2\r\n:3\r\n"
    val result = RespStreamParser.parse(ByteBuffer.wrap(test.getBytes), WaitingForCommand)
  }
}
