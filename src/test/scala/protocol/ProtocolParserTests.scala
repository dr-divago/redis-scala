package protocol

import codecrafters_redis.protocol.{Continue, ProtocolParser, WaitingForBulkString, WaitingForCommand}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ProtocolParserTests extends AnyFlatSpec with Matchers {

  "ProtocolParser" should "initialize with an array command" in {
    val result = ProtocolParser.parse("*2", WaitingForCommand())
    result shouldBe a[Continue]
    result.asInstanceOf[Continue].nextState shouldBe WaitingForBulkString(Vector(), 2)
  }
}
