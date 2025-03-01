import codecrafters_redis.protocol.Decoder
import org.scalatest.funsuite.AnyFunSuite


class DecoderTest extends AnyFunSuite {

  test("Parse Hello World") {
    val arr = Array[Byte](0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21)
    val (size, str) = Decoder.decodeString(arr)
    assert(size == 13)
    assert(str == "Hello, World!")
  }
  test("Parse C0 string") {
    val arr = Array[Byte](0xC0.toByte, 0x7B)
    val (size, str) = Decoder.decodeString(arr)
    assert(size == 1)
    assert(str == "123")
  }

  test("Parse C1 string") {
    val arr = Array[Byte](0xC1.toByte, 0x39, 0x30)
    val (size, str) = Decoder.decodeString(arr)
    assert(size == 2)
    assert(str == "12345")
  }

  test("Parse C2 string") {
    val arr = Array[Byte](0xC2.toByte, 0x87.toByte, 0xD6.toByte, 0x12.toByte, 0x00.toByte)
    val (size, str) = Decoder.decodeString(arr)
    assert(size == 4)
    assert(str == "1234567")
  }
}
