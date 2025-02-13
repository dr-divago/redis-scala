package codecrafters_redis.protocol

import java.nio.{ByteBuffer, ByteOrder}

object Decoder {
  val C0 : Byte = 0xC0.toByte
  val C1 : Byte = 0XC1.toByte
  val C2 : Byte = 0XC2.toByte
  val C3 : Byte = 0XC2.toByte

  def decodeString(bytes: Array[Byte]) : (Int, String)  = {
    bytes(0) match {
      case C0 => decode8Bit(bytes)
      case C1 => decode16Bit(bytes)
      case C2 => decode32Bit(bytes)
      case C3 => decodeLZF(bytes)
      case _ => decodeNormal(bytes)
    }
  }

  private def decodeNormal(bytes: Array[Byte]) = {
    (bytes(0).toInt, bytes.slice(1, bytes(0).toInt + 1).map(_.toChar).mkString)
  }

  private def decodeLZF(bytes: Array[Byte]) = ???

  private def decode32Bit(bytes: Array[Byte]) = {
    val value = ByteBuffer.wrap(Array(bytes(1), bytes(2), bytes(3), bytes(4)))
      .order(ByteOrder.LITTLE_ENDIAN)
      .getInt() & 0xFFFFFFFFL
    (4, value.toString)
  }

  private def decode16Bit(bytes: Array[Byte]) = {
    val value = ByteBuffer.wrap(Array(bytes(1), bytes(2)))
      .order(ByteOrder.LITTLE_ENDIAN).
      getShort() & 0xFFFF
    (2, value.toString)
  }

  private def decode8Bit(bytes: Array[Byte]) = {
    (1, java.lang.Byte.toUnsignedInt(bytes(1)).toString)
  }

  def decode64bit(bytes: Array[Byte]) = {
    val value = ByteBuffer.wrap(Array(bytes(0), bytes(1), bytes(2), bytes(3), bytes(4), bytes(5), bytes(6), bytes(7)))
      .order(ByteOrder.LITTLE_ENDIAN).getLong()
    (8, value)
  }
}
