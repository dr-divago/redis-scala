package codecrafters_redis.protocol

import org.scalactic.TimesOnInt.convertIntToRepeater

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ListBuffer

object RDBDecoder {
  def isRedisRDB(content: Array[Byte]): Boolean = {
    content(0) == 0x52 &&
      content(1) == 0x45 &&
      content(2) == 0x44 &&
      content(3) == 0x49 &&
      content(4) == 0x53 &&
      content(5) == 0x30 &&
      content(6) == 0x30 &&
      content(7) == 0x31 &&
      content(8) == 0x31
  }

  def findMetadata(fileByte: Array[Byte]): Int = fileByte.indexOf(0xfa.toByte, 0)

  def findStartDB(fileByte: Array[Byte]): Int = fileByte.indexOf(0xfe.toByte, 0)

  def findStartHashTable(fileByte: Array[Byte]): Int = fileByte.indexOf(0xfb.toByte, 0)

  def sizeHashTable(fileByte: Array[Byte]): Int = {
    val indexStartHashtable = findStartHashTable(fileByte)
    fileByte(indexStartHashtable + 1)
  }

  def readKeyValue(fileByte: Array[Byte]): List[(String, String)] = {
    val startHT = findStartHashTable(fileByte)
    val sizeHT = sizeHashTable(fileByte)
    var index = startHT + 4
    val res = ListBuffer.empty[(String, String)]
    sizeHT.times {
      val (sizeKey, key) = Decoder.decodeString(fileByte.drop(index))
      val (sizeValue, value) = Decoder.decodeString(fileByte.drop(index + sizeKey + 1))
      index = index + (sizeKey + 1) + (sizeValue + 1) + 1
      res.append((key, value))
    }
    res.toList
  }

  def readKeyValueWithExpire(fileByte: Array[Byte]): (Option[Long], Option[Long]) = {
    val startKeyWithExpireSec = findKeyWithExpireSec(fileByte)

    if (startKeyWithExpireSec != -1) {
      val value = ByteBuffer.wrap(Array(
          fileByte(startKeyWithExpireSec),
          fileByte(startKeyWithExpireSec + 1),
          fileByte(startKeyWithExpireSec + 2),
          fileByte(startKeyWithExpireSec + 3),
          fileByte(startKeyWithExpireSec + 4),
          fileByte(startKeyWithExpireSec + 5),
          fileByte(startKeyWithExpireSec + 6),
          fileByte(startKeyWithExpireSec + 7)))
        .order(ByteOrder.LITTLE_ENDIAN).getLong()

    }
    val startKeyWithExpireMilli = findKeyWithExpireMillis(fileByte)
    if (startKeyWithExpireMilli != -1) {
      val value = ByteBuffer.wrap(Array(
          fileByte(startKeyWithExpireSec),
          fileByte(startKeyWithExpireSec + 1),
          fileByte(startKeyWithExpireSec + 2),
          fileByte(startKeyWithExpireSec + 3),
          fileByte(startKeyWithExpireSec + 4),
          fileByte(startKeyWithExpireSec + 5),
          fileByte(startKeyWithExpireSec + 6),
          fileByte(startKeyWithExpireSec + 7)))
        .order(ByteOrder.LITTLE_ENDIAN).getLong()


    }
  }

  private def findKeyWithExpireMillis(fileByte: Array[Byte]) = fileByte.indexOf(0xfd.toByte, 0)

  private def findKeyWithExpireSec(fileByte: Array[Byte]) = fileByte.indexOf(0xfc.toByte, 0)
}
