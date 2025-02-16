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

  def readKeyValue(fileByte: Array[Byte]): List[(String, String, Option[Long])] = {
    val startHT = findStartHashTable(fileByte)
    val sizeHT = fileByte(startHT+1)
    val sizeHTwithExpire = fileByte(startHT+2)

    val res = ListBuffer.empty[(String, String, Option[Long])]
    if (sizeHTwithExpire > 0) {
      var idx = startHT+3
      sizeHTwithExpire.times {
        val result = fileByte(idx) & 0xFF match {
          case 0xfc => Some(readExpirationForMilliSec(fileByte.drop(idx+1)))
          case 0xfd => Some(readExpirationForSec(fileByte.drop(idx+1)))
          case _ => None
        }
        val (sizeKey, key) = Decoder.decodeString(fileByte.drop(idx+result.get._2+1+1))
        val (sizeValue, value) = Decoder.decodeString(fileByte.drop(idx+result.get._2+sizeKey+1+1+1))
        idx = idx + result.get._2 + 1 + (sizeKey + 1) + (sizeValue + 1) + 1
        res.append((key, value, result.get._1))
      }
    }
    if (sizeHT - sizeHTwithExpire > 0) {
      var idx = startHT+4
      sizeHT.times {
        val (sizeKey, key) = Decoder.decodeString(fileByte.drop(idx))
        val (sizeValue, value) = Decoder.decodeString(fileByte.drop(idx+sizeKey+1))
        idx = idx + (sizeKey + 1) + (sizeValue + 1) + 1
        res.append((key, value, Option.empty))
      }
    }
    res.toList
  }

  private def readExpirationForSec(fileByte: Array[Byte]) = {
    def readLongAt(pos: Int, length: Int): Option[Long] = pos match {
      case -1 => None
      case n =>
        val bytes = new Array[Byte](length)
        for (i <- 0 until length) {
          bytes(i) = fileByte(n + i)
        }
        Some(ByteBuffer.wrap(bytes)
          .order(ByteOrder.LITTLE_ENDIAN)
          .getLong())
    }
    (readLongAt(0, 4), 4)
  }

  def readExpirationForMilliSec(fileByte: Array[Byte]) = {
    def readLongAt(pos: Int, length: Int): Option[Long] = pos match {
      case -1 => None
      case n =>
        val bytes = new Array[Byte](length)
        for (i <- 0 until length) {
          bytes(i) = fileByte(n + i)
        }
        Some(ByteBuffer.wrap(bytes)
          .order(ByteOrder.LITTLE_ENDIAN)
          .getLong())
    }

    (readLongAt(0, 8), 8)
  }
}
