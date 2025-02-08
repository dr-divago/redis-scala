package codecrafters_redis

object RDBDecoder {
  def isRedisRDB(content: Array[Byte]) : Boolean = {
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

  def findMetadata(fileByte: Array[Byte]) : Int = fileByte.indexOf(0xfa.toByte, 0)
  def findStartDB(fileByte: Array[Byte]): Int = fileByte.indexOf(0xfe.toByte, 0)
  def findStartHashTable(fileByte : Array[Byte]): Int = fileByte.indexOf(0xfb.toByte, 0)
  def sizeHashTable(fileByte: Array[Byte]) : Int = {
    val indexStartHashtable = findStartHashTable(fileByte)
    fileByte(indexStartHashtable+1)
  }
}
