import codecrafters_redis.protocol.RDBDecoder
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

class RDBDecoderTest extends AnyFunSuite {

  test("RDS file is a redis db") {
    val fileByte = Files.readAllBytes(Paths.get("dump.rdb"))
    assert(RDBDecoder.isRedisRDB(fileByte))
  }

  test("Find index for RDS FA section") {
    val fileByte = Files.readAllBytes(Paths.get("dump.rdb"))
    assert(9 == RDBDecoder.findMetadata(fileByte))
  }

  test("Find index for RDS FE section") {
    val fileByte = Files.readAllBytes(Paths.get("dump.rdb"))
    assert(79 == RDBDecoder.findStartDB(fileByte))
  }

  test("Find size hashtable") {
    val fileByte = Files.readAllBytes(Paths.get("dump.rdb"))
    assert(1 == RDBDecoder.sizeHashTable(fileByte))
  }

  test("Find key in the hash") {
    val fileByte = Files.readAllBytes(Paths.get("dump.rdb"))
    val l = RDBDecoder.readKeyValue(fileByte)
    assert("mykey" == l.head._1)
    assert("myval" == l.head._2)
  }

  test("Find keys in db with many keys") {
    val fileByte = Files.readAllBytes(Paths.get("redis-db-key.rdb"))
    val l = RDBDecoder.readKeyValue(fileByte)
    assert("mykey" == l.head._1)
    assert("myval" == l.head._2)
  }
}

