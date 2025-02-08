import codecrafters_redis.RDBDecoder
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
}

