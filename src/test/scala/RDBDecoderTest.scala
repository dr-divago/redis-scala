import codecrafters_redis.RDBDecoder
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

class RDBDecoderTest extends AnyFunSuite {

  test("RDS file is a redis db") {
    val fileByte = Files.readAllBytes(Paths.get("dump.rdb"))
    assert(RDBDecoder.isRedisRDB(fileByte))
  }
}

