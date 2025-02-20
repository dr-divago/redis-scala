package codecrafters_redis.config

import codecrafters_redis.db.{ExpiresAt, InMemoryDB, NeverExpires}
import codecrafters_redis.protocol.RDBDecoder

import java.io.File
import java.nio.file.{Files, Paths}

case class Context(config: Config) {
  def getDB: InMemoryDB = {
    val inMemoryDB = new InMemoryDB
    if (config.dbParam.isEmpty)
      return inMemoryDB

    val path = Paths.get(config.dirParam+File.separator+config.dbParam)
    if (!Files.exists(path))
      return inMemoryDB

    val fileByte: Array[Byte] = Files.readAllBytes(path)
    RDBDecoder.readKeyValue(fileByte).foreach(kv => {
      if (kv._3.isDefined) {
        inMemoryDB.add(kv._1, kv._2, ExpiresAt(kv._3.get))
      }
      else {
        inMemoryDB.add(kv._1, kv._2, NeverExpires())
      }
    })
    inMemoryDB
  }

  def getPort: Int = {
    config.port.toInt
  }

  def getReplication : String = {
    if (config.replicaof.isEmpty) {
      s"role:master"
    } else {
      val replicaof = config.replicaof.split(" ")
      s"role:slave"
    }
  }
}
