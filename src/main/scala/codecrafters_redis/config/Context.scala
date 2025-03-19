package codecrafters_redis.config

import codecrafters_redis.db.{ExpiresAt, MemoryDB, NeverExpires}
import codecrafters_redis.protocol.RDBDecoder

import java.io.File
import java.net.Socket
import java.nio.channels.SocketChannel
import java.nio.file.{Files, Paths}
import scala.collection.mutable

case class Context(config: Config,
                   masterSocket: Option[Socket] = None) {

  private lazy val memoryDBInstance: MemoryDB = {
    val memoryDB = new MemoryDB

    if (config.dbParam.isEmpty) {
      memoryDB
    } else {
      val path = Paths.get(config.dirParam + File.separator + config.dbParam)
      if (!Files.exists(path)) {
        memoryDB
      } else {
        val fileByte: Array[Byte] = Files.readAllBytes(path)
        RDBDecoder.readKeyValue(fileByte).foreach(kv => {
          if (kv._3.isDefined) {
            memoryDB.add(kv._1, kv._2, ExpiresAt(kv._3.get))
          } else {
            memoryDB.add(kv._1, kv._2, NeverExpires())
          }
        })
        memoryDB
      }
    }
  }

  def getDB: MemoryDB = memoryDBInstance

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

  def getMasterReplOffset : String = {
    "0"
  }

  def getMasterIdStr : String = {
    s"master_replid:$getMasterId"
  }

  def getMasterId : String = {
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
  }

  var replicaChannels: mutable.Seq[SocketChannel] = mutable.ArrayBuffer[SocketChannel]()
}
