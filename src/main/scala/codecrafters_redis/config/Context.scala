package codecrafters_redis.config

import codecrafters_redis.db.{ExpiresAt, MemoryDB, NeverExpires}
import codecrafters_redis.protocol.RDBDecoder

import java.io.File
import java.net.Socket
import java.nio.channels.SocketChannel
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Vector

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

  private val _masterId: String = {
    val bytes = new Array[Byte](20)
    new SecureRandom().nextBytes(bytes)
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  @volatile private var _masterReplOffset: Long = 0

  def getMasterReplOffset: String = _masterReplOffset.toString

  def incrementReplOffset(bytes: Int): Unit = { _masterReplOffset += bytes }

  def getMasterIdStr: String = s"master_replid:$getMasterId"

  def getMasterId: String = _masterId

  @volatile var replicaChannels: Vector[SocketChannel] = Vector.empty

  @volatile var replicaOffset: Long = 0

  val replicaAckOffsets: TrieMap[SocketChannel, Long] = TrieMap.empty

  def isReplica: Boolean = {
    config.replicaof.nonEmpty
  }

  def syncedReplicaCount(targetOffset: Long): Int =
    replicaAckOffsets.count { case (_, off) => off >= targetOffset }

  def masterReplOffsetLong: Long = _masterReplOffset
}
