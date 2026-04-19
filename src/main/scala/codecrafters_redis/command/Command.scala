package codecrafters_redis.command

import codecrafters_redis.Logger
import codecrafters_redis.config.Context
import codecrafters_redis.db.{ExpiresIn, NeverExpires}
import codecrafters_redis.protocol.{Continue, Parsed, ParserResult}

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.util.Try

sealed trait Command {
  def execute(context : Context) : Array[Byte]
}
case object Ping extends Command {
  override def execute(context: Context): Array[Byte] = "+PONG\r\n".getBytes
}

case class Echo(message: String) extends Command {
  override def execute(context: Context): Array[Byte] = s"$$${message.length}\r\n$message\r\n".getBytes
}

case class Set(key: String, value: String, expiry: Option[Int] = None) extends Command {
  override def execute(context: Context): Array[Byte] = {
    context.getDB.add(key, value, expiry.map(ExpiresIn(_)).getOrElse(NeverExpires()))
    Logger.debug(s"SET keys ${context.getDB.keys()}")
    val (failed, cmdBytes) = propagateToReplicas(key, value, context.replicaChannels)
    if (failed.nonEmpty) {
      context.replicaChannels = context.replicaChannels.filterNot(failed.toSet)
    }
    context.incrementReplOffset(cmdBytes)
    "+OK\r\n".getBytes
  }

  private def propagateToReplicas(key: String, value: String, replicaChannels: collection.Seq[SocketChannel]): (Seq[SocketChannel], Int) = {
    val commandBytes = s"*3\r\n$$3\r\nSET\r\n$$${key.length}\r\n$key\r\n$$${value.length}\r\n$value\r\n".getBytes
    Logger.debug(s"Propagating SET to ${replicaChannels.size} replica(s)")
    val buffer = ByteBuffer.wrap(commandBytes)
    val failedChannels = mutable.ArrayBuffer[SocketChannel]()

    for (ch <- replicaChannels) {
      try {
        ch.write(buffer)
        buffer.rewind()
      } catch {
        case _: IOException =>
          Logger.error(s"Failed to propagate to replica ${ch.socket().getInetAddress}")
          try { ch.close() } catch { case _: IOException => }
          failedChannels += ch
      }
    }
    (failedChannels.toSeq, commandBytes.length)
  }
}

case class Get(key: String) extends Command {
  override def execute(context: Context): Array[Byte] = {
    Logger.debug(s"GET $key")
    context.getDB.get(key) match {
      case Some(value) => s"$$${value.length}\r\n$value\r\n".getBytes
      case None => "$-1\r\n".getBytes
    }
  }
}

case class Config(value: String) extends Command {
  override def execute(context: Context): Array[Byte] = {
    val conf = value match {
      case "dir" => context.config.dirParam
      case "dbfilename" => context.config.dbParam
    }
    s"*2\r\n$$${value.length}\r\n$value\r\n$$${conf.length}\r\n$conf\r\n".getBytes
  }
}

case object Keys extends Command {
  override def execute(context: Context): Array[Byte] = {
    val keys = context.getDB.keys()
    val sizeOfArrayResponse = s"*${keys.size}\r\n"
    val elements = keys.map(key => s"$$${key.length}\r\n$key\r\n").mkString
    (sizeOfArrayResponse + elements.mkString).getBytes
  }
}

case object Info extends Command {
  override def execute(context: Context): Array[Byte] = {
    val role = context.getReplication
    val masterId = context.getMasterIdStr
    val replicationId = s"master_repl_offset:${context.getMasterReplOffset}"
    val allResp = s"$role\n$masterId\n$replicationId\n"
    s"$$${allResp.length}\r\n$allResp\r\n".getBytes
  }
}

case object ReplicationConfig extends Command {
  override def execute(context: Context): Array[Byte] = "+OK\r\n".getBytes
}

case object Psync extends Command {
  override def execute(context: Context): Array[Byte] = {
    val masterId = context.getMasterId
    val rdbSize = Files.size(Paths.get("empty.rdb"))
    s"+FULLRESYNC $masterId ${context.getMasterReplOffset}\r\n$$${rdbSize}\r\n".getBytes
  }
}

case object ReplConfGetAck extends Command {
  override def execute(context: Context): Array[Byte] = {
    val offsetStr = context.replicaOffset.toString
    s"*3\r\n$$8\r\nREPLCONF\r\n$$3\r\nACK\r\n$$${offsetStr.length}\r\n$offsetStr\r\n".getBytes
  }
}

case class ReplConfAck(offset: Long) extends Command {
  override def execute(context: Context): Array[Byte] = Array.empty
}

case class Wait(numReplicas: Int, timeoutMs: Long) extends Command {
  override def execute(context: Context): Array[Byte] = Array.empty
}

case class UnknownCommand(tokens: Vector[String]) extends Command {
  override def execute(context: Context): Array[Byte] =
    s"-ERR unknown command '${tokens.headOption.getOrElse("")}'\r\n".getBytes
}


object Command {

  private val pingParser : PartialFunction[Vector[String], Command] = {
    case Vector("PING") => Ping
  }

  private val echoParser : PartialFunction[Vector[String], Command] = {
    case Vector("ECHO", message) => Echo(message)
  }

  private val setParser : PartialFunction[Vector[String], Command] = {
    case Vector("SET", key, value) => Set(key, value)
    case Vector("SET", key, value, px, ms) if px.equalsIgnoreCase("px") && Try(ms.toInt).isSuccess =>
      Set(key, value, Some(ms.toInt))
    case Vector("SET", key, value, ex, secs) if ex.equalsIgnoreCase("ex") && Try(secs.toInt).isSuccess =>
      Set(key, value, Some(secs.toInt * 1000))
  }

  private val getParser : PartialFunction[Vector[String], Command] = {
    case Vector("GET", value) => Get(value)
  }

  private val configParser : PartialFunction[Vector[String], Command] = {
    case Vector("CONFIG", "GET", value) => Config(value)
  }

  private val keysParser : PartialFunction[Vector[String], Command] = {
    case Vector("KEYS", "*") => Keys
  }

  private val infoParser : PartialFunction[Vector[String], Command] = {
    case Vector("INFO", "replication") => Info
  }

  private val replicationConfParser : PartialFunction[Vector[String], Command] = {
    case Vector("REPLCONF", "listening-port", _) => ReplicationConfig
    case Vector("REPLCONF", "capa", "psync2") => ReplicationConfig
    case Vector("REPLCONF", "GETACK", _) => ReplConfGetAck
    case Vector("REPLCONF", "ACK", offset) if Try(offset.toLong).isSuccess => ReplConfAck(offset.toLong)
  }

  private val psyncParser : PartialFunction[Vector[String], Command] = {
    case Vector("PSYNC", "?", _) => Psync
  }

  private val waitParser : PartialFunction[Vector[String], Command] = {
    case Vector("WAIT", n, t) if Try(n.toInt).isSuccess && Try(t.toLong).isSuccess =>
      Wait(n.toInt, t.toLong)
  }

  private val parsers: List[PartialFunction[Vector[String], Command]] = List(
    pingParser,
    echoParser,
    setParser,
    getParser,
    configParser,
    keysParser,
    infoParser,
    replicationConfParser,
    psyncParser,
    waitParser
  )

  private val commandParser : PartialFunction[Vector[String], Command] = {
     parsers.foldLeft(PartialFunction.empty[Vector[String], Command]) {
       (acc, parser) => acc orElse parser}
  }

  def parse(parserResult: ParserResult): Option[Command] = parserResult match {
    case Parsed(args, _) => fromArgs(args)
    case Continue(_) => None
  }

  def fromArgs(args: Vector[String]): Option[Command] = {
    if (commandParser.isDefinedAt(args)) Some(commandParser(args))
    else if (args.nonEmpty) Some(UnknownCommand(args))
    else None
  }
}
