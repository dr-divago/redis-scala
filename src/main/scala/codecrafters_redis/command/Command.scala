package codecrafters_redis.command

import codecrafters_redis.config.Context
import codecrafters_redis.db.{ExpiresIn, MemoryDB, NeverExpires}
import codecrafters_redis.protocol.{Continue, Parsed, ParserResult}

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
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
    "+OK\r\n".getBytes
  }
}

case class Get(key: String) extends Command {
  override def execute(context: Context): Array[Byte] = {
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
    s"+FULLRESYNC $masterId ${context.getMasterReplOffset}\r\n$$88\r\n".getBytes
  }
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
    case Vector("SET", key, value, "px", milliseconds) if Try(milliseconds.toInt).isSuccess => Set(key, value, Some(milliseconds.toInt))
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
  }

  private val psyncParser : PartialFunction[Vector[String], Command] = {
    case Vector("PSYNC", "?", _) => Psync
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
    psyncParser
  )

  private val commandParser : PartialFunction[Vector[String], Command] = {
     parsers.foldLeft(PartialFunction.empty[Vector[String], Command]) {
       (acc, parser) => acc orElse parser}
  }

  def parse(parserResult: ParserResult): Option[Command] = parserResult match {
    case Parsed(args, _) => fromArgs(args)
    case Continue(_) => None
  }

  private def fromArgs(args: Vector[String]): Option[Command]= {
    if (commandParser.isDefinedAt(args)) Some(commandParser(args)) else None
  }
}
