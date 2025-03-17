package codecrafters_redis.command

import codecrafters_redis.db.{ExpiresIn, MemoryDB, NeverExpires}
import codecrafters_redis.protocol.{Continue, Parsed, ParserResult}

import scala.util.Try

sealed trait Command {
  def execute(memoryDB : MemoryDB) : String
}
case class Ping() extends Command {
  override def execute(memoryDB: MemoryDB): String = "+PONG\r\n"
}

case class Echo(message: String) extends Command {
  override def execute(memoryDB: MemoryDB): String = s"$$${message.length}\r\n$message\r\n"
}

case class Set(key: String, value: String, expiry: Option[Int] = None) extends Command {
  override def execute(db: MemoryDB): String = {
    db.add(key, value, expiry.map(ExpiresIn(_)).getOrElse(NeverExpires()))
    "+OK\r\n"
  }
}

case class Get(key: String) extends Command {
  override def execute(db: MemoryDB): String = {
    db.get(key) match {
      case Some(value) => s"$$${value.length}\r\n$value\r\n"
      case None => "$-1\r\n"
    }
  }
}


object Command {

  private val pingParser : PartialFunction[Vector[String], Command] = {
    case Vector("PING") => Ping()
  }

  private val echoParser : PartialFunction[Vector[String], Command] = {
    case Vector("ECHO", message) => Echo(message)
  }

  private val setParser : PartialFunction[Vector[String], Command] = {
    case Vector("SET", key, value) => Set(key, value)
    case Vector("SET", key, value, "PX", milliseconds) if Try(milliseconds.toInt).isSuccess => Set(key, value, Some(milliseconds.toInt))
  }

  private val getParser : PartialFunction[Vector[String], Command] = {
    case Vector("GET", value) => Get(value)
  }

  private val commandParser : PartialFunction[Vector[String], Command] = {
      pingParser  orElse
      echoParser  orElse
      setParser   orElse
      getParser
  }

  def parse(parserResult: ParserResult): Option[Command] = parserResult match {
    case Parsed(args, _) => fromArgs(args)
    case Continue(_) => None
  }

  private def fromArgs(args: Vector[String]): Option[Command]= {
    if (commandParser.isDefinedAt(args)) Some(commandParser(args)) else None
  }
}
