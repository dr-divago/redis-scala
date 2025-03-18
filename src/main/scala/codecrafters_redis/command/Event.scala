package codecrafters_redis.command

import codecrafters_redis.protocol.{Continue, Parsed, ParserResult}

sealed trait Event
case object ConnectionEstablished extends Event
case object ConnectionClosed extends Event
case object PongEvent extends Event
case object OkEvent extends Event
case object FullResync extends Event
case class DimensionReplication(dim: Int) extends Event


object Event {

  private val pongParser : PartialFunction[Vector[String], Event] = {
    case Vector("PONG") => PongEvent
  }

  private val okParser : PartialFunction[Vector[String], Event] = {
    case Vector("OK") => OkEvent
  }

  private val fullResync : PartialFunction[Vector[String], Event] = {
    case Vector("FULLRESYNC", id, number) => FullResync
  }

  private val dimensionReplication : PartialFunction[Vector[String], Event] = {
    case Vector(dim) => DimensionReplication(dim.toInt)
  }

  private val parsers: List[PartialFunction[Vector[String], Event]] = List(
    pongParser,
    okParser,
    fullResync,
    dimensionReplication
  )

  private val commandParser : PartialFunction[Vector[String], Event] = {
    parsers.foldLeft(PartialFunction.empty[Vector[String], Event]) {
      (acc, parser) => acc orElse parser}
  }

  def parse(parserResult: ParserResult): Option[Event] = parserResult match {
    case Parsed(args, _) => fromArgs(args)
    case Continue(_) => None
  }

  private def fromArgs(args: Vector[String]): Option[Event]= {
    if (commandParser.isDefinedAt(args)) Some(commandParser(args)) else None
  }

}
