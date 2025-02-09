package codecrafters_redis.eventloop

object EventLoopState extends Enumeration {
  val INIT, RUNNING, STOPPED, SHUTTING_DOWN = Value
}