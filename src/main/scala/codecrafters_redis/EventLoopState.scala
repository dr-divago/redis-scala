package codecrafters_redis

object EventLoopState extends Enumeration {
  val INIT, RUNNING, STOPPED, SHUTTING_DOWN = Value
}