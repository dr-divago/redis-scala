package codecrafters_redis.eventloop

import java.net.Socket
import scala.collection.immutable.Queue

case class TaskQueue() {
  var taskQueue: Map[Socket, Queue[Task]] = Map.empty
  def addTask(task : Task): Unit = {
    val queue = taskQueue.getOrElse(task.connection.socketChannel.socket(), Queue.empty)
    taskQueue = taskQueue + (task.connection.socketChannel.socket -> queue.enqueue(task))
  }
  def nextTask(socket: Socket) : Option[Task] = {
    taskQueue.get(socket).flatMap(_.headOption)
  }

  def removeFirstTask(socket: Socket): Unit = {
    taskQueue.get(socket).foreach{ queue =>
      if (queue.isEmpty) taskQueue = taskQueue - socket
      else taskQueue = taskQueue + (socket -> queue.tail)
    }
  }

  def isNonEmpty : Boolean = taskQueue.nonEmpty
}