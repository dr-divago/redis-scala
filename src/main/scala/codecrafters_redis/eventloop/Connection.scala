package codecrafters_redis.eventloop

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import scala.collection.mutable

case class Connection(socketChannel: SocketChannel,
                      buffer: ByteBuffer = ByteBuffer.allocate(1024),
                      lineParser: LineParser = new LineParser(),
                      tasks: mutable.Queue[Task] = mutable.Queue.empty) {

  def addTask(task : Task) : Unit = {
    tasks.enqueue(task)
  }

  def nextTask() : Option[Task] = {
    if (tasks.isEmpty) None
    else Some(tasks.dequeue())
  }

  def addData(data: String) : Unit = {
    lineParser.append(data)
  }
}



object Connection {
  def apply(socketChannel: SocketChannel, initialTask: Task) : Connection = {
    val connection = Connection(socketChannel)
    connection.addTask(initialTask)
    connection
  }
}

