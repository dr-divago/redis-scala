package codecrafters_redis

import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel

object EventLoop {
  var isRunning: Boolean = false
  val eventLoopState = EventLoopState.STOPPED
  val taskQueue = TaskQueue()

  def start(): Boolean = {
    if (isRunning) {
      return false;
    }
    this.isRunning = true
    val serverSocket = ServerSocketChannel.open()
    serverSocket.bind(new InetSocketAddress("localhost", 6379))
    serverSocket.configureBlocking(false)
    while(isRunning) {
      val clientSocket = serverSocket.accept()
      if (clientSocket != null) {
        taskQueue.addTask(new Task(clientSocket.socket(), _ => "+PONG\r\n"))
      }
      if (taskQueue.isNonEmpty) {
        val task = taskQueue.nextTask()
        if (task.in.ready()) {
          val command = task.in.readLine()
          if (command.equals("PING")) {
            val response = task.callBack(command)
            task.socket.getOutputStream.write(response.getBytes)
          }
        }
        taskQueue.addTask(task)
      }
    }
    isRunning
  }
}
