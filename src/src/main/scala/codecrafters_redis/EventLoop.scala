package codecrafters_redis

import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel

object EventLoop {
  var isRunning: Boolean = false
  val eventLoopState = EventLoopState.STOPPED
  val taskQueue = TaskQueue()
  val parser = new ProtocolParser()

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
        taskQueue.addTask(new Task(clientSocket.socket(), WaitingForCommand()))
      }
      if (taskQueue.isNonEmpty) {
        val task = taskQueue.nextTask()
        if (task.in.ready()) {
          val line = task.in.readLine()
          val t = parser.parse(line, task.currentState) match {
            case Parsed(value, nextState) =>
              value.head match {
                case "PING" => task.out.write("+PONG\r\n".getBytes)
                case "ECHO" => task.out.write((value(1)+"\r\n").getBytes)
              }
              new Task(task.socket, nextState)
            case Continue(nextState) => new Task(task.socket,nextState)
          }
          taskQueue.addTask(t)
        }
        else {
          taskQueue.addTask(task)
        }
      }
    }
    isRunning
  }

}
