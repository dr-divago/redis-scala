package codecrafters_redis

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.{ConcurrentHashMap}

object EventLoop {
  private val taskQueue: TaskQueue = TaskQueue()
  private val clientBuffers = new ConcurrentHashMap[SocketChannel, StringBuilder]()

  def start(): Unit = {
    val serverSocket = ServerSocketChannel.open()
    val selector = Selector.open()
    serverSocket.configureBlocking(false)
    serverSocket.bind(new InetSocketAddress("localhost", 6379))
    serverSocket.register(selector, SelectionKey.OP_ACCEPT)
    while (true) {
      if (selector.select() > 0) {
        val keys = selector.selectedKeys()
        val iterator = keys.iterator()
        while (iterator.hasNext) {
          iterator.next() match {
            case key if key.isAcceptable => acceptClient(selector, key)
            case key if key.isReadable => readData(key)
          }
          iterator.remove()
        }
      }
    }
  }

  private def acceptClient(selector: Selector, key: SelectionKey): Unit = {
    val serverChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val client = serverChannel.accept()
    println(s"CONNECT: ${client.socket().getInetAddress}")
    client.configureBlocking(false)
    client.register(selector, SelectionKey.OP_READ)
    taskQueue.addTask(new Task(client.socket(), WaitingForCommand()))
  }

  private def readData(key: SelectionKey): Unit = {
    val buffer = ByteBuffer.allocate(1024)
    val client = key.channel().asInstanceOf[SocketChannel]
    try {
      val bytesRead = client.read(buffer)
      if (bytesRead == -1) {
        client.close()
        key.cancel()
      }
      else if (bytesRead > 0) {
        buffer.flip()
        val data = new String(buffer.array(), 0, buffer.limit())

        val clientBuffer = clientBuffers.computeIfAbsent(client, _ => new StringBuilder())
        clientBuffer.append(data)

        var endLineIndex = clientBuffer.indexOf("\r\n")
        while (endLineIndex != -1) {
          val line = clientBuffer.substring(0, endLineIndex)
          clientBuffer.delete(0, endLineIndex + 2)

          taskQueue.nextTask(client.socket()) match {
            case Some(task) => parseLine(client, line, task)
            case None => println(s"No task found for client ${client.socket().getInetAddress}")
          }
          endLineIndex = clientBuffer.indexOf("\r\n")
        }
      }
    } catch {
      case _: IOException =>
        client.close()
        key.cancel()
    } finally {
      buffer.clear()
    }
  }

  private def parseLine(client: SocketChannel, line: String, task: Task): Unit = {
    taskQueue.removeFirstTask(client.socket())
    ProtocolParser.parse(line, task.currentState) match {
      case Parsed(value, nextState) =>
        value.head match {
          case "PING" => client.write(ByteBuffer.wrap("+PONG\r\n".getBytes))
          case "ECHO" => client.write(ByteBuffer.wrap(("$"+value(1).length+"\r\n"+value(1) + "\r\n").getBytes))
        }
        taskQueue.addTask(new Task(task.socket, nextState))
      case Continue(nextState) => taskQueue.addTask(new Task(task.socket, nextState))
    }
  }
}
