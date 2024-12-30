package codecrafters_redis

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetSocketAddress, ServerSocket}

object Server {
  def main(args: Array[String]): Unit = {
    println("Logs from your program will appear here!")

    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress("localhost", 6379))
    val clientSocket = serverSocket.accept()

    val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))

    while (in.readLine() != null) {
      val out = clientSocket.getOutputStream
      out.write("+PONG\r\n".getBytes)
    }
  }
}
