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
    val out = clientSocket.getOutputStream

    in.lines()
      .filter(_ == "PING")
      .forEach(_ => out.write("+PONG\r\n".getBytes()))

    clientSocket.close()
  }
}
