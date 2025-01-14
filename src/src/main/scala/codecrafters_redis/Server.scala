package codecrafters_redis

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetSocketAddress, ServerSocket}

object Server {
  def main(args: Array[String]): Unit = {
    println("REDIS CLONE STARTING!")
    EventLoop.start()
  }
}
