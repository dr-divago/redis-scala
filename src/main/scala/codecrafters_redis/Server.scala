package codecrafters_redis

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.EventLoop

import java.io.PrintWriter
import java.net.Socket

object Server {
  def main(args: Array[String]): Unit = {
    println("REDIS CLONE STARTING!")
    val config = Config.fromArgs(args)
    val context = Context(config)
    if (config.replicaof.nonEmpty) {
      init_replication(config)
    }
    val eventLoop = new EventLoop(context)
    eventLoop.start()
  }

  private def init_replication(config: Config): Unit = {

    val master_ip_port = config.replicaof.split(" ")
    val master_socket = new Socket("localhost", master_ip_port(1).toInt)
    val out = new PrintWriter(master_socket.getOutputStream)

    out.write("*1\r\n$4\r\nPING\r\n")
    out.flush()
    out.close()
    master_socket.close()
    println("Sent PING to master")
  }
}
