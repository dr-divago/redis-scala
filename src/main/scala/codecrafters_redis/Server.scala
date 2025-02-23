package codecrafters_redis

import codecrafters_redis.config.{Config, Context}
import codecrafters_redis.eventloop.EventLoop

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.Socket
import java.nio.ByteBuffer

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
    val port = config.port
    val out = new PrintWriter(master_socket.getOutputStream)
    val in = master_socket.getInputStream

    val buffer = ByteBuffer.allocate(17)

    out.write("*1\r\n$4\r\nPING\r\n")
    out.flush()
    in.read(buffer.array(), 0, 7)
    val pingResponse = new String(buffer.array(), 0, 7)
    if (!pingResponse.equals("+PONG\r\n")) {
      throw new Exception(s"Expected +PONG but got ${pingResponse} ${pingResponse.length}")
    }

    out.write(s"*3\r\n$$8\r\nREPLCONF\r\n$$14\r\nlistening-port\r\n$$${port.length}\r\n$port\r\n")
    out.flush()
    in.read(buffer.array(), 7, 5)
    val ok1Response = new String(buffer.array(), 7, 5)
    if (ok1Response != "+OK\r\n") {
      throw new Exception(s"Expected +OK but got ${ok1Response} ")
    }

    out.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    out.flush()
    in.read(buffer.array(), 12, 5)
    val ok2Response = new String(buffer.array(), 12, 5)
    if (ok2Response != "+OK\r\n") {
      throw new Exception(s"Expected +OK but go ${ok2Response}")
    }
    println("Sent PING to master")

    out.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
    out.flush()
  }
}
