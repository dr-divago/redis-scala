package codecrafters_redis

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

class Task(val socket: Socket, val callBack : String => String) {
  val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
  val out = socket.getOutputStream
}
