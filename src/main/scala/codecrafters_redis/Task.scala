package codecrafters_redis

import java.io.{BufferedReader, InputStreamReader, OutputStream}
import java.net.Socket

class Task(val socket: Socket, val currentState: ParseState) {
  val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
  val out: OutputStream = socket.getOutputStream
}
