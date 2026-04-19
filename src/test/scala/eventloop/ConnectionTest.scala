package eventloop

import codecrafters_redis.command.{Ping, Set => RedisSet}
import codecrafters_redis.eventloop.Connection
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}

class ConnectionSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private def mkConnection(): Connection = {
    val socketChannel = mock[SocketChannel]
    val selectionKey  = mock[SelectionKey]
    Connection(socketChannel, selectionKey)
  }

  "Connection.process" should "parse a PING command" in {
    val connection = mkConnection()
    val commands   = connection.process("*1\r\n$4\r\nPING\r\n".getBytes)

    commands should not be empty
    commands.head shouldBe Ping
  }

  it should "return empty list for empty input" in {
    val connection = mkConnection()
    connection.process(Array.empty[Byte]) shouldBe empty
  }

  it should "parse multiple SET commands in one buffer" in {
    val connection = mkConnection()
    val input      = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n" +
                     "*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n" +
                     "*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
    val commands   = connection.process(input.getBytes)

    commands should have length 3

    val s1 = commands(0).asInstanceOf[RedisSet]
    s1.key   shouldBe "foo"
    s1.value shouldBe "123"

    val s2 = commands(1).asInstanceOf[RedisSet]
    s2.key   shouldBe "bar"
    s2.value shouldBe "456"

    val s3 = commands(2).asInstanceOf[RedisSet]
    s3.key   shouldBe "baz"
    s3.value shouldBe "789"
  }

  it should "return UnknownCommand for an unrecognised command" in {
    val connection = mkConnection()
    val commands   = connection.process("*1\r\n$7\r\nUNKNOWN\r\n".getBytes)

    commands should have length 1
    val response = new String(commands.head.execute(null))
    response should startWith("-ERR unknown command")
  }

  "Connection.write" should "delegate to the underlying SocketChannel" in {
    val socketChannel = mock[SocketChannel]
    val selectionKey  = mock[SelectionKey]
    val connection    = Connection(socketChannel, selectionKey)

    val testData = "+PONG\r\n".getBytes
    when(socketChannel.write(any[ByteBuffer])).thenReturn(testData.length)

    val bytesWritten = connection.write(testData)

    verify(socketChannel).write(any[ByteBuffer])
    bytesWritten shouldEqual testData.length
  }
}
