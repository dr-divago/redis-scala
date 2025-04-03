package eventloop

import codecrafters_redis.command.{FullResync, Ping, Set}
import codecrafters_redis.config.Context
import codecrafters_redis.eventloop.Connection
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}


class ConnectionSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "Connection" should "read data from socket channel" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val selectionKey = mock[SelectionKey]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // Set up mock behavior - simulate 10 bytes being read
    when(socketChannel.read(any[ByteBuffer])).thenAnswer { invocation =>
      val buffer = invocation.getArgument[ByteBuffer](0)
      buffer.put("PING\r\n".getBytes)
      6 // Return number of bytes read
    }

    // Call the method under test
    val result = connection.readData(selectionKey)

    // Verify the result
    result should equal("PING\r\n".getBytes)
    verify(socketChannel).read(any[ByteBuffer])
  }

  it should "handle zero bytes read from socket channel" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val selectionKey = mock[SelectionKey]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // Set up mock behavior - simulate 0 bytes being read
    when(socketChannel.read(any[ByteBuffer])).thenReturn(0)

    // Call the method under test
    val result = connection.readData(selectionKey)

    // Verify the result
    result should equal(Array.empty[Byte])
    verify(socketChannel).read(any[ByteBuffer])
  }

  it should "process Redis commands correctly" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // Test with a simple PING command in Redis protocol format
    val commands = connection.process("*1\r\n$4\r\nPING\r\n")

    // Verify commands parsed correctly
    commands should not be empty
    commands.head shouldBe a [Ping.type]
  }

  it should "process protocol handshake correctly" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // Test with a FULLRESYNC command followed by a dimension
    val events = connection.processResponse("+FULLRESYNC 1234abcd 0\r\n")

    // Verify events parsed correctly
    events should not be empty
    events.head shouldBe a[FullResync.type]
  }

  it should "write data to socket channel" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val context = mock[Context]

    // Create connection
    val connection = Connection(socketChannel, context)

    // Set up mock behavior
    val testData = "PONG\r\n".getBytes
    when(socketChannel.write(any[ByteBuffer])).thenReturn(testData.length)

    // Call the method under test
    val bytesWritten = connection.write(testData)

    // Verify the interaction and result
    verify(socketChannel).write(any[ByteBuffer])
    bytesWritten should equal(testData.length)
  }

  it should "accumulate data across multiple reads" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val selectionKey = mock[SelectionKey]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // Set up mock behavior for first read
    when(socketChannel.read(any[ByteBuffer])).thenAnswer { invocation =>
      val buffer = invocation.getArgument[ByteBuffer](0)
      buffer.put("*3\r\n$3\r\n".getBytes)
      8 // Return number of bytes read
    }.thenAnswer { invocation =>
      val buffer = invocation.getArgument[ByteBuffer](0)
      buffer.put("SET\r\n$3\r\nfoo\r\n".getBytes)
      14 // Return number of bytes read
    }.thenAnswer { invocation =>
      val buffer = invocation.getArgument[ByteBuffer](0)
      buffer.put("$1\r\n1\r\n".getBytes)
      7
    }

    // First read
    val result1 = connection.readData(selectionKey)
    result1 should equal("*3\r\n$3\r\n".getBytes)

    // Second read
    val result2 = connection.readData(selectionKey)
    result2 should equal("SET\r\n$3\r\nfoo\r\n".getBytes)

    val result3 = connection.readData(selectionKey)
    result3 should equal("$1\r\n1\r\n".getBytes)

    val res = new String(result1) + new String(result2) + new String(result3)

    // Process the complete command
    val commands = connection.process(res)

    // Verify commands parsed correctly
    commands should not be empty
    commands.head shouldBe a[Set]

    val setCommand = commands.head.asInstanceOf[Set]
    setCommand.key should be ("foo")
    setCommand.value should be ("1")
    setCommand.expiry shouldBe None
  }

  it should "handle large data that exceeds buffer size" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val selectionKey = mock[SelectionKey]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // Create data larger than the 1024 buffer
    val largeData = "*1\r\n$1024\r\n" + "a" * 1024 + "\r\n"

    // Set up mock behavior for multiple reads
    val mockedBehavior = Iterator(
      (largeData.substring(0, 512).getBytes, 512),
      (largeData.substring(512, 1024).getBytes, 512),
      (largeData.substring(1024).getBytes, largeData.length - 1024)
    )

    when(socketChannel.read(any[ByteBuffer])).thenAnswer { invocation =>
      if (mockedBehavior.hasNext) {
        val (bytes, count) = mockedBehavior.next()
        val buffer = invocation.getArgument[ByteBuffer](0)
        buffer.put(bytes)
        count
      } else {
        0
      }
    }

    // Perform multiple reads
    val chunks = Seq(
      connection.readData(selectionKey),
      connection.readData(selectionKey),
      connection.readData(selectionKey)
    )

    // Combine and process
    val combinedData = new String(chunks.flatMap(_.toSeq).toArray)
    val commands = connection.process(combinedData)

    // Verify commands parsed correctly
    commands shouldBe empty
    //commands.head.args.head.length should equal(1024)
  }

  it should "maintain parsing state between process calls" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // First part of a command
    val commands1 = connection.process("*3\r\n$3\r\n")
    commands1 should be(empty) // Should not have complete command yet

    // Second part
    val commands2 = connection.process("SET\r\n$3\r\n")

    val commands3 = connection.process("foo\r\n$1\r\n1\r\n")

    // Verify commands parsed correctly after getting all parts
    commands3 should not be empty
    commands3.head shouldBe a[Set]

    val setCommand = commands3.head.asInstanceOf[Set]
    setCommand.key should be ("foo")
    setCommand.value should be ("1")
    setCommand.expiry shouldBe None
  }

  it should "parse multiple command in the buffer" in {
    // Mock dependencies
    val socketChannel = mock[SocketChannel]
    val context = mock[Context]

    // Create connection with mocked dependencies
    val connection = Connection(socketChannel, context)

    // First part of a command
    val commands1 = connection.process("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n")
    commands1 should not be empty
    commands1.head shouldBe a[Set]

    val setCommand1 = commands1.head.asInstanceOf[Set]
    setCommand1.key should be ("foo")
    setCommand1.value should be ("123")
    setCommand1.expiry shouldBe None

    val setCommand2 = commands1(1).asInstanceOf[Set]
    setCommand2.key should be ("bar")
    setCommand2.value should be ("456")
    setCommand2.expiry shouldBe None

    val setCommand3 = commands1(2).asInstanceOf[Set]
    setCommand3.key should be ("baz")
    setCommand3.value should be ("789")
    setCommand3.expiry shouldBe None
  }
}

