package codecrafters_redis.db

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.concurrent.TrieMap

sealed trait Expiration
case class ExpiresAt(time : Long) extends Expiration
case class ExpiresIn(time : Long) extends Expiration
case class NeverExpires() extends Expiration


class MemoryDB {
  private val db = new TrieMap[String, String]()
  private val scheduler = Executors.newScheduledThreadPool(1)

  def add(key: String, value: String, expiration: Expiration): MemoryDB = {
    db.put(key, value)
    expiration match {
      case ExpiresIn(time) =>
        scheduler.schedule(new Runnable {
            override def run(): Unit = {
              db.remove(key)
            }
          }, time, TimeUnit.MILLISECONDS)
      case ExpiresAt(time) =>
        val toDate = convertToDate(time)
        if (toDate.isBefore(LocalDateTime.now())) {
          db.remove(key)
        } else {
          val diff = ChronoUnit.MILLIS.between(LocalDateTime.now(), convertToDate(time))
          scheduler.schedule(new Runnable {
            override def run(): Unit = {
              db.remove(key)
            }
          }, diff, TimeUnit.MILLISECONDS)
        }
      case NeverExpires() =>
    }
    this
  }

  private def convertToDate(time: Long) = {
    val instant = Instant.ofEpochMilli(time)
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
  }

  def get(key: String): Option[String] = db.get(key)

  def keys() = db.keys

  def shutdown(): Unit = {
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        scheduler.shutdownNow()
    }
  }
}
