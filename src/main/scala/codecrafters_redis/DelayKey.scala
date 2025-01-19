package codecrafters_redis

import java.util.concurrent.{Delayed, TimeUnit}

sealed trait Expiration
case class ExpiresAt(time : Long) extends Expiration
case class NeverExpires() extends Expiration

case class DelayKey(key: String, delayForExpiration: Expiration, map: Map[String, String]) extends Delayed {
  private val expirationTime: Long = delayForExpiration match {
    case ExpiresAt(time) => time
    case NeverExpires() => Long.MaxValue
  }

  override def getDelay(timeUnit: TimeUnit): Long = {
    val diff = expirationTime - System.currentTimeMillis
    println(s"exiration time ${expirationTime} diff ${diff}")
    timeUnit.convert(diff, TimeUnit.MILLISECONDS)
  }

  override def compareTo(other: Delayed): Int = other match {
      case o: DelayKey => expirationTime.compareTo(o.expirationTime)
      case _ => 0
  }
}
