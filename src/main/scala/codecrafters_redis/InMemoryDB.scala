package codecrafters_redis


import java.util.concurrent.DelayQueue

class InMemoryDB {
  private var db: Map[String, String] = Map.empty
  private val delayQueue = new DelayQueue[DelayKey]()

  def add(key: String, value: String, expiration: Expiration): Unit = {
    val keyForQueue = DelayKey(key, expiration, db)
    delayQueue.put(keyForQueue)
    db = db + (key -> value)
    println(s"Insered ${value} at ${System.currentTimeMillis}")
  }

  def get(key: String) : Option[String] = {
    db.get(key)
  }

  private val consumerThread = new Thread(() => {
    try while (true) {
      val element = delayQueue.take
      println(s"Consumed:  ${element} at ${System.currentTimeMillis}")
      db = db.removed(element.key)
      delayQueue.remove(element)

    }
    catch {
      case _: InterruptedException =>
        Thread.currentThread.interrupt()
    }

  })
  consumerThread.start()
}
