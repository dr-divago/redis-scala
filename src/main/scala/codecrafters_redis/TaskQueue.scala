package codecrafters_redis

import scala.collection.immutable.Queue

case class TaskQueue() {
  val taskQueue : Queue[Task]  = Queue[Task]

  def addTask(task : Task) = taskQueue.appended(task)
}
