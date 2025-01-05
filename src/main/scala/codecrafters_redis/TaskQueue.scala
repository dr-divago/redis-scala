package codecrafters_redis

import scala.collection.immutable.Queue

case class TaskQueue() {
  var taskQueue: Queue[Task] = Queue[Task]()
  def addTask(task : Task): Queue[Task] = {
    taskQueue = taskQueue.enqueue(task)
    taskQueue
  }
  def nextTask() : Task = {
    val (task, remainingQueue) = taskQueue.dequeue
    taskQueue = remainingQueue
    task
  }
  def isNonEmpty : Boolean = taskQueue.nonEmpty
}
