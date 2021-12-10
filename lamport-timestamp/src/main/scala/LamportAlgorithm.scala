import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.Breaks

//noinspection ScalaFileName
object Processor {

  sealed trait Message

  final case class RequestResource(id: Int, timestamp: Long)                   extends Message
  final case class RequestResourceAcked(id: Int, timestamp: Long)              extends Message
  final case class ReleaseResource(id: Int, timestamp: Long)                   extends Message
  final case class Boot(processors: Array[ActorRef[Message]])                  extends Message
  final case class ClockTick()                                                 extends Message
  final case class Registered(id: Int, processor: ActorRef[Processor.Message]) extends Message
  final case class RegisterProcessor(id: Int, processor: ActorRef[Processor.Message])
      extends Message
  final case class ResourceUseEvent(
    inUse: Boolean,
    id: Int,
    timestamp: Long,
    proessor: ActorRef[Processor.Message]
  ) extends Message

  class Clock extends AtomicLong(0L) {
    val breaks = new Breaks
    import breaks.{ breakable, break }

    def setIfGreater(update: Long): Unit = {
      breakable {
        val cur = get()

        if (update > cur) {
          if (compareAndSet(cur, update))
            break()
        } else
          break()
      }
    }
  }

  implicit class PriorityQueueExt(queue: mutable.PriorityQueue[RequestResource]) {
    // Painful but true, to remove all the items that are not Processor[id]
    // we have to dequeue all the items, keeping the ones that are not
    // Processor[id] and then re-add them. <sigh>
    def removeAll(id: Int): Unit =
      queue.addAll(queue.dequeueAll.filterNot(_.id == id))
  }

  class Instance(val id: Int, val numProcessors: Int, val listener: ActorRef[ResourceUseEvent]) {
    // Ordering that will cause the RequestResource to be ordered by its timestamp in ascending fashion
    implicit def ordering: Ordering[RequestResource] = Ordering.by(_.timestamp * -1L)

    val clock: Clock                                  = new Clock
    val isUsingResource                               = new AtomicBoolean(false)
    val queue: mutable.PriorityQueue[RequestResource] = mutable.PriorityQueue.empty
    val otherProcessors: mutable.HashMap[Int, ActorRef[Message]] =
      mutable.HashMap.empty[Int, ActorRef[Message]]

    def isResourceAvailable: Boolean = {
      val queueSnapshot = queue.clone().dequeueAll
      queueSnapshot.nonEmpty && queueSnapshot.head.id == id &&
      queueSnapshot.filterNot(_.id == id).size == numProcessors - 1
    }

    override def toString: String =
      s"Processor[$id]"
  }

  def apply(id: Int, numProcessors: Int, listener: ActorRef[ResourceUseEvent]): Behavior[Message] =
    Behaviors.withStash(10)(preBoot(new Instance(id, numProcessors, listener), _))

  private def preBoot(processor: Instance, buffer: StashBuffer[Message]) =
    Behaviors.receive[Message] { case (context, message) =>
      message match {
        case Boot(processors) =>
          registerMe(processor.id, context.self, processors)
          boot(processor, buffer)

        case other =>
          buffer.stash(other)
          Behaviors.same
      }
    }

  private def boot(processor: Instance, buffer: StashBuffer[Message]): Behavior[Message] =
    Behaviors.setup { context =>
      context.log.info(s"$processor booting up...")

      Behaviors.receiveMessage {
        case RegisterProcessor(id, otherProcessor) =>
          processor.otherProcessors += id -> otherProcessor
          context.log.info(
            s"$processor saw another processor: ${processor.otherProcessors.values.mkString(",")}"
          )

          if (processor.otherProcessors.size == processor.numProcessors - 1) {
            buffer.unstashAll(running(processor))
          } else {
            Behaviors.same
          }

        case other =>
          buffer.stash(other)
          Behaviors.same

      }
    }

  private def running(processor: Instance): Behavior[Message] =
    Behaviors.withTimers { timers =>
      Behaviors.setup { context =>
        context.log.info(s"$processor online...")
        timers.startTimerAtFixedRate(ClockTick(), 1 second)
        Behaviors.receiveMessagePartial {
          case ClockTick() =>
            if (processor.isResourceAvailable) {
              context.log.info(
                s"Processor[${processor.id}] using resource at ${processor.clock.get}"
              )
              processor.listener ! ResourceUseEvent(
                inUse = true,
                processor.id,
                processor.clock.get,
                context.self
              )
              context.self ! ReleaseResource(processor.id, processor.clock.addAndGet(1L))
            } else {
              processor.clock.addAndGet(1L)
              processor.otherProcessors.values.foreach(
                _ ! RequestResource(processor.id, processor.clock.get())
              )
            }
            Behaviors.same

          case RequestResource(id, timestamp) =>
            context.log.info(s"Processor[$id] requested access to resource at $timestamp")
            processor.clock.setIfGreater(timestamp + 1)
            val msg = RequestResource(id, timestamp)
            processor.queue.enqueue(msg)
            processor.otherProcessors(id) ! RequestResourceAcked(id, processor.clock.get)
            Behaviors.same

          case RequestResourceAcked(id, timestamp) =>
            context.log.info(s"Processor[$id] ack'ed resource request at $timestamp")
            processor.clock.setIfGreater(timestamp + 1)
            Behaviors.same

          case ReleaseResource(id, timestamp) =>
            processor.clock.setIfGreater(timestamp + 1)
            context.log.info(s"$processor releasing access to resource at ${processor.clock.get}")
            processor.queue.removeAll(id)
            Behaviors.same

        }
      }
    }

  private def registerMe(
    id: Int,
    self: ActorRef[Message],
    processors: Array[ActorRef[Message]]
  ): Unit =
    (processors.slice(0, id) ++ processors.slice(id + 1, processors.length))
      .foreach(processor => processor ! RegisterProcessor(id, self))
}
