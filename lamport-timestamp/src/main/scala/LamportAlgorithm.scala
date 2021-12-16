import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer

import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

//noinspection ScalaFileName
object Processor {

  sealed trait Message

  final case class RequestResource(id: Int, timestamp: Long) extends Message with Ordered[RequestResource] {
    override def compare(that: RequestResource): Int =
      if (this.timestamp == this.timestamp) {
        this.id - that.id
      } else {
        this.timestamp - that.timestamp
      }.toInt * -1
  }

  final case class RequestResourceAcked(id: Int, timestamp: Long)                                     extends Message
  final case class ReleaseResource(id: Int, timestamp: Long)                                          extends Message
  final case class Boot(processors: Array[ActorRef[Message]])                                         extends Message
  final case class ClockTick()                                                                        extends Message
  final case class Registered(id: Int, processor: ActorRef[Processor.Message])                        extends Message
  final case class RegisterProcessor(id: Int, processor: ActorRef[Processor.Message])                 extends Message
  final case class ResourceUseEvent(id: Int, timestamp: Long, processor: ActorRef[Processor.Message]) extends Message

  class Clock extends AtomicLong(0L) {
    def setIfGreater(update: Long): Unit = {
      @tailrec
      def loop(): Unit = {
        val cur = get()
        if (update > cur && !compareAndSet(cur, update)) {
          loop()
        }
      }
      loop()
    }
  }

  implicit class PriorityQueueExt(queue: mutable.PriorityQueue[RequestResource]) {
    // Painful but true, to remove all the items that are not Processor[id]
    // we have to dequeue all the items, keeping the ones that are not
    // Processor[id] and then re-add them. <sigh>
    def removeAll(id: Int): Unit =
      queue.addAll(queue.dequeueAll.filterNot(_.id == id))
  }

  class Instance(val id: Int, val numProcessors: Int, val listener: ActorRef[Message]) {

    val clock: Clock                                             = new Clock
    val queue: mutable.PriorityQueue[RequestResource]            = mutable.PriorityQueue.empty
    val otherProcessors: mutable.HashMap[Int, ActorRef[Message]] = mutable.HashMap.empty[Int, ActorRef[Message]]
    val rand                                                     = new Random()

    // Algorithm step 5 logic for determining if resource is available for use
    def isResourceAvailable: Boolean = {
      val queueSnapshot = queue.clone().dequeueAll

      queueSnapshot.size == numProcessors &&
      queueSnapshot.head.id == id &&
      clock.get() > queueSnapshot.head.timestamp
    }

    def isResourceRequestPending: Boolean =
      queue.clone().dequeueAll.exists(_.id == id)

    override def toString: String =
      Instance.toString(this)
  }

  object Instance {
    def toString(instance: Instance): String =
      s"Processor[${instance.id}]<C=${instance.clock.get()}>"
  }

  def apply(id: Int, numProcessors: Int, listener: ActorRef[Message]): Behavior[Message] =
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
          context.log.info(s"$processor saw another processor: ${processor.otherProcessors.values.mkString(",")}")

          // If we've seen all the other processors, we can now set ourselves into run mode
          // We do this by un-stashing any messages that were sent to us while we were booting
          if (processor.otherProcessors.size == processor.numProcessors - 1) {
            buffer.unstashAll(running(processor))
          } else {
            // Haven't seen all processors, so keep waiting
            Behaviors.same
          }

        // Got a message while we're still waiting to see the rest of the processors
        case other =>
          buffer.stash(other)
          Behaviors.same

      }
    }

  private def running(processor: Instance): Behavior[Message] =
    Behaviors.withTimers { timers =>
      Behaviors.setup { context =>
        context.log.info(s"$processor online...")
        timers.startTimerAtFixedRate(ClockTick(), 2 seconds)

        Behaviors.receiveMessagePartial {
          // Algorithm step 1 - we just periodically ask for the resource
          case ClockTick() =>
            // Algorithm step 5 - We detect that the resource is available by checking our queue

            // IR1. Each process increments its clock in between events
            processor.clock.incrementAndGet()

            context.log.info(s"$processor checking if resource available (${processor.isResourceAvailable})")
            if (processor.isResourceAvailable) {
              context.log.info(s"$processor using resource at ${processor.clock.get}")
              processor.listener ! ResourceUseEvent(processor.id, processor.clock.get, context.self)
              context.self ! ReleaseResource(processor.id, processor.clock.incrementAndGet())
            } else if (!processor.isResourceRequestPending) {
              context.log.info(s"$processor requesting access to resource")
              processor.queue.enqueue(RequestResource(processor.id, processor.clock.get()))
              processor.otherProcessors.values.foreach(_ ! RequestResource(processor.id, processor.clock.get()))
            }
            Behaviors.same

          // Algorithm step 2, part 1 - Put another processor's request onto our queue and ack it
          case RequestResource(id, timestamp) =>
            context.log.info(s"$processor: Processor[$id] requested access to resource at $timestamp")
            processor.clock.setIfGreater(timestamp)
            val msg = RequestResource(id, timestamp)
            processor.queue.enqueue(msg)
            processor.otherProcessors(id) ! RequestResourceAcked(id, processor.clock.get())
            Behaviors.same

          // Algorithm step 2, part 2 - Process their ack to our request
          case RequestResourceAcked(id, timestamp) =>
            context.log.info(s"$processor: Processor[$id] ack'ed resource request at $timestamp")
            processor.clock.setIfGreater(timestamp)
            Behaviors.same

          // When we release the resource, we do it asynchronously by sending ourselves a message
          case ReleaseResource(processor.id, timestamp) =>
            context.log.info(s"$processor: Releasing resource")
            processor.clock.setIfGreater(timestamp)
            processor.queue.removeAll(processor.id)
            processor.otherProcessors.values.foreach(_ ! ReleaseResource(processor.id, processor.clock.get()))
            processor.listener ! ReleaseResource(processor.id, processor.clock.get())
            Behaviors.same

          // Algorithm step 4 - Handle another process' resource release
          case ReleaseResource(id, timestamp) =>
            processor.clock.setIfGreater(timestamp)
            context.log.info(s"$processor: Processor[$id] releasing access to resource at ${processor.clock.get}")
            processor.queue.removeAll(id)
            Behaviors.same

        }
      }
    }

  private def registerMe(id: Int, self: ActorRef[Message], processors: Array[ActorRef[Message]]): Unit =
    (processors.slice(0, id) ++ processors.slice(id + 1, processors.length))
      .foreach(processor => processor ! RegisterProcessor(id, self))
}
