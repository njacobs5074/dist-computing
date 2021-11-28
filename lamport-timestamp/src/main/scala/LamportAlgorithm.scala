import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.Breaks

object Network {
  import Processor.{ Message => ProcessorMessage }

  sealed trait Message
  final case class RegisterProcessor(id: Int, source: ActorRef[ProcessorMessage])   extends Message
  final case class UnregisterProcessor(id: Int, source: ActorRef[ProcessorMessage]) extends Message
  final case class RequestResource(id: Int, timestamp: Long)                        extends Message
  final case class ReleaseResource(id: Int, timestamp: Long)                        extends Message
  final case class RequestResourceAcked(sourceId: Int, targetId: Int, timestamp: Long)
      extends Message

  def apply(): Behavior[Message] =
    network(Map.empty)

  private def network(processors: Map[Int, ActorRef[ProcessorMessage]]): Behavior[Message] =
    Behaviors.setup { context =>
      context.log.info(
        s"Network running: active processors = [${processors.keySet.toList.sorted.mkString(",")}]"
      )
      Behaviors.receiveMessagePartial {
        case RegisterProcessor(id, processor) =>
          processor ! Processor.Registered()
          network(processors + (id -> processor))

        case UnregisterProcessor(id, processor) =>
          processor ! Processor.Unregistered()
          network(processors - id)

        case RequestResource(requestorId, timestamp) =>
          processors.foreach { case (id, processor) =>
            if (id != requestorId) {
              context.log
                .info(s"Sending resource request to Processor[$id] from Processor[$requestorId]")
              processor ! Processor.RequestResource(requestorId, timestamp)
            }
          }
          Behaviors.same

        case ReleaseResource(requestorId, timestamp) =>
          processors.foreach { case (id, processor) =>
            if (id != requestorId) {
              context.log.info(
                s"Sending resource release to Processor[$id] from Processor[$requestorId]"
              )
              processor ! Processor.ReleaseResource(requestorId, timestamp)
            }
          }
          Behaviors.same

        case RequestResourceAcked(sourceId, targetId, timestamp) =>
          processors.get(targetId).foreach { processor =>
            context.log.info(
              s"Sending resource request ack to Processor[$targetId] from Processor[$sourceId]"
            )
            processor ! Processor.RequestResourceAcked(sourceId, timestamp)
          }
          Behaviors.same

      }
    }
}

object Processor {

  sealed trait Message
  final case class RequestResource(id: Int, timestamp: Long)      extends Message
  final case class RequestResourceAcked(id: Int, timestamp: Long) extends Message
  final case class ReleaseResource(id: Int, timestamp: Long)      extends Message

  final case class Boot()         extends Message
  final case class StartWork()    extends Message
  final case class StopWork()     extends Message
  final case class Shutdown()     extends Message
  final case class Registered()   extends Message
  final case class Unregistered() extends Message
  final case class ClockTick()    extends Message

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

  class Instance(val id: Int, val numProcessors: Int) {
    // Ordering that will cause the RequestResource to be ordered by its timestamp in ascending fashion
    implicit def ordering: Ordering[RequestResource] = Ordering.by(_.timestamp * -1L)

    val clock: Clock                                  = new Clock
    val useClock                                      = false
    val queue: mutable.PriorityQueue[RequestResource] = mutable.PriorityQueue.empty

    def isResourceAvailable: Boolean =
      !queue.isEmpty && queue.head.id == id && queue.filterNot(_.id == id).size == numProcessors - 1
  }

  def apply(id: Int, numProcessors: Int, network: ActorRef[Network.Message]): Behavior[Message] =
    Behaviors.withTimers(timers => boot(timers, new Instance(id, numProcessors), network))

  private def boot(
    timers: TimerScheduler[Message],
    processor: Instance,
    network: ActorRef[Network.Message]
  ): Behavior[Message] =
    Behaviors.setup { context =>
      context.log.info(s"Processor[${processor.id}] booting up...")
      timers.startTimerAtFixedRate(ClockTick(), 1 second)

      Behaviors.receiveMessagePartial {
        case Boot() =>
          context.log.info(s"Processor[${processor.id}] connecting to network...")
          network ! Network.RegisterProcessor(processor.id, context.self)
          Behaviors.same
        case Registered() =>
          context.log.info(s"Processor[${processor.id}] connected to network")
          processor.queue.enqueue(RequestResource(processor.id, 0L))
          running(timers, processor, network)
      }
    }

  private def running(
    timers: TimerScheduler[Message],
    processor: Instance,
    network: ActorRef[Network.Message]
  ): Behavior[Message] =
    Behaviors.setup { context =>
      context.log.info(s"Processor[${processor.id}] online...")
      Behaviors.receiveMessagePartial {
        case ClockTick() =>
          if (processor.isResourceAvailable) {
            context.log.info(s"Processor[${processor.id}] using resource at ${processor.clock.get}")
            context.self ! ReleaseResource(processor.id, processor.clock.addAndGet(1L))
          }
          Behaviors.same

        case StartWork() =>
          context.log.info(s"Processor[${processor.id}] requesting access to resource...")
          val msg = RequestResource(processor.id, processor.clock.addAndGet(1L))
          processor.queue.addOne(msg)
          network ! Network.RequestResource(processor.id, msg.timestamp)
          Behaviors.same

        case RequestResource(id, timestamp) =>
          context.log.info(s"Processor[$id] requested access to resource at $timestamp")
          processor.clock.setIfGreater(timestamp + 1)
          val msg = RequestResource(id, timestamp)
          processor.queue.enqueue(msg)
          network ! Network.RequestResourceAcked(processor.id, id, processor.clock.addAndGet(1L))
          Behaviors.same

        case RequestResourceAcked(id, timestamp) =>
          context.log.info(s"Processor[$id] ack'ed resource request at $timestamp")
          processor.clock.setIfGreater(timestamp + 1)
          Behaviors.same

        case ReleaseResource(id, timestamp) =>
          processor.clock.setIfGreater(timestamp + 1)
          context.log.info(s"Processor[$id] releasing access to resource at ${processor.clock.get}")

          // Painful but true, to remove all the items that are not Processor[id]
          // we have to dequeue all the items, keeping the ones that are not
          // Processor[id] and then re-add them. <sigh>
          processor.queue.addAll(processor.queue.dequeueAll.filterNot(_.id == id))

          Behaviors.same

        case StopWork() =>
          context.log.info(s"Processor ${processor.id} stopping work")
          processor.queue.addAll(processor.queue.dequeueAll.filterNot(_.id == processor.id))
          network ! Network.ReleaseResource(processor.id, processor.clock.addAndGet(1L))
          Behaviors.same

        case Shutdown() =>
          network ! Network.UnregisterProcessor(processor.id, context.self)
          shutdown(timers, processor, network)
      }
    }

  private def shutdown(
    timers: TimerScheduler[Message],
    processor: Instance,
    network: ActorRef[Network.Message]
  ): Behavior[Message] =
    Behaviors.receive { (context, message) =>
      message match {
        case Unregistered() =>
          context.log.info(s"Processor ${processor.id} disconnected from network")
          boot(timers, processor, network)

        case _ =>
          Behaviors.unhandled
      }
    }
}
