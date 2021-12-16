import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.matchers.should.Matchers
import akka.actor.testkit.typed.scaladsl.FishingOutcomes

import scala.concurrent.duration._

class LamportAlgorithmSpec extends AnyWordSpec with BeforeAndAfterAll with LogCapturing with Matchers {

  val testKit: ActorTestKit = ActorTestKit()
  import testKit._

  override protected def afterAll(): Unit = testKit.shutdownTestKit()

  "Lamport clocks testing" must {

    "confirm booting a processor" in {

      val eventListener = createTestProbe[Processor.Message]()
      val processor0    = spawn(Processor(0, 2, eventListener.ref), "processor-00")
      val fakeProcessor = createTestProbe[Processor.Message]()

      processor0 ! Processor.Boot(Array(processor0, fakeProcessor.ref))
      fakeProcessor.expectMessage(Processor.RegisterProcessor(0, processor0))
      stop(processor0)
    }

    "confirm a processor gets the resource" in {
      val eventListener = createTestProbe[Processor.Message]()
      val processors = Array(
        spawn(Processor(0, 2, eventListener.ref), "processor-01"),
        spawn(Processor(1, 2, eventListener.ref), "processor-02")
      )

      processors.foreach(_ ! Processor.Boot(processors))
      val msgs = eventListener.fishForMessage(5.seconds) {
        case Processor.ResourceUseEvent(_, _, _) =>
          FishingOutcomes.complete

        case _ =>
          FishingOutcomes.continue
      }

      assert(msgs.size == 1)
      processors.foreach(processor => stop(processor.ref))
    }
  }

  "confirm a processor releases the resource after it acquires it" in {
    val eventListener = createTestProbe[Processor.Message]()
    val processors = Array(
      spawn(Processor(0, 2, eventListener.ref), "processor-01"),
      spawn(Processor(1, 2, eventListener.ref), "processor-02")
    )

    processors.foreach(_ ! Processor.Boot(processors))
    var resourceUseEvent: Option[Processor.ResourceUseEvent] = None
    val msgs = eventListener.fishForMessage(5.seconds) {
      case Processor.ResourceUseEvent(id, timestamp, actorRef) =>
        info(s"Processor[$id] is using resource at $timestamp")
        resourceUseEvent = Some(Processor.ResourceUseEvent(id, timestamp, actorRef))
        FishingOutcomes.continue

      case Processor.ReleaseResource(id, timestamp) if resourceUseEvent.isDefined =>
        info(s"Processor[$id] is stopped resource at $timestamp")
        FishingOutcomes.complete

      case _ =>
        FishingOutcomes.continue
    }

    assert(msgs.size == 2)
    processors.foreach(processor => stop(processor.ref))
  }
}
