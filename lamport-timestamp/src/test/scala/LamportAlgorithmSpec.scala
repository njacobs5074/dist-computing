import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.matchers.should.Matchers
import akka.actor.testkit.typed.scaladsl.FishingOutcomes

import scala.concurrent.duration._

class LamportAlgorithmSpec
    extends AnyWordSpec
    with BeforeAndAfterAll
    with LogCapturing
    with Matchers {

  val testKit: ActorTestKit = ActorTestKit()
  import testKit._

  override protected def afterAll(): Unit = testKit.shutdownTestKit()

  "Lamport clocks testing" must {

    "confirm booting a processor" in {

      val eventListener = createTestProbe[Processor.ResourceUseEvent]()
      val processor0    = spawn(Processor(0, 2, eventListener.ref), "processor-0")
      val fakeProcessor = createTestProbe[Processor.Message]()

      processor0 ! Processor.Boot(Array(processor0, fakeProcessor.ref))
      fakeProcessor.expectMessage(Processor.RegisterProcessor(0, processor0))
      stop(processor0)
    }

    "confirm a processor gets the resource" in {
      val eventListener = createTestProbe[Processor.ResourceUseEvent]()
      val processors = Array(
        spawn(Processor(0, 3, eventListener.ref), "processor-0"),
        spawn(Processor(1, 3, eventListener.ref), "processor-1"),
        spawn(Processor(2, 3, eventListener.ref), "processor-2")
      )

      processors.foreach(_ ! Processor.Boot(processors))
      val msgs = eventListener.fishForMessage(5.seconds) {
        case Processor.ResourceUseEvent(inUse, id, timestamp, _) if inUse =>
          info(s"Processor[$id] is using resource at $timestamp")
          FishingOutcomes.complete

        case _ =>
          FishingOutcomes.continue
      }

      assert(msgs.size == 1)
      processors.foreach(processor => stop(processor.ref))
    }
  }
}
