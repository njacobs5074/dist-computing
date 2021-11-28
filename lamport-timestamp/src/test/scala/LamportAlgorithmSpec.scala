import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class LamportAlgorithmSpec
    extends AnyWordSpec
    with BeforeAndAfterAll
    with LogCapturing
    with Matchers {

  val testKit = ActorTestKit()
  import testKit._

  override protected def afterAll(): Unit = testKit.shutdownTestKit()

  "Lamport clocks testing" must {

    "confirm booting a processor" in {
      val probe = testKit.createTestProbe[Processor.Message]()

      val network   = spawn(Network(), "network")
      val processor = spawn(Processor(0, 1, network), "processor-0")

      processor ! Processor.Boot()
      probe.expectMessageType[Processor.Registered]

      stop(processor, 5.seconds)
      stop(network)

    }

  }
}
