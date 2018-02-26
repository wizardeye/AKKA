import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import org.scalatest.{MustMatchers, WordSpecLike}

class LifeTest extends TestKit(ActorSystem("testSystem"))
  with WordSpecLike
  with MustMatchers
  with StopSystemAfterAll
{
  "foo" must {
    val testActorRef = system.actorOf(Props[LifeCycleHooks], "LifeCycleHooks")

    "bar" in {
      testActorRef ! "restart"
      testActorRef.tell("msg", testActor)
      expectMsg("msg")
      system.stop(testActorRef)
      Thread.sleep(1000)
    }
  }
}
