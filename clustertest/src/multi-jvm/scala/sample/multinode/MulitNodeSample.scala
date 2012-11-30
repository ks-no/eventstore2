package sample.multinode

import akka.testkit.ImplicitSender
import akka.actor.{Props, Actor}
import akka.remote.testkit.MultiNodeSpec

class MultiNodeSampleSpecMultiJvmNode1 extends MultiNodeSample
class MultiNodeSampleSpecMultiJvmNode2 extends MultiNodeSample


class MultiNodeSample extends MultiNodeSpec(MultiNodeSampleConfig)
with STMultiNodeSpec with ImplicitSender {

  import MultiNodeSampleConfig._

  def initialParticipants = roles.size

  "A MultiNodeSample" must {

    "wait for all nodes to enter a barrier" in {
      enterBarrier("startup")
    }

    "send to and receive from a remote node" in {
      runOn(node1) {
        enterBarrier("deployed")
        val ponger = system.actorFor(node(node2) / "user" / "ponger")
        ponger ! "ping"
        expectMsg("pong")
      }

      runOn(node2) {
        system.actorOf(Props(new Actor {
          def receive = {
            case "ping" => sender ! "pong"
          }
        }), "ponger")
        enterBarrier("deployed")
      }

      enterBarrier("finished")
    }
  }
}

