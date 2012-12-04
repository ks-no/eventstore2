package sample.multinode

import akka.testkit.ImplicitSender
import akka.actor.{ActorSystem, Props, Actor}
import akka.remote.testkit.{MultiNodeSpecCallbacks, MultiNodeConfig, MultiNodeSpec}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import sample.multinode.STMultiNodeSpec
import no.ks.eventstore2.eventstore.EventStoreFactory
import org.springframework.jdbc.datasource.embedded.{EmbeddedDatabaseType, EmbeddedDatabaseBuilder}
import akka.cluster.routing.{ClusterRouterSettings, ClusterRouterConfig}
import akka.routing.RoundRobinRouter
import scala.concurrent.duration._
import sample.cluster.{Increase, TestProjection}
import concurrent.Await
import com.typesafe.config.ConfigFactory

class MultiNodeSampleSpecMultiJvmNode1 extends MultiNodeSample
class MultiNodeSampleSpecMultiJvmNode2 extends MultiNodeSample

object MultiNodeSampleConfig extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
}

class MultiNodeSample extends MultiNodeSpec(MultiNodeSampleConfig)
with STMultiNodeSpec with ImplicitSender {

  import MultiNodeSampleConfig._

  def initialParticipants = roles.size

  "A MultiNodeSample" must {

    "wait for all nodes to enter a barrier" in {
      enterBarrier("startup")
    }

   /* "be able to send event from one node to another" in {

      runOn(node1){
        val eventStoreFactory: EventStoreFactory = new EventStoreFactory()
        val builder: EmbeddedDatabaseBuilder = new EmbeddedDatabaseBuilder
        val db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build
        eventStoreFactory.setDs(db);
        eventStoreFactory.addRemoteEventStores(system.actorFor(node(node2) / "user" / "eventStore"));
        println("setup" + system.actorOf(Props(eventStoreFactory),name = "eventStore").path)

        system.actorOf(Props[TestProjection],"testProjection")
      }

      runOn(node2){
        val eventStoreFactory: EventStoreFactory = new EventStoreFactory()
        val builder: EmbeddedDatabaseBuilder = new EmbeddedDatabaseBuilder
        val db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build
        eventStoreFactory.setDs(db);
        eventStoreFactory.addRemoteEventStores(system.actorFor(node(node1) / "user" / "eventStore"));
        println("setup" + system.actorOf(Props(eventStoreFactory),name = "eventStore").path)
      }
      enterBarrier("startup_finished")
      runOn(node2){
        system.actorFor(node(node1) / "user" / "eventStore") ! new Increase
      }

      runOn(node1){
        import no.ks.eventstore2.projection.CallProjection.call
        import akka.pattern.{ ask, pipe}
        var result = 0;
        while(result == 0){
          Thread.sleep(100);
          val future = system.actorFor("akka://MultiNodeSample/user/testProjection").ask(call("getCount"))(5 seconds)
          result = Await.result(future, 5.seconds).asInstanceOf[Integer]

        }
        assert(result == 1)
      }
      enterBarrier("finished")
    }

    "be able to handle node down" in {
       runOn(node2){
         system.shutdown();

         testConductor.removeNode(myself)

       }


    }*/
  }
}

