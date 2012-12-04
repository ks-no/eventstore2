/**
 *  Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster

import routing.{ClusterRouterSettings, ClusterRouterConfig}
import scala.collection.immutable.SortedSet
import com.typesafe.config.ConfigFactory
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._
import scala.concurrent.duration._
import no.ks.eventstore2.eventstore.{Subscription, EventStoreFactory}
import akka.actor.{Actor, Props}
import org.springframework.jdbc.datasource.embedded.{EmbeddedDatabaseType, EmbeddedDatabaseBuilder}
import sample.cluster.{Increase, TestProjection}
import actors.ActorRef
import akka.util.Timeout
import concurrent.Await
import akka.routing.RoundRobinRouter

object NodeLeavingAndExitingAndBeingRemovedMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class Eventstore2MultiJvmNode1 extends EventStore2Test
class Eventstore2MultiJvmNode2 extends EventStore2Test

abstract class EventStore2Test
  extends MultiNodeSpec(NodeLeavingAndExitingAndBeingRemovedMultiJvmSpec)
  with MultiNodeClusterSpec {

  import NodeLeavingAndExitingAndBeingRemovedMultiJvmSpec._

  val reaperWaitingTime = 30.seconds.dilated

  "testing eventstore2" must {

    "cluster start up correctly" in {

      awaitClusterUp(first, second)
    }
    "send event to projection on other node" in {
      var eventStore = "";
      runOn(first){
        val eventStoreFactory: EventStoreFactory = new EventStoreFactory()
        val builder: EmbeddedDatabaseBuilder = new EmbeddedDatabaseBuilder
        val db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build
        eventStoreFactory.setDs(db);
        eventStore = system.actorOf(Props(eventStoreFactory), name = "eventStore").path.toString;
      }
      runOn(second){
        val eventStoreFactory: EventStoreFactory = new EventStoreFactory()
        val builder: EmbeddedDatabaseBuilder = new EmbeddedDatabaseBuilder
        val db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build
        eventStoreFactory.setDs(db);
        eventStore = system.actorOf(Props(eventStoreFactory), name = "eventStore").path.toString;
      }
      enterBarrier("afterEeventStore")
      runOn(second){
        system.actorOf(Props[TestProjection],"testProjection");
      }

      enterBarrier("BeforeFirstEvent")
      runOn(first) {
          println(eventStore)
          system.actorFor(eventStore) ! new Increase
      }
      enterBarrier("Count")
      runOn(second){
        import no.ks.eventstore2.projection.CallProjection.call
        import akka.pattern.{ ask, pipe}
        var result = 0;
        val startTime = java.lang.System.currentTimeMillis();
        var endTime = startTime
        while(result == 0 && endTime < startTime + 1000*3){
          Thread.sleep(100);
          val future = system.actorFor(node(second) / "user" / "testProjection").ask(call("getCount"))(5 seconds)
          result = Await.result(future, 5.seconds).asInstanceOf[Integer]
          endTime = java.lang.System.currentTimeMillis();
        }
        System.out.println("Result is " + result)
        assert(result == 1)
      }

      enterBarrier("finished")
      /*runOn(first) {
        cluster.leave(second)
      }*/
      enterBarrier("second-left")

      /*runOn(first) {
        // verify that the 'second' node is no longer part of the 'members' set
        awaitCond(clusterView.members.forall(_.address != address(second)), reaperWaitingTime)

        // verify that the 'second' node is not part of the 'unreachable' set
        awaitCond(clusterView.unreachableMembers.forall(_.address != address(second)), reaperWaitingTime)
      }

      runOn(second) {
        // verify that the second node is shut down and has status REMOVED
        awaitCond(!cluster.isRunning, reaperWaitingTime)
        awaitCond(clusterView.status == MemberStatus.Removed, reaperWaitingTime)
      } */

      enterBarrier("finished")
    }
  }
}
