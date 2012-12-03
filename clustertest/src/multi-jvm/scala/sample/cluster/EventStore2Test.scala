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

      runOn(first){
        val eventStoreFactory: EventStoreFactory = new EventStoreFactory()
        val builder: EmbeddedDatabaseBuilder = new EmbeddedDatabaseBuilder
        val db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build
        eventStoreFactory.setDs(db);
        println(system.actorOf(Props(eventStoreFactory).withRouter(
          ClusterRouterConfig(RoundRobinRouter(1), ClusterRouterSettings(
            totalInstances = 1, routeesPath = "/user/localEventStore",
            allowLocalRoutees = true))),
          name = "eventStore").path.address)
      }
      enterBarrier("afterEeventStore")
      runOn(second){
        system.actorOf(Props[TestProjection],"testProjection");
      }
      enterBarrier("BeforeFirstEvent")
      runOn(first) {
        system.actorFor("eventStore") ! new Increase
      }
      enterBarrier("Count")
      runOn(second){
        import no.ks.eventstore2.projection.CallProjection.call
        import akka.pattern.{ ask, pipe}


        val future = system.actorFor("cluster://user/testProjection").ask(call("getCount"))(5 seconds)
        val result = Await.result(future, 5.seconds).asInstanceOf[Integer]
        assert(result == 0)
      }

      enterBarrier("per")
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
