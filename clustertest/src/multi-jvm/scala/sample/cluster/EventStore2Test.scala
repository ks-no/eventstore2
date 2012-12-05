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
import akka.actor.{Address, Actor, Props}
import org.springframework.jdbc.datasource.embedded.{EmbeddedDatabaseType, EmbeddedDatabaseBuilder}
import sample.cluster.{Database, Increase, TestProjection}
import actors.ActorRef
import akka.util.Timeout
import concurrent.Await
import akka.routing.RoundRobinRouter

object NodeLeavingAndExitingAndBeingRemovedMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet).withFallback(ConfigFactory.parseString("akka.cluster.auto-join = off")))
}

class Eventstore2MultiJvmNode1 extends EventStore2Test
class Eventstore2MultiJvmNode2 extends EventStore2Test
class Eventstore2MultiJvmNode3 extends EventStore2Test

abstract class EventStore2Test
  extends MultiNodeSpec(NodeLeavingAndExitingAndBeingRemovedMultiJvmSpec)
  with MultiNodeClusterSpec {

  import NodeLeavingAndExitingAndBeingRemovedMultiJvmSpec._

  val reaperWaitingTime = 30.seconds.dilated

  def seedNodes: IndexedSeq[Address] = IndexedSeq(first,second)

  override def afterAll {
    Database.cleanDatabase
  }

  override def beforeAll{
    runOn(first){
      Database.setupDatabase(Database.datasource)
    }
  }

  def startEventStore = {
    val eventStoreFactory: EventStoreFactory = new EventStoreFactory()
    eventStoreFactory.setDs(Database.datasource);
    system.actorOf(Props(eventStoreFactory), name = "eventStore").path.toString;
  }

  "testing eventstore2" must {

    "cluster start up correctly" in {
      enterBarrier("before cluster start")
      runOn(first, second){
        cluster.joinSeedNodes(seedNodes)
        awaitUpConvergence(2)
      }
      enterBarrier("cluster started")
    }
    "send event to projection on other node" in {
      var eventStore = "";
      runOn(first,second){
        eventStore= startEventStore;
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

    }

    def seedNodes: IndexedSeq[Address] = IndexedSeq(first)


    "a node leave and reconnect" in {
      enterBarrier("start")
      runOn(first) {
        cluster.leave(second)
      }
      enterBarrier("second-left")

      runOn(first) {
        // verify that the 'second' node is no longer part of the 'members' set
        awaitCond(clusterView.members.forall(_.address != address(second)), reaperWaitingTime)
        // verify that the 'second' node is not part of the 'unreachable' set
        awaitCond(clusterView.unreachableMembers.forall(_.address != address(second)), reaperWaitingTime)
      }

      runOn(second) {
        // verify that the second node is shut down and has status REMOVED
        awaitCond(!cluster.isRunning, reaperWaitingTime)
        awaitCond(clusterView.status == MemberStatus.Removed, reaperWaitingTime)
      }
      enterBarrier("Node shut down")
      runOn(third) {
        cluster.joinSeedNodes(seedNodes);
      }
      runOn(first, third){
        awaitUpConvergence(2)
      }
      runOn(third){
          startEventStore
      }
      runOn(third){
        system.actorOf(Props[TestProjection],"testProjection");
      }
      runOn(third){
        import no.ks.eventstore2.projection.CallProjection.call
        import akka.pattern.{ ask, pipe}
        var result = 0;
        val startTime = java.lang.System.currentTimeMillis();
        var endTime = startTime
        while(result == 0 && endTime < startTime + 1000*3){
          Thread.sleep(100);
          val future = system.actorFor(node(third) / "user" / "testProjection").ask(call("getCount"))(5 seconds)
          result = Await.result(future, 5.seconds).asInstanceOf[Integer]
          endTime = java.lang.System.currentTimeMillis();
        }
        System.out.println("Result is " + result)
        assert(result == 1)
      }
      enterBarrier("finished2")
    }
  }
}
