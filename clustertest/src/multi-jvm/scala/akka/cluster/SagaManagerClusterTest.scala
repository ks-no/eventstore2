package akka.cluster

import com.typesafe.config.ConfigFactory
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._
import scala.concurrent.duration._
import no.ks.eventstore2.eventstore.{EventStoreFactory}
import akka.actor.{ActorRef, Actor, Address, Props}
import no.ks.eventstore2.saga.{Saga, SagaDatasourceRepository, SagaInMemoryRepository, SagaManagerFactory}
import no.ks.eventstore2.command.Command
import concurrent.Await
import no.test.IncreaseSaga
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberExited, CurrentClusterState}
import akka.cluster.MemberStatus.Exiting

object SagaManagerClusterSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet).withFallback(ConfigFactory.parseString("akka.cluster.auto-join = off")))
}

class SagaManagerClusterTestMultiJvmNode1 extends SagaManagerClusterTest
class SagaManagerClusterTestMultiJvmNode2 extends SagaManagerClusterTest
class SagaManagerClusterTestMultiJvmNode3 extends SagaManagerClusterTest


abstract class SagaManagerClusterTest
  extends MultiNodeSpec(SagaManagerClusterSpec)
  with MultiNodeClusterSpec {

  import SagaManagerClusterSpec._

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
    system.actorOf(Props(eventStoreFactory), name = "eventstore");
  }

  var eventStore:ActorRef = null;
  val repository: SagaDatasourceRepository = new SagaDatasourceRepository(Database.datasource)

  def startSagaManager(eventStore:ActorRef) {

    val factory: SagaManagerFactory = new SagaManagerFactory(repository, system.actorFor(node(first) / "user" / "dispatcher"), eventStore)

    system.actorOf(Props(factory), "sagaManager")
  }


  var commandDispatcherRef:ActorRef = null;

  "testing sagaManager" must {

    "cluster start up correctly" in {
      enterBarrier("before cluster start")

      runOn(first,second){
        cluster.joinSeedNodes(seedNodes)
        awaitUpConvergence(2)
      }

      enterBarrier("cluster started")
    }

    "sagaDatasource is working correctly" in {
      runOn(first){
        repository.saveState(classOf[Saga],"sagaidtest", 6);
      }
      enterBarrier("saved")
      runOn(second){
        assert(repository.getState(classOf[Saga], "sagaidtest") == 6)
      }
      enterBarrier("finished")
    }

    "SagaManager must start"  in {
      enterBarrier("staring sagamanager")
      runOn(first){
        commandDispatcherRef = system.actorOf(Props[CommandDispatcher],"dispatcher")
        println(commandDispatcherRef.path)
      }
      enterBarrier("commandDispatcherCreated")
        runOn(first,second){
          eventStore = startEventStore
          startSagaManager(eventStore)
        }
      enterBarrier("SystemUp")
      runOn(first,second){
        eventStore ! new Increase
      }
      enterBarrier("Wating to see result")
      runOn(first){
        import akka.pattern.{ ask, pipe}
        var result = 0;
        val startTime = java.lang.System.currentTimeMillis();
        var endTime = startTime
        while(result == 0 && endTime < startTime + 1000*3){
          Thread.sleep(300);
          result = Await.result(ask(system.actorFor(node(first) / "user" / "dispatcher" ), "list")(5 seconds), 5.seconds).asInstanceOf[Int]
          endTime = java.lang.System.currentTimeMillis();
        }
        System.out.println("Result is " + result)
        assert(result == 1)
      }
      enterBarrier("finished")
    }

    "Node 2 die, and node 3 awaken" in {
      enterBarrier("start")
      runOn(first) {
        enterBarrier("registered-listener")
        cluster.leave(second)
      }
      runOn(third){
        enterBarrier("registered-listener")
      }

      runOn(second) {
        // verify that the second node is shut down and has status REMOVED
        val exitingLatch = TestLatch()
        val removedLatch = TestLatch()
        val secondAddress = address(second)
        cluster.subscribe(system.actorOf(Props(new Actor {
          def receive = {
            case state: CurrentClusterState ⇒
              if (state.members.exists(m ⇒ m.address == secondAddress && m.status == Exiting))
                exitingLatch.countDown()
            case MemberExited(m) if m.address == secondAddress ⇒
              exitingLatch.countDown()
            case MemberRemoved(m) if m.address == secondAddress ⇒
              removedLatch.countDown()
            case _ ⇒ // ignore
          }
        })), classOf[MemberEvent])
        enterBarrier("registered-listener")
        exitingLatch.await
        removedLatch.await
      }
      enterBarrier("Node shut down")

      runOn(first) {
        // verify that the 'second' node is no longer part of the 'members' set
        awaitCond(clusterView.members.forall(_.address != address(second)), reaperWaitingTime)
        // verify that the 'second' node is not part of the 'unreachable' set
        awaitCond(clusterView.unreachableMembers.forall(_.address != address(second)), reaperWaitingTime)
      }

      runOn(third) {
        cluster.joinSeedNodes(seedNodes);
      }
      runOn(first, third){
        awaitUpConvergence(2)
      }
      enterBarrier("shutdown and awake done")
    }

    "Node 3 should send another command and the sate should be the same" in {
      enterBarrier("testNewNode")
      runOn(third){
        eventStore = startEventStore
        startSagaManager(eventStore)
      }
      enterBarrier("eventstore started")
      runOn(third) {
        val increase: Increase = new Increase
        eventStore ! new Increase
        eventStore ! new Increase
        increase.setId("id2")
        eventStore ! increase
        eventStore ! increase
      }
      enterBarrier("node3 ready")
      runOn(first){
        import akka.pattern.{ ask, pipe}
        var result = 1;
        val startTime = java.lang.System.currentTimeMillis();
        var endTime = startTime
        while(result == 1 && endTime < startTime + 1000*3){
          Thread.sleep(300);
          result = Await.result(ask(system.actorFor(node(first) / "user" / "dispatcher" ), "list")(5 seconds), 5.seconds).asInstanceOf[Int]
          endTime = java.lang.System.currentTimeMillis();
        }
        System.out.println("Result is " + result)
        assert(result == 2)
      }
    enterBarrier("fisished")
   }

    "Node 3 should fail" in {
      enterBarrier("start")
      runOn(first) {
        enterBarrier("registered-listener")
        cluster.leave(third)
      }

      runOn(second){
        enterBarrier("registered-listener")
      }

      runOn(third) {
        // verify that the second node is shut down and has status REMOVED
        val exitingLatch = TestLatch()
        val removedLatch = TestLatch()
        val secondAddress = address(third)
        cluster.subscribe(system.actorOf(Props(new Actor {
          def receive = {
            case state: CurrentClusterState ⇒
              if (state.members.exists(m ⇒ m.address == secondAddress && m.status == Exiting))
                exitingLatch.countDown()
            case MemberExited(m) if m.address == secondAddress ⇒
              exitingLatch.countDown()
            case MemberRemoved(m) if m.address == secondAddress ⇒
              removedLatch.countDown()
            case _ ⇒ // ignore
          }
        })), classOf[MemberEvent])
        enterBarrier("registered-listener")
        exitingLatch.await
        removedLatch.await
      }
      enterBarrier("Node shut down")

      runOn(first) {
        // verify that the 'second' node is no longer part of the 'members' set
        awaitCond(clusterView.members.forall(_.address != address(second)), reaperWaitingTime)
        // verify that the 'second' node is not part of the 'unreachable' set
        awaitCond(clusterView.unreachableMembers.forall(_.address != address(second)), reaperWaitingTime)
      }

      runOn(third) {
        cluster.joinSeedNodes(seedNodes);
      }
      runOn(first){
        awaitUpConvergence(1)
      }
      enterBarrier("shutdown and awake done")
    }

    "Node 1 should work and have correct state" in {
      enterBarrier("start")
      runOn(first){
        eventStore ! new Increase
        eventStore ! new Increase
        val increase: Increase = new Increase
        increase.setId("id2")
        eventStore ! increase
        eventStore ! increase
        val increase2: Increase = new Increase
        increase2.setId("id3")
        eventStore ! increase2
      }
      enterBarrier("sending event done")
      runOn(first){
        import akka.pattern.{ ask, pipe}
        var result = 2;
        val startTime = java.lang.System.currentTimeMillis();
        var endTime = startTime
        while(result == 2 && endTime < startTime + 1000*3){
          Thread.sleep(300);
          result = Await.result(ask(system.actorFor(node(first) / "user" / "dispatcher" ), "list")(5 seconds), 5.seconds).asInstanceOf[Int]
          endTime = java.lang.System.currentTimeMillis();
        }
        System.out.println("Result is " + result)
        assert(result == 3)
      }
      enterBarrier("fisished")
    }
  }
}