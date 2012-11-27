package no.ks.eventstore2.eventstore.projection;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import akka.util.Timeout;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.testImplementations.EventA1;
import no.ks.eventstore2.eventstore.testImplementations.ProjectionA;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.saga.SagaManager;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import static akka.pattern.Patterns.ask;
import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertEquals;

public class ProjectionTest extends TestKit{

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));
    private SagaInMemoryRepository sagaInMemoryRepository;

    public ProjectionTest() {
        super(_system);
        sagaInMemoryRepository = new SagaInMemoryRepository();
    }

    @Test
    public void testProjectionReceivesMessage() throws Exception {

        final TestActorRef<ProjectionA> ref = TestActorRef.create(_system, new Props(ProjectionA.class), "projection_A");
        ref.tell(new EventA1(), super.testActor());
        Future<Object> getProjectedEvents = ask(ref, "getProjectedEvents", 3000);
        assertEquals(1, ((ImmutableSet<Event>)(Await.result(getProjectedEvents, duration("3 seconds")))).size());

    }
}
