package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.formProcessorProject.FormStatuses;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;

import static akka.pattern.Patterns.ask;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static no.ks.eventstore2.projection.CallProjection.call;


public class ProjectionProtobufManagerTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public ProjectionProtobufManagerTest() {
        super(_system);
    }

    @Test
    public void testProjectionManagerCreatesProjections() throws Exception {

        List<Props> factories = new ArrayList<>();
        factories.add(Props.create(FormStatuses.class, super.testActor()));

        final TestActorRef<ProjectionProtobufManager> ref = TestActorRef.create(_system, ProjectionProtobufManager.mkProps(super.testActor(), factories), "projectionProtobufManager");

        Future<Object> getProjectionref = ask(ref, call("getProjectionRef", FormStatuses.class), 3000);

        ActorRef projectionRef = (ActorRef) Await.result(getProjectionref, Duration.create("3 seconds"));

        assertNotNull(projectionRef);
    }

    @Test
    public void testProjectionsInSubscribePhase () throws Exception {

        List<Props> factories = new ArrayList<>();
        factories.add(Props.create(FormStatuses.class, super.testActor()));

        final TestActorRef<ProjectionProtobufManager> actorRef = TestActorRef.create(_system, ProjectionProtobufManager.mkProps(super.testActor(), factories), "projectionProtoManager2");

        completeSubscription(actorRef);

        Future<Object> getProjectionref = ask(actorRef, call("isAnyoneInSubscribePhase", FormStatuses.class), 3000 );
        Boolean isInSubscribePhase = (Boolean) Await.result(getProjectionref, Duration.create("3 seconds"));

        assertTrue(!isInSubscribePhase.booleanValue());

    }

    private void completeSubscription(TestActorRef<ProjectionProtobufManager> actorRef) {
        ActorSelection sel = _system.actorSelection("/user/projectionProtoManager2/FormStatuses");
        sel.tell(new CompleteSubscriptionRegistered("agg"), actorRef);
        final Future<Object> getNumberOfForms = ask(sel, call("getNumberOfForms"), 3000);
        try {
            Await.result(getNumberOfForms, Duration.create("3 seconds"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
