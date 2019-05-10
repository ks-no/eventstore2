package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import eventstore.Messages;
import no.ks.eventstore2.formProcessorProject.FormStatuses;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.Collections;
import java.util.UUID;

import static akka.pattern.Patterns.ask;
import static no.ks.eventstore2.projection.CallProjection.call;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class ProjectionManagerTest extends EventstoreEventstore2TestKit {

    @Test
    void testProjectionManagerCreatesProjections() throws Exception {
        final TestActorRef<ProjectionManager> ref = TestActorRef.create(_system,
                ProjectionManager.mkProps(super.testActor(),
                        Collections.singletonList(Props.create(FormStatuses.class, eventstoreConnection))),
                "projectionManager");

        Future<Object> getProjectionref = ask(ref, call("getProjectionRef", FormStatuses.class), 3000);

        ActorRef projectionRef = (ActorRef) Await.result(getProjectionref, Duration.create("3 seconds"));

        assertNotNull(projectionRef);
    }

    @Test
    void testProjectionsInSubscribePhase() throws Exception {
        final TestActorRef<ProjectionManager> actorRef = TestActorRef.create(_system,
                ProjectionManager.mkProps(super.testActor(),
                        Collections.singletonList(Props.create(FormStatuses.class, eventstoreConnection))),
                "projectionManager2");

        ActorRef projectionRef = (ActorRef) Await.result(ask(actorRef, call("getProjectionRef", FormStatuses.class), 3000), Duration.create("3 seconds"));
        Await.result(ask(projectionRef, call("getNumberOfForms"), 3000), Duration.create("3 seconds"));

        Future<Object> getProjectionref = ask(actorRef, call("isAnyoneInSubscribePhase", FormStatuses.class), 3000 );
        Boolean isInSubscribePhase = (Boolean) Await.result(getProjectionref, Duration.create("3 seconds"));

        assertFalse(isInSubscribePhase);
    }

    @Test
    void testAcknowledgePreviousEventsProcessed() {
        TestProbe testProbe = new TestProbe(_system);
        final TestActorRef<ProjectionManager> actorRef = TestActorRef.create(_system,
                ProjectionManager.mkProps(super.testActor(), Collections.singletonList(Props.create(FormStatuses.class, eventstoreConnection))), UUID.randomUUID().toString());

        actorRef.tell(Messages.AcknowledgePreviousEventsProcessed.getDefaultInstance(), testProbe.ref());
        testProbe.expectMsgClass(Messages.PreviousEventsProcessed.class);
    }
}
