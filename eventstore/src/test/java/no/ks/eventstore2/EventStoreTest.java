package no.ks.eventstore2;


import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import eventstore.Messages;
import no.ks.eventstore2.eventstore.EventStore;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.Test;

class EventStoreTest extends EventstoreEventstore2TestKit {

    @Test
    void testAcknowledgeIsSentToProjectionManager() {
        TestProbe projectionManagerProbe = new TestProbe(_system);
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(projectionManagerProbe.ref(), journal), "eventstore");
        eventstore.tell(Messages.AcknowledgePreviousEventsProcessed.getDefaultInstance(), null);
        projectionManagerProbe.expectMsgClass(Messages.AcknowledgePreviousEventsProcessed.class);
    }
}
