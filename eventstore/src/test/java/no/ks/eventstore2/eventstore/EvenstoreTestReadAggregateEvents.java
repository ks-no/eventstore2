package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.Inbox;
import akka.testkit.TestActorRef;
import eventstore.Messages;
import no.ks.events.svarut.Test.EventstoreTest;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

class EvenstoreTestReadAggregateEvents extends EventstoreEventstore2TestKit {

    private JournalStorage journal;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        journal = new EventStoreJournalStorage(eventstoreConnection, _system.dispatcher());
    }

    @Test
    void testReadEventsForOneAggregateId() throws TimeoutException {
        String aggregateRootId = UUID.randomUUID().toString();
        for(int i = 0; i < 3; i++) {
            journal.saveEvent(ProtobufHelper.newEventWrapper("Test", aggregateRootId,
                    EventstoreTest.TestEvent.newBuilder().setMessage(UUID.randomUUID().toString()).build()));
        }

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(super.testActor(), journal));
        Inbox inbox = Inbox.create(_system);

        actorTestActorRef.tell(
                Messages.RetreiveAggregateEvents.newBuilder()
                        .setAggregateType("no.ks.events.svarut.Test")
                        .setAggregateRootId(aggregateRootId)
                        .build(),
                inbox.getRef());

        Messages.EventWrapperBatch receive = (Messages.EventWrapperBatch) inbox.receive(Duration.create(3, TimeUnit.SECONDS));
        assertThat(aggregateRootId, is(receive.getAggregateRootId()));
        assertThat(3, is(receive.getEventsCount()));
    }

    @Test
    void testReadEventsForOneAggregateIdAndContinueWhenBatchIsFull() throws Exception {
        EventStoreJournalStorage journal = new EventStoreJournalStorage(eventstoreConnection, _system.dispatcher(), 10);

        String aggregateRootId = UUID.randomUUID().toString();
        for(int i = 0; i < 12; i++) {
            this.journal.saveEvent(ProtobufHelper.newEventWrapper("Test", aggregateRootId,
                    EventstoreTest.TestEvent.newBuilder().setMessage(aggregateRootId + "-" + i).build()));
        }

        TestActorRef<Actor> actorTestActorRef = TestActorRef.create(_system, EventStore.mkProps(super.testActor(), journal));
        Inbox inbox = Inbox.create(_system);

        actorTestActorRef.tell(
                Messages.RetreiveAggregateEvents.newBuilder()
                        .setAggregateType("no.ks.events.svarut.Test")
                        .setAggregateRootId(aggregateRootId)
                        .build(),
                inbox.getRef());

        Messages.EventWrapperBatch receive = (Messages.EventWrapperBatch) inbox.receive(Duration.create(3, TimeUnit.SECONDS));
        assertThat(aggregateRootId, is(receive.getAggregateRootId()));
        assertThat(10, is(receive.getEventsCount()));
        assertThat(receive.getEvents(0).getEvent().unpack(EventstoreTest.TestEvent.class).getMessage(), is(aggregateRootId + "-0"));
        assertThat(receive.getEvents(9).getEvent().unpack(EventstoreTest.TestEvent.class).getMessage(), is(aggregateRootId + "-9"));

        actorTestActorRef.tell(
                Messages.RetreiveAggregateEvents.newBuilder()
                        .setAggregateType("no.ks.events.svarut.Test")
                        .setAggregateRootId(aggregateRootId)
                        .setFromJournalId(10)
                        .build(),
                inbox.getRef());

        receive = (Messages.EventWrapperBatch) inbox.receive(Duration.create(3, TimeUnit.SECONDS));
        assertThat(aggregateRootId, is(receive.getAggregateRootId()));
        assertThat(2, is(receive.getEventsCount()));
        assertThat(receive.getEvents(0).getEvent().unpack(EventstoreTest.TestEvent.class).getMessage(), is(aggregateRootId + "-10"));
        assertThat(receive.getEvents(1).getEvent().unpack(EventstoreTest.TestEvent.class).getMessage(), is(aggregateRootId + "-11"));
    }
}
