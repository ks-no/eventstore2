package no.ks.eventstore2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.events.Event1;
import no.ks.eventstore2.events.Event4;
import no.ks.eventstore2.events.NewEvent;
import no.ks.eventstore2.events.OldEvent;
import no.ks.eventstore2.eventstore.AcknowledgePreviousEventsProcessed;
import no.ks.eventstore2.eventstore.EventStore;
import no.ks.eventstore2.eventstore.H2JournalStorage;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.response.Success;
import org.junit.After;
import org.junit.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

public class EventStoreTest extends TestKit {
    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));
    private final EmbeddedDatabase db;


    public EventStoreTest() {
        super(_system);
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build();
    }

    @Test
    public void testAcknowledgeRespondsCorrectly() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db)), "eventstore");
        eventstore.tell(new AcknowledgePreviousEventsProcessed(),super.testActor());
        expectMsgClass(Success.class);
    }

    @Test
    public void testPendingSubscriptionsIsFilled() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db)), "eventstore_pendingSubscriptiontest");
        FormParsed event = new FormParsed("formid");
        eventstore.tell(event,super.testActor());
        eventstore.tell(new Subscription(event.getAggregateType()),super.testActor());
        expectMsg(event);
    }

    @Test
    public void testEventsAreUpgraded() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db)), "eventstore_upgradEvent");
        eventstore.tell(new OldEvent(),super.testActor());
        eventstore.tell(new Subscription(new NewEvent().getAggregateType()),super.testActor());
        expectMsgClass(NewEvent.class);
    }

    @Test
    public void testEventsAreUpgradedMultipleTimes() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db)), "eventstore_upgradEventMultipleTimes");
        eventstore.tell(new Event1(),super.testActor());
        eventstore.tell(new Subscription(new Event4().getAggregateType()),super.testActor());
        expectMsgClass(Event4.class);
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

}
