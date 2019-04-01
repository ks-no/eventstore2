package no.ks.eventstore2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import com.esotericsoftware.kryo.Kryo;
import com.typesafe.config.ConfigFactory;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.events.Event1;
import no.ks.eventstore2.events.Event4;
import no.ks.eventstore2.events.NewEvent;
import no.ks.eventstore2.events.OldEvent;
import no.ks.eventstore2.eventstore.*;
import no.ks.eventstore2.response.Success;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import static org.junit.jupiter.api.Assertions.fail;

public class EventStoreTest extends TestKit {

    private static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory.load().getConfig("TestSys"));

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(EventStoreForm.FormParsed.class, 1001);
            kryo.register(OldEvent.class, 1002);
            kryo.register(NewEvent.class, 1003);
            kryo.register(Event1.class, 1004);
            kryo.register(Event4.class, 1005);
        }
    };

    private final EmbeddedDatabase db;

    public EventStoreTest() {
        super(_system);
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build();
    }

    @Test
    public void testAcknowledgeRespondsCorrectly() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db, kryoClassRegistration)), "eventstore");
        eventstore.tell(new AcknowledgePreviousEventsProcessed(),super.testActor());
        expectMsgClass(Success.class);
    }

    @Test
    public void testPendingSubscriptionsIsFilled() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db, kryoClassRegistration)), "eventstore_pendingSubscriptiontest");
        fail();
//        FormParsed event = new FormParsed("formid");
//        eventstore.tell(event,super.testActor());
//        eventstore.tell(new Subscription(event.getAggregateType()),super.testActor());
//        expectMsg(event);
    }


    @Test
    public void testEventsAreUpgraded() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db, kryoClassRegistration)), "eventstore_upgradEvent");
        eventstore.tell(new OldEvent(),super.testActor());
        eventstore.tell(new Subscription(new NewEvent().getAggregateType()),super.testActor());
        expectMsgClass(NewEvent.class);
    }

    @Test
    public void testEventsAreUpgradedMultipleTimes() throws Exception {
        ActorRef eventstore = _system.actorOf(EventStore.mkProps(new H2JournalStorage(db, kryoClassRegistration)), "eventstore_upgradEventMultipleTimes");
        eventstore.tell(new Event1(),super.testActor());
        eventstore.tell(new Subscription(new Event4().getAggregateType()),super.testActor());
        expectMsgClass(Event4.class);
    }

    @AfterEach
    public void tearDown() throws Exception {
        db.shutdown();
    }

}
