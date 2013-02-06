package no.ks.eventstore2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.eventstore.AcknowledgePreviousEventsProcessed;
import no.ks.eventstore2.eventstore.EventStoreFactory;
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
        EventStoreFactory factory = new EventStoreFactory();
        factory.setDs(db);
        ActorRef eventstore = _system.actorOf(new Props(factory), "eventstore");
        eventstore.tell(new AcknowledgePreviousEventsProcessed(),super.testActor());
        expectMsgClass(Success.class);
    }

    @Test
    public void testPendingSubscriptionsIsFilled() throws Exception {
        EventStoreFactory factory = new EventStoreFactory();
        factory.setDs(db);
        ActorRef eventstore = _system.actorOf(new Props(factory), "eventstore_pendingSubscriptiontest");
        FormParsed event = new FormParsed("formid");
        eventstore.tell(event,super.testActor());
        eventstore.tell(new Subscription(event.getAggregateId()),super.testActor());
        expectMsg(event);
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

}
