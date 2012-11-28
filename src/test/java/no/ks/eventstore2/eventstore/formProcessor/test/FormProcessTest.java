package no.ks.eventstore2.eventstore.formProcessor.test;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import no.ks.eventstore2.command.CommandDispatcherFactory;
import no.ks.eventstore2.command.CommandHandlerFactory;
import no.ks.eventstore2.eventstore.EmbeddedDatabaseTest;
import no.ks.eventstore2.eventstore.EventStoreFactory;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.eventstore.formProcessor.*;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.saga.SagaManagerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class FormProcessTest extends EmbeddedDatabaseTest {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        system.shutdown();
    }

    @Test
    public void testFormStatusIsCorrectlyUpdatedOnFormReceived() throws Exception {
        new JavaTestKit(system) {{
            EventStoreFactory eventStoreFactory = new EventStoreFactory();
            eventStoreFactory.setDs(db);

            final Props eventStoreProps = new Props(eventStoreFactory);
            final ActorRef eventStore = system.actorOf(eventStoreProps, "eventStore");

            ArrayList<CommandHandlerFactory> commandHandlerFactories = new ArrayList<CommandHandlerFactory>();

            commandHandlerFactories.add(new CommandHandlerFactory() {
                @Override
                public Actor create() throws Exception {
                    return new FormParser(eventStore);
                }
            });

            commandHandlerFactories.add(new CommandHandlerFactory() {
                @Override
                public Actor create() throws Exception {
                    return new FormDeliverer(eventStore);
                }
            });

            final ActorRef commandDispatcher = system.actorOf(new Props(new CommandDispatcherFactory(commandHandlerFactories, eventStore)), "commandDispatcher");
            final ActorRef sagaManager = system.actorOf(new Props(new SagaManagerFactory(new SagaInMemoryRepository(), commandDispatcher, eventStore)), "sagaManager");

            eventStore.tell(new FormReceived("form_id_1"), getRef());
            eventStore.tell(new Subscription("FORM"), getRef());
            expectMsgClass(FormReceived.class);
            expectMsgClass(FormParsed.class);
            expectMsgClass(FormDelivered.class);

        }};
    }
}
