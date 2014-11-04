package no.ks.eventstore2;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import com.esotericsoftware.kryo.Kryo;
import no.ks.eventstore2.command.CommandDispatcherFactory;
import no.ks.eventstore2.command.CommandHandlerFactory;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.eventstore.EventStore;
import no.ks.eventstore2.eventstore.H2JournalStorage;
import no.ks.eventstore2.eventstore.KryoClassRegistration;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.formProcessorProject.FormDelivered;
import no.ks.eventstore2.formProcessorProject.FormDeliverer;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormParser;
import no.ks.eventstore2.formProcessorProject.FormReceived;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.saga.SagaManagerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class FormProcessorIntegrationTest extends EmbeddedDatabaseTest {

    private static ActorSystem system;

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(FormDelivered.class, 1001);
            kryo.register(FormDeliverer.class, 1002);
            kryo.register(FormParsed.class, 1003);
            kryo.register(FormParser.class, 1004);
            kryo.register(FormReceived.class, 1005);
        }
    };

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

            final Props eventStoreProps = EventStore.mkProps(new H2JournalStorage(db, kryoClassRegistration));
            final ActorRef eventStore = system.actorOf(eventStoreProps, "eventStore");

            ArrayList<CommandHandlerFactory> commandHandlerFactories = new ArrayList<CommandHandlerFactory>();

            commandHandlerFactories.add(new CommandHandlerFactory() {
                public Actor create() throws Exception {
                    return new FormParser(eventStore);
                }
            });

            commandHandlerFactories.add(new CommandHandlerFactory() {
                public Actor create() throws Exception {
                    return new FormDeliverer(eventStore);
                }
            });

            CommandDispatcherFactory commandDispatcherFactory = new CommandDispatcherFactory(commandHandlerFactories, eventStore);
            final ActorRef commandDispatcher = system.actorOf(new Props(commandDispatcherFactory), "commandDispatcher");
            final ActorRef sagaManager = system.actorOf(new Props(new SagaManagerFactory(new SagaInMemoryRepository(), commandDispatcher, eventStore)), "sagaManager");

            eventStore.tell(new FormReceived("form_id_1"), getRef());
            eventStore.tell(new Subscription("FORM"), getRef());
            expectMsgClass(FormReceived.class);
            expectMsgClass(CompleteSubscriptionRegistered.class);
            expectMsgClass(FormParsed.class);
            expectMsgClass(FormDelivered.class);

        }};
    }
}
