package no.ks.eventstore2;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import com.esotericsoftware.kryo.Kryo;
import no.ks.eventstore2.command.CommandDispatcher;
import no.ks.eventstore2.eventstore.*;
import no.ks.eventstore2.formProcessorProject.*;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.saga.SagaManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FormProcessorIntegrationTest extends MongoDbEventstore2TestKit {

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
        EventstoreSingelton.kryoSerializedEvents.add("FORM");
    }

    @AfterClass
    public static void teardown() throws TimeoutException, InterruptedException {
        Await.ready(system.terminate(), Duration.create(30, TimeUnit.SECONDS));
    }

    @Test
    public void testFormStatusIsCorrectlyUpdatedOnFormReceived() throws Exception {
        new JavaTestKit(system) {{

            final Props eventStoreProps = EventStore.mkProps(new MongoDBJournalV2(mongoClient.getDatabase("Journal"), kryoClassRegistration, Arrays.asList(new String[]{"FORM"}), null));
            final ActorRef eventStore = system.actorOf(eventStoreProps, "eventStore");

            ArrayList<Props> commandHandlerProps = new ArrayList<>();

            commandHandlerProps.add(Props.create(FormParser.class, eventStore));

            commandHandlerProps.add(Props.create(FormDeliverer.class, eventStore));


            final ActorRef commandDispatcher = system.actorOf(CommandDispatcher.mkProps(commandHandlerProps), "commandDispatcher");
            final ActorRef sagaManager = system.actorOf(SagaManager.mkProps(system, commandDispatcher, new SagaInMemoryRepository(), eventStore), "sagaManager");

            eventStore.tell(new FormReceived("form_id_1"), getRef());
            eventStore.tell(new Subscription("FORM"), getRef());
            expectMsgClass(FormReceived.class);
            expectMsgClass(CompleteSubscriptionRegistered.class);
            expectMsgClass(FormParsed.class);
            expectMsgClass(FormDelivered.class);

        }};
    }
}
