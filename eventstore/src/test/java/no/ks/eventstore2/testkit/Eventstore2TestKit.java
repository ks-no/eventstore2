package no.ks.eventstore2.testkit;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import events.test.Order.Order;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.events.svarut.Test.EventstoreTest;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaInMemoryRepository;
import no.ks.eventstore2.testkit.EventReceiver;
import no.ks.eventstore2.util.IdUtil;
import org.junit.jupiter.api.BeforeEach;

public class Eventstore2TestKit extends TestKit {

	protected static final ActorSystem _system = ActorSystem.create("eventstore2", ConfigFactory.parseResources("classpath:/application_local.conf"));
	protected static final int DURATION = 10000;

	public Eventstore2TestKit() {
		super(_system);
        System.setProperty("CONSTRETTO_TAGS", "ITEST");
    }

	@BeforeEach
	public void setUp() throws Exception {
		ProtobufHelper.registerDeserializeMethod(Order.SearchRequest.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(Order.SearchResult.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchRequest.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchResult.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormReceived.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormDelivered.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormParsed.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventstoreTest.TestEvent.getDefaultInstance());
	}

	protected <T extends Actor> TestActorRef<T> createCommandHandler(Props props) {
		return TestActorRef.create(_system, props, IdUtil.createUUID());
	}

	protected <T extends Actor> TestActorRef<T> createProjectionRef(Props props) {
		return TestActorRef.create(_system, props, IdUtil.createUUID());
	}

	protected <T extends Saga> T createSaga(Class<T> clz) {
		Props sagaProps = Props.create(clz, IdUtil.createUUID(), testActor(), new SagaInMemoryRepository());
		return (T) TestActorRef.create(_system, sagaProps, IdUtil.createUUID()).underlyingActor();
	}

	protected EventReceiver createEventReceiver() {
		return (EventReceiver) TestActorRef.create(_system, Props.create(EventReceiver.class), IdUtil.createUUID()).underlyingActor();
	}
}
