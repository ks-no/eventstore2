package no.ks.eventstore2.testkit;

import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.events.svarut.Test.EventstoreTest;
import no.ks.eventstore2.ProtobufHelper;
import org.junit.jupiter.api.BeforeEach;

public class Eventstore2TestKit extends TestKit {

	protected static final ActorSystem _system = ActorSystem.create("eventstore2", ConfigFactory.parseResources("classpath:/appalication_local.conf"));

	public Eventstore2TestKit() {
		super(_system);
        System.setProperty("CONSTRETTO_TAGS", "ITEST");
    }

	@BeforeEach
	public void setUp() throws Exception {
		ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchRequest.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchResult.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormReceived.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormDelivered.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventStoreForm.FormParsed.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventstoreTest.TestEvent.getDefaultInstance());
	}
}
