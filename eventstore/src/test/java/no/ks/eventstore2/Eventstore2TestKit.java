package no.ks.eventstore2;

import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import events.test.Order.Order;
import events.test.form.Form;
import no.ks.events.svarut.Order.EventstoreOrder;
import org.junit.Before;

public class Eventstore2TestKit extends TestKit{

	protected static final ActorSystem _system = ActorSystem.create("eventstore2", ConfigFactory.parseResources("classpath:/application_local.conf"));
	protected static final int DURATION = 10000;

	public Eventstore2TestKit() {
		super(_system);
        System.setProperty("CONSTRETTO_TAGS", "ITEST");
    }

	@Before
	public void setUp() throws Exception {
		ProtobufHelper.registerDeserializeMethod(Order.SearchRequest.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(Order.SearchResult.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchRequest.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(EventstoreOrder.SearchResult.getDefaultInstance());
		ProtobufHelper.registerDeserializeMethod(Form.FormReceived.getDefaultInstance());
	}
}
