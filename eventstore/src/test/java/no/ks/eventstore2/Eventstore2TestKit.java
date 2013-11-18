package no.ks.eventstore2;

import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;

public class Eventstore2TestKit extends TestKit{

	protected static final ActorSystem _system = ActorSystem.create("eventstore2", ConfigFactory.parseResources("classpath:/application_local.conf"));
	protected static final int DURATION = 10000;

	public Eventstore2TestKit() {
		super(_system);
        System.setProperty("CONSTRETTO_TAGS", "ITEST");
    }
}
