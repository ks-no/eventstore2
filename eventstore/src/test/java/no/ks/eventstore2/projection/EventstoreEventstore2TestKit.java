package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import eventstore.Settings;
import eventstore.j.SettingsBuilder;
import eventstore.tcp.ConnectionActor;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.eventstore.EventstoreJournalStorage;
import no.ks.eventstore2.eventstore.JournalStorage;

import java.net.InetSocketAddress;

class EventstoreEventstore2TestKit extends Eventstore2TestKit {

    protected ActorRef eventstoreConnection;
    protected final JournalStorage journal;

    public EventstoreEventstore2TestKit() {
        final Settings settings = new SettingsBuilder()
                .address(new InetSocketAddress("127.0.0.1", 1113))
                .defaultCredentials("admin", "changeit")
                .build();

        this.eventstoreConnection = _system.actorOf(ConnectionActor.getProps(settings));
        this.journal = new EventstoreJournalStorage(_system);
        journal.open();
    }
}
