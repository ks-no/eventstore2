package no.ks.eventstore2.testkit;

import akka.actor.ActorRef;
import eventstore.Messages;
import eventstore.Settings;
import eventstore.j.SettingsBuilder;
import eventstore.tcp.ConnectionActor;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.events.svarut.Order.EventstoreOrder;
import no.ks.eventstore2.eventstore.EventStoreJournalStorage;
import no.ks.eventstore2.eventstore.JournalStorage;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class EventstoreEventstore2TestKit extends Eventstore2TestKit {

    protected ActorRef eventstoreConnection;
    protected final JournalStorage journal;

    public EventstoreEventstore2TestKit() {
        final Settings settings = new SettingsBuilder()
                .address(new InetSocketAddress("127.0.0.1", 51113))
                .defaultCredentials("admin", "changeit")
                .build();

        this.eventstoreConnection = _system.actorOf(ConnectionActor.getProps(settings));
        this.journal = new EventStoreJournalStorage(eventstoreConnection, _system.dispatcher());
    }

    protected long getLatestJournalId(String category) {
        Messages.EventWrapper lastEvent = getLastEvent(category);
        if (lastEvent != null) {
            return lastEvent.getJournalid();
        }
        return -1;
    }

    protected Messages.EventWrapper getLastEvent(String category) {
        try {
            final List<Messages.EventWrapper> events = getAllEvents(category);

            if (events.isEmpty()) {
                return null;
            }
            return events.get(events.size() - 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected List<Messages.EventWrapper> getAllEvents(String category) {
        try {
            final ArrayList<Messages.EventWrapper> events = new ArrayList<>();
            boolean finished = journal.loadEventsAndHandle(category, events::add);
            while(!finished){
                finished = journal.loadEventsAndHandle(category, events::add, events.size());
            }

            return events;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected EventstoreOrder.SearchRequest buildSearchRequest() {
        return EventstoreOrder.SearchRequest.newBuilder()
                .setQuery(UUID.randomUUID().toString())
                .setPageNumber(ThreadLocalRandom.current().nextInt(0, 100) + 1)
                .setResultPerPage(ThreadLocalRandom.current().nextInt(0, 100) + 1)
                .build();
    }

    protected EventstoreOrder.SearchResult buildSearchResult() {
        EventstoreOrder.SearchResult.Builder builder = EventstoreOrder.SearchResult.newBuilder();

        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10); i++) {
            builder = builder.addResult(UUID.randomUUID().toString());
        }

        return builder
                .setNumberOfResults(builder.getResultCount())
                .build();
    }

    protected EventStoreForm.FormParsed buildFormParsed() {
        return EventStoreForm.FormParsed.newBuilder()
                .setFormId(UUID.randomUUID().toString())
                .build();
    }

    protected EventStoreForm.FormDelivered buildFormDelivered() {
        return EventStoreForm.FormDelivered.newBuilder()
                .setFormId(UUID.randomUUID().toString())
                .build();
    }

    protected EventStoreForm.FormReceived buildFormReceived() {
        return EventStoreForm.FormReceived.newBuilder()
                .setFormId(UUID.randomUUID().toString())
                .build();
    }
}
