package no.ks.eventstore2.testkit;

import akka.actor.ActorRef;
import events.test.Order.Order;
import eventstore.Messages;
import eventstore.Settings;
import eventstore.j.SettingsBuilder;
import eventstore.tcp.ConnectionActor;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.eventstore.EventstoreJournalStorage;
import no.ks.eventstore2.eventstore.HandleEventMetadata;
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
                .address(new InetSocketAddress("127.0.0.1", 1113))
                .defaultCredentials("admin", "changeit")
                .build();

        this.eventstoreConnection = _system.actorOf(ConnectionActor.getProps(settings));
        this.journal = new EventstoreJournalStorage(eventstoreConnection);
        journal.open();
    }

    protected long getLatestJournalId(String category) {
        Messages.EventWrapper lastEvent = getLastEvent(category);
        if (lastEvent != null) {
            return lastEvent.getJournalid();
        }
        return -1;
    }

    protected Messages.EventWrapper getLastEvent(String category) {
        final List<Messages.EventWrapper> events = getAllEvents(category);

        if (events.isEmpty()) {
            return null;
        }
        return events.get(events.size() - 1);
    }

    protected List<Messages.EventWrapper> getAllEvents(String category) {
        final ArrayList<Messages.EventWrapper> events = new ArrayList<>();
        final HandleEventMetadata loadEvents = new HandleEventMetadata() {
            @Override
            public void handleEvent(Messages.EventWrapper event) {
                events.add(event);
            }
        };
        boolean finished = journal.loadEventsAndHandle(category, loadEvents);
        while(!finished){
            finished = journal.loadEventsAndHandle(category, loadEvents, events.size());
        }

        return events;
    }

    protected Order.SearchRequest buildSearchRequest() {
        return Order.SearchRequest.newBuilder()
                .setQuery(UUID.randomUUID().toString())
                .setPageNumber(ThreadLocalRandom.current().nextInt(0, 100) + 1)
                .setResultPerPage(ThreadLocalRandom.current().nextInt(0, 100) + 1)
                .build();
    }

    protected Order.SearchResult buildSearchResult() {
        Order.SearchResult.Builder builder = Order.SearchResult.newBuilder();

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
