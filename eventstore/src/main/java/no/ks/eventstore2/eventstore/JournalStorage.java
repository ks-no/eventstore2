package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;

public interface JournalStorage {

    void saveEvent(Event event);

    void loadEventsAndHandle(String aggregateid, final HandleEvent handleEvent);

    void open();
    void close();
}
