package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;

public interface HandleEvent {

    void handleEvent(final Event event);
}
