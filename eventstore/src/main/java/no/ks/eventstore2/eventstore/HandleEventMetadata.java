package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.EventWrapper;

public abstract class HandleEventMetadata {
    public abstract void handleEvent(EventWrapper event);
}
