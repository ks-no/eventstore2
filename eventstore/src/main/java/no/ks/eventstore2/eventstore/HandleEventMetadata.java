package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.EventMetadata;

public abstract class HandleEventMetadata {
    public abstract void handleEvent(EventMetadata event);
}
