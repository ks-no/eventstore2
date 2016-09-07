package no.ks.eventstore2.eventstore;

import eventstore.Messages;

public abstract class HandleEventMetadata {
    public abstract void handleEvent(Messages.EventWrapper event);
}
