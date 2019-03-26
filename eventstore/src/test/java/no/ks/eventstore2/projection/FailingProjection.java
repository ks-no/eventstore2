package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.eventstore.Subscription;

public class FailingProjection extends ProjectionOld {

    boolean failed = false;

    public FailingProjection(ActorRef eventStore) {
        super(eventStore);
    }

    @Override
    protected Subscription getSubscribe() {
        return new Subscription("agg");
    }

    @Handler
    public void handleEvent(Event event) {
        if (failed)
            sender().tell(event, self());
        else {
            failed = true;
            throw new RuntimeException("Failing");
        }
    }
}

