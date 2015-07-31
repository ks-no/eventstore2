package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import no.ks.eventstore2.eventstore.LiveSubscription;

public class LiveSubscriptionProjection extends Projection {

    public LiveSubscriptionProjection(ActorRef eventStore) {
        super(eventStore);
    }

    @Override
    protected void subscribe() {
        setInSubscribe();
        eventStore.tell(new LiveSubscription(getSubscribe().getAggregateType()), self());
    }
}
