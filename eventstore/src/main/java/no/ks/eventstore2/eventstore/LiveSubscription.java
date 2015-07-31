package no.ks.eventstore2.eventstore;

public class LiveSubscription extends Subscription{

    public LiveSubscription(String aggregateType) {
        super(aggregateType);
    }
}
