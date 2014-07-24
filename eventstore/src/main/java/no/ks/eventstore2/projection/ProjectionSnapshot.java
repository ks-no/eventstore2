package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.eventstore.AsyncSubscription;
import no.ks.eventstore2.eventstore.Subscription;

public abstract class ProjectionSnapshot extends Projection {
    public ProjectionSnapshot(ActorRef eventStore) {
        super(eventStore);
    }

    @Override
    public void preStart(){
        loadSnapshot();
        Subscription subscribe = getSubscribe();
        eventStore.tell(new AsyncSubscription(subscribe.getAggregateType(), latestJournalidReceived), self());
    }


    @Override
    public void onReceive(Object o) {
        super.onReceive(o);
        if(o instanceof TakeSnapshot){
            saveSnapshot();
        }
    }

    public abstract void saveSnapshot();

    public abstract void loadSnapshot();

}
