package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import no.ks.eventstore2.TakeSnapshot;

public abstract class ProjectionSnapshot extends Projection {

    public ProjectionSnapshot(ActorRef eventStoreConnection) {
        super(eventStoreConnection);
    }

    public ProjectionSnapshot(ActorRef eventStoreConnection, boolean shouldNotifyProjectionManager) {
        super(eventStoreConnection, shouldNotifyProjectionManager);
    }

    @Override
    public void preStart() {
        loadSnapshot();
        super.preStart();
    }

    @Override
    public Receive createReceive() {
        return createReceiveBuilder().build();
    }

    protected ReceiveBuilder createReceiveBuilder() {
        return super.createReceiveBuilder()
                .match(TakeSnapshot.class, o -> saveSnapshot());
    }

    public abstract void saveSnapshot();

    public abstract void loadSnapshot();

}
