package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import no.ks.eventstore2.TakeSnapshot;

public abstract class ProjectionSnapshot extends Projection {
    public ProjectionSnapshot(ActorRef eventstoreConnection) {
        super(eventstoreConnection);
    }

    @Override
    public void preStart() {
        loadSnapshot();
        super.preStart();
    }

    @Override
    public Receive createReceive() {
        return super.createReceiveBuilder()
                .match(TakeSnapshot.class, o -> saveSnapshot())
                .build();
    }

    public abstract void saveSnapshot();

    public abstract void loadSnapshot();

}
