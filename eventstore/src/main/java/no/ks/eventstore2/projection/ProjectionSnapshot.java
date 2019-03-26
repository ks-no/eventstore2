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
    public void onReceive(Object o) {
        super.onReceive(o);
        if (o instanceof TakeSnapshot) {
            saveSnapshot();
        }
    }

    public abstract void saveSnapshot();

    public abstract void loadSnapshot();

}
