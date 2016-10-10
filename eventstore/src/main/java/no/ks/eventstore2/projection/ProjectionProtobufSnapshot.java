package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import no.ks.eventstore2.TakeSnapshot;

public abstract class ProjectionProtobufSnapshot extends ProjectionProtobuf {
    public ProjectionProtobufSnapshot(ActorRef eventStore) {
        super(eventStore);
    }

    @Override
    public void preStart() {
        loadSnapshot();
        subscribe();
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
