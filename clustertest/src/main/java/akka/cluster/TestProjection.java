package akka.cluster;

import no.ks.eventstore2.projection.ListensTo;
import no.ks.eventstore2.projection.Projection;

@ListensTo(value = {Increase.class}, aggregates = "test")

public class TestProjection extends Projection {

    @Override
    public void preStart() {
        eventStore = getContext().actorFor("akka://EventStore2Test/user/eventStore");
        subscribe(eventStore);
    }

    int count = 0;

    public void handleEvent(Increase event){
        count++;
    }

    public Integer getCount(){
        return count;
    }
}
