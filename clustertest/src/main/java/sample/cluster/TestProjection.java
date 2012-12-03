package sample.cluster;

import no.ks.eventstore2.projection.ListensTo;
import no.ks.eventstore2.projection.Projection;

@ListensTo(value = {Increase.class}, aggregates = "Test")

public class TestProjection extends Projection {

    @Override
    public void preStart() {
        eventStore = getContext().actorFor("eventStore");
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
