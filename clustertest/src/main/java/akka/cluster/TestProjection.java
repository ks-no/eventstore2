package akka.cluster;

import akka.actor.ActorRef;
import no.ks.eventstore2.projection.ListensTo;
import no.ks.eventstore2.projection.Projection;

@ListensTo(value = {Increase.class}, aggregates = "test")
public class TestProjection extends Projection {

    public TestProjection(ActorRef eventStore) {
        super(eventStore);
    }

    int count = 0;

    public void handleEvent(Increase event){
        System.out.println("IncreasingCount");
        count++;
    }

    public Integer getCount(){
        return count;
    }
}
