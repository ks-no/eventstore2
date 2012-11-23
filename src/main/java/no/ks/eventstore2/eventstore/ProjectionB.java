package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;

public class ProjectionB extends UntypedActor{
    List<Event> eventProjection = new ArrayList<Event>();

    @Override
    public void preStart(){
        final ActorRef eventStoreRef = getContext().actorFor("akka://default/user/eventStore");
        eventStoreRef.tell(new Subscription("A1"), self());
        eventStoreRef.tell(new Subscription("A2"), self());
    }

    @Override
    public void onReceive(Object o) throws Exception {
        if (o instanceof Event){
            eventProjection.add((Event) o);
        } else if (o instanceof String){
            String message = (String) o;
            if (message.equals("getProjectedEvents")){
                getProjectedEvents();
            }
        }
    }

    private void getProjectedEvents() {
        sender().tell(ImmutableSet.copyOf(eventProjection), self());
    }
}
