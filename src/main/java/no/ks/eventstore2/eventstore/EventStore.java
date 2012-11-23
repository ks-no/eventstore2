package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EventStore extends UntypedActor {

    List<Event> store = new ArrayList<Event>();
    Map<String, List<ActorRef>> aggregateSubscribers = new HashMap<String, List<ActorRef>>();

    @Override
    public void preStart(){
        System.out.println(getSelf().path().toString());
    }

    public void onReceive(Object o) throws Exception {
        if (o instanceof Event){
            store.add((Event) o);
            publishEvent((Event) o);
        } else if (o instanceof Subscription){
            Subscription subscription = (Subscription) o;

            publishEvents(subscription.getAggregateId());
            addSubscriber(subscription);
        }
    }

    private void publishEvent(Event event) {
        List<ActorRef> actorRefs = aggregateSubscribers.get(event.getAggregateId());
        if (actorRefs == null)
            return;
        for (ActorRef ref : actorRefs) {
            ref.tell(event, self());
        }
    }

    private void addSubscriber(Subscription subscription) {
        if (!aggregateSubscribers.containsKey(subscription.getAggregateId()))
            aggregateSubscribers.put(subscription.getAggregateId(), new ArrayList<ActorRef>());

        aggregateSubscribers.get(subscription.getAggregateId()).add(sender());
    }

    private void publishEvents(String aggregateid) {
        for (Event event : store) {
            if(aggregateid.equals(event.getAggregateId())){
                sender().tell(event,self());
            }
        }
    }

}
