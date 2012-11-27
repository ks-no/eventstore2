package no.ks.eventstore2.eventstore.testImplementations;

import com.google.common.collect.ImmutableSet;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.projection.ListensTo;
import no.ks.eventstore2.projection.Projection;

import java.util.ArrayList;
import java.util.List;

@ListensTo(value={EventA1.class}, aggregates = "A1")
public class ProjectionA extends Projection {
    List<Event> eventProjection = new ArrayList<Event>();

    @Override
    public void onReceive(Object o) throws Exception {
        super.onReceive(o);
        if (o instanceof String){
            String message = (String) o;
            if (message.equals("getProjectedEvents")){
                getProjectedEvents();
            }
        }
    }

    private void getProjectedEvents() {
        System.out.println(self().path() + ": svarer med projeksjon, størrelse " + eventProjection.size());
        sender().tell(ImmutableSet.copyOf(eventProjection), self());
    }

    public void handleEvent(EventA1 event){
        eventProjection.add((Event) event);
        System.out.println(self().path() + ": ny event, har nå " + eventProjection.size());
    }
}
