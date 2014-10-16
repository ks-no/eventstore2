package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;

import java.io.Serializable;
import java.util.List;

public class StoreEvents implements Serializable {

    private String aggregateType;

    private List<? extends Event> events;

    public StoreEvents() {
    }

    public StoreEvents(List<? extends Event> events) {
        this.events = events;
    }

    public List<? extends Event> getEvents() {
        return events;
    }

    public void setEvents(List<? extends Event> events) {
        this.events = events;
    }

    public String getLogMessage() {
        String logmessage = "";
        for (Event event : events) {
            logmessage += event.getLogMessage();
        }
        return logmessage;
    }

    @Override
    public String toString() {
        return "StoreEvents{" +
                "aggregateType='" + aggregateType + '\'' +
                ", events=" + events +
                '}';
    }
}
