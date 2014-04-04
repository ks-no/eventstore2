package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class EventBatch implements Serializable{

    private final String aggregateType;
    private final String aggregateId;
    private final boolean readAllEvents;
    private List<Event> events;

    public EventBatch(String aggregateType, String aggregateId, List<Event> events, boolean readAllEvents) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.readAllEvents = readAllEvents;
        setEvents(events);
    }

    private void setEvents(List<Event> events){
        if(events == null) this.events = new ArrayList<>();
        else this.events = events;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public List<Event> getEvents() {
        return events;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public boolean isReadAllEvents() {
        return readAllEvents;
    }

    public String latestJournalId(){
        if(events.size() == 0) return null;
        return events.get(events.size()-1).getJournalid();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventBatch)) return false;

        EventBatch that = (EventBatch) o;

        if (readAllEvents != that.readAllEvents) return false;
        if (aggregateId != null ? !aggregateId.equals(that.aggregateId) : that.aggregateId != null) return false;
        if (aggregateType != null ? !aggregateType.equals(that.aggregateType) : that.aggregateType != null)
            return false;
        if (events != null ? !events.equals(that.events) : that.events != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = aggregateType != null ? aggregateType.hashCode() : 0;
        result = 31 * result + (aggregateId != null ? aggregateId.hashCode() : 0);
        result = 31 * result + (readAllEvents ? 1 : 0);
        result = 31 * result + (events != null ? events.hashCode() : 0);
        return result;
    }
}
