package no.ks.eventstore2;

import org.joda.time.DateTime;

import java.io.Serializable;

public abstract class Event implements Serializable{
	private static final long serialVersionUID = 1L;

	protected String aggregateId;

    protected DateTime created;

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public DateTime getCreated() {
        return created;
    }

    public void setCreated(DateTime created) {
        this.created = created;
    }

    public abstract String getLogMessage();

    public Event upgrade(){return this;}
}