package no.ks.eventstore2;

import org.joda.time.DateTime;

import java.io.Serializable;

public abstract class Event implements Serializable{
	private static final long serialVersionUID = 1L;
    /* aggregate type*/
    protected String aggregateId;

    protected String aggregateRootId;

    private String journalid;

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

    public String getJournalid() {
        return journalid;
    }

    public void setJournalid(String journalid) {
        this.journalid = journalid;
    }

    public String getAggregateRootId() {
        return aggregateRootId;
    }

    public void setAggregateRootId(String aggregateRootId) {
        this.aggregateRootId = aggregateRootId;
    }

    @Override
    public String toString() {
        return "Event{"
                + "aggregateId='" + getAggregateId() + '\''
                + "aggregateRootId='" + getAggregateRootId() + '\''
                + ", journalid='" + getJournalid() + '\''
                + ", created=" + created
                + '}';
    }
}