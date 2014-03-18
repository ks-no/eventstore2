package no.ks.eventstore2;

import org.joda.time.DateTime;

import java.io.Serializable;

public abstract class Event implements Serializable{
	private static final long serialVersionUID = 1L;
    /* aggregate type*/
    protected String aggregateId;

    private String journalid;

    protected DateTime created;

    /* @Deprecated use getAggregateType() instead. */
    @Deprecated
    public String getAggregateId() {
        return aggregateId;
    }

    /* @Deprecated do not set the aggregate id, just override getAggregateType() instead. */
    @Deprecated
    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getAggregateType() {
    	return aggregateId;
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

    public abstract String getAggregateRootId();

    @Override
    public String toString() {
        return "Event{"
                + "aggregateId='" + getAggregateType() + '\''
                + "aggregateRootId='" + getAggregateRootId() + '\''
                + ", journalid='" + getJournalid() + '\''
                + ", created=" + created
                + '}';
    }
}