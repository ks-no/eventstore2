package no.ks.eventstore2;

import eventstore.Messages;
import org.joda.time.DateTime;

import java.io.Serializable;

public abstract class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    private String journalid;
    protected DateTime created;
    private int version = -1;

    public abstract String getAggregateType();

    public DateTime getCreated() {
        return created;
    }

    public void setCreated(DateTime created) {
        this.created = created;
    }

    public abstract String getLogMessage();

    public Event upgrade() {
        return this;
    }

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
                + "aggregateType='" + getAggregateType() + '\''
                + "aggregateRootId='" + getAggregateRootId() + '\''
                + ", journalid='" + getJournalid() + '\''
                + ", created=" + created
                + '}';
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Messages.EventWrapper upgradeToProto(){throw new RuntimeException("UpgradeToProto not implemented in " + this.getClass().toString());}
}