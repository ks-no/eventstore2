package no.ks.eventstore2;

import com.google.protobuf.Message;
import org.joda.time.DateTime;

import java.lang.reflect.InvocationTargetException;

public class EventMetadata<T extends Message> {

    String correlationId;
    String protoSerializationType;
    String aggregateRootId;
    String journalid;
    String aggregateType;
    int version;
    DateTime occurredTimestamp;
    T event;

    public EventMetadata(String aggregateType, String aggregateId, T event) {
        this.aggregateType = aggregateType;
        this.aggregateRootId = aggregateId;
        this.event = event;
        protoSerializationType = event.getDescriptorForType().getFullName();
    }

    public EventMetadata() {

    }

    public EventMetadata(String journalid, String aggregateRootId, Integer version, String protoSerializationType, Object eventData) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        this.journalid = journalid;
        this.aggregateRootId = aggregateRootId;
        this.version = version;
        this.protoSerializationType = protoSerializationType;
        event = (T) (Class.forName(protoSerializationType)).getDeclaredMethod("parseFrom", byte[].class).invoke(eventData);
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getProtoSerializationType() {
        return protoSerializationType;
    }

    public void setProtoSerializationType(String protoSerializationType) {
        this.protoSerializationType = protoSerializationType;
    }

    public String getAggregateRootId() {
        return aggregateRootId;
    }

    public void setAggregateRootId(String aggregateRootId) {
        this.aggregateRootId = aggregateRootId;
    }

    public String getJournalid() {
        return journalid;
    }

    public void setJournalid(String journalid) {
        this.journalid = journalid;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public DateTime getOccurredTimestamp() {
        return occurredTimestamp;
    }

    public void setOccurredTimestamp(DateTime occurredTimestamp) {
        this.occurredTimestamp = occurredTimestamp;
    }

    public T getEvent() {
        return event;
    }

    public void setEvent(T event) {
        this.event = event;
    }
}
