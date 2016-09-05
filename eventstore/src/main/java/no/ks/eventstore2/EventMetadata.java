package no.ks.eventstore2;

import com.google.protobuf.Message;
import org.bson.types.Binary;
import org.joda.time.DateTime;

import java.lang.reflect.InvocationTargetException;

public class EventMetadata<T extends Message> {

    String correlationId;
    String protoSerializationType;
    String aggregateRootId;
    long journalid;
    String aggregateType;
    long version;
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

    public EventMetadata(long journalid, String aggregateRootId, long version, String protoSerializationType, Binary eventData) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        this.journalid = journalid;
        this.aggregateRootId = aggregateRootId;
        this.version = version;
        this.protoSerializationType = protoSerializationType;
        event = ProtobufHelper.deserializeByteArray(protoSerializationType, eventData.getData());
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

    public long getJournalid() {
        return journalid;
    }

    public void setJournalid(long journalid) {
        this.journalid = journalid;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
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
