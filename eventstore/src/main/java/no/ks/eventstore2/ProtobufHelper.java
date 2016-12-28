package no.ks.eventstore2;

import com.google.protobuf.*;
import eventstore.Messages;
import org.bson.types.Binary;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ProtobufHelper {

    private static Map<String, Class> deserializeClasses = new HashMap<>();

    static Map<String, Parser<? extends Message>> deserializeMethods = new HashMap<>();
    private static Map<Class<? extends Message>, String> classToType = new HashMap<>();

    private ProtobufHelper() {
    }

    public static void registerDeserializeMethod(Message message) {
        deserializeMethods.put(message.getDescriptorForType().getFullName(), message.getParserForType());
        deserializeClasses.put(message.getDescriptorForType().getFullName(), message.getClass());
        classToType.put(message.getClass(), message.getDescriptorForType().getFullName());
    }

    public static void registerDeserializeMethod(Message message, String type) {
        deserializeMethods.put(type, message.getParserForType());
        deserializeClasses.put(type, message.getClass());
        classToType.put(message.getClass(), type);
    }

    public static String getTypeForClass(Class<? extends Message> clazz){
        return classToType.get(clazz);
    }

    public static <T extends Message> T deserializeByteArray(String type, byte[] message) {
        try {
            return (T) deserializeMethods.get(type).parseFrom(message);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Message> T unPackAny(String type, Any any) {
        try {
            return (T) any.unpack(deserializeClasses.get(type));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException();
        }
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, long version, Message event) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(version)
                .setOccurredOn(DateTime.now().getMillis())
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event)).build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, long version, Message event, String createdByUser) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(version)
                .setOccurredOn(DateTime.now().getMillis())
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event))
                .setCreatedByUser(createdByUser)
                .build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, long version, DateTime occuredon, Message event) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(version)
                .setOccurredOn(occuredon.getMillis())
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event)).build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, long version, long journalid, DateTime occuredon, Message event) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(version)
                .setOccurredOn(occuredon.getMillis())
                .setJournalid(journalid)
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event)).build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, long version, long journalid, DateTime occuredon, Message event, String createdByUser) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(version)
                .setOccurredOn(occuredon.getMillis())
                .setJournalid(journalid)
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event))
                .setCreatedByUser(createdByUser)
                .build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, long version, DateTime occuredon, Message event, String createdByUser) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(version)
                .setOccurredOn(occuredon.getMillis())
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event))
                .setCreatedByUser(createdByUser)
                .build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, Message event) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(-1)
                .setOccurredOn(DateTime.now().getMillis())
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event)).build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, Message event, String correlationid, String createdByUser) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(-1)
                .setOccurredOn(DateTime.now().getMillis())
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event))
                .setCorrelationId(correlationid)
                .setCreatedByUser(createdByUser)
                .build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, Message event, String createdByUser) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(-1)
                .setOccurredOn(DateTime.now().getMillis())
                .setProtoSerializationType(classToType.get(event.getClass()))
                .setEvent(Any.pack(event))
                .setCreatedByUser(createdByUser)
                .build();
    }


    public static Messages.EventWrapper newEventWrapper(String correlationid, String protoSerializationType, String aggregateRootId, long journalid, String aggregateType, long version, long occuredon, Binary data, String createdByUser) throws InvalidProtocolBufferException {
        return Messages.EventWrapper.newBuilder()
                .setCorrelationId(correlationid)
                .setProtoSerializationType(protoSerializationType)
                .setAggregateRootId(aggregateRootId)
                .setJournalid(journalid)
                .setAggregateType(aggregateType)
                .setVersion(version)
                .setOccurredOn(occuredon)
                .setEvent(Any.parseFrom(data.getData()))
                .setCreatedByUser(createdByUser)
                .build();
    }

    public static Class<?> getClassForSerialization(String protoSerializationType) {
        return deserializeClasses.get(protoSerializationType);
    }

    public static String toLog(Messages.EventWrapper eventWrapper) {
        return "Wrapper: " + eventWrapper + " event: " + ProtobufHelper.unPackAny(eventWrapper.getProtoSerializationType(), eventWrapper.getEvent());
    }

    public static Timestamp toTimestamp(DateTime time){
        long millis = time.getMillis();
        return Timestamp.newBuilder().setSeconds(millis / 1000)
         .setNanos((int) ((millis % 1000) * 1000000)).build();
    }

    public static DateTime fromTimestamp(Timestamp ts){
        return new DateTime((ts.getSeconds() * 1000) + ts.getNanos() / 1000000);
    }

    public static Date fromTimestampToDate(Timestamp ts){
        return new Date((ts.getSeconds() * 1000) + ts.getNanos() / 1000000);
    }

    public static Timestamp toTimestamp(Date now) {
        long millis = now.getTime();
        return Timestamp.newBuilder().setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000000)).build();
    }
}
