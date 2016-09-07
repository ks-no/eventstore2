package no.ks.eventstore2;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import eventstore.Messages;
import org.bson.types.Binary;

import java.util.HashMap;
import java.util.Map;

public class ProtobufHelper {

    private static Map<String, Class> deserializeClasses = new HashMap<>();

    private ProtobufHelper() {
    }

    static Map<String, Parser<? extends Message>> deserializeMethods = new HashMap<>();

    public static void registerDeserializeMethod(Message message){
        deserializeMethods.put(message.getDescriptorForType().getFullName(), message.getParserForType());
        deserializeClasses.put(message.getDescriptorForType().getFullName(), message.getClass());
    }

    public static <T extends Message> T deserializeByteArray(String type, byte[] message){
        try {
            return (T) deserializeMethods.get(type).parseFrom(message);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, long version, Message event) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(version)
                .setProtoSerializationType(event.getDescriptorForType().getFullName())
                .setEvent(Any.pack(event)).build();
    }

    public static Messages.EventWrapper newEventWrapper(String aggregateType, String aggregateRootId, Message event) {
        return Messages.EventWrapper.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .setVersion(-1)
                .setProtoSerializationType(event.getDescriptorForType().getFullName())
                .setEvent(Any.pack(event)).build();
    }


    public static Messages.EventWrapper newEventWrapper(String correlationid, String protoSerializationType, String aggregateRootId, long journalid, String aggregateType, long version, long occuredon, Binary data) throws InvalidProtocolBufferException {
        return Messages.EventWrapper.newBuilder()
                .setCorrelationId(correlationid)
                .setProtoSerializationType(protoSerializationType)
                .setAggregateRootId(aggregateRootId)
                .setJournalid(journalid)
                .setAggregateType(aggregateType)
                .setVersion(version)
                .setOccurredOn(occuredon)
                .setEvent(Any.parseFrom(data.getData()))
                .build();
    }
}
