package no.ks.eventstore2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.HashMap;
import java.util.Map;

public class ProtobufHelper {

    private ProtobufHelper() {
    }

    static Map<String, Parser<? extends Message>> deserializeMethods = new HashMap<>();

    public static void registerDeserializeMethod(Message message){
        deserializeMethods.put(message.getDescriptorForType().getFullName(), message.getParserForType());
    }

    public static <T extends Message> T deserializeByteArray(String type, byte[] message){
        try {
            return (T) deserializeMethods.get(type).parseFrom(message);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
