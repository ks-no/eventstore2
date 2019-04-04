package no.ks.eventstore2.eventstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eventstore.Messages;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Map;

public class JsonMetadataBuilder {

    private static final String AGGREGATE = "aggregate";
    private static final String AGGREGATE_ID = "aggregateId";
    private static final String OCCURRED_ON = "occurredOn";
    private static final String SERIALIZATION_ID = "serializationId";
    private static final String USER = "user";
    private static final String MONGO_JOURNAL_ID = "mongoJournalId";

    private static final String CORRELATION_ID = "$correlationId";

    private static final ObjectMapper mapper = new ObjectMapper();

    private JsonMetadataBuilder() { }

    static String build(Messages.EventWrapper protoEvent, String aggregate) {
        ObjectNode metadata = mapper.createObjectNode();

        if (StringUtils.isNotBlank(protoEvent.getCorrelationId())) {
            metadata.put(CORRELATION_ID, protoEvent.getCorrelationId());
        }

        metadata.put(AGGREGATE, aggregate);
        metadata.put(AGGREGATE_ID, protoEvent.getAggregateRootId());
        metadata.put(OCCURRED_ON, protoEvent.getOccurredOn());
        metadata.put(SERIALIZATION_ID, protoEvent.getProtoSerializationType());
        metadata.put(MONGO_JOURNAL_ID, protoEvent.getJournalid());

        if (StringUtils.isNotBlank(protoEvent.getCreatedByUser())) {
            metadata.put(USER, protoEvent.getCreatedByUser());
        }

        try {
            return mapper.writeValueAsString(metadata);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error occurred while generating event json metadata", e);
        }
    }

    public static Map<String,String> read(byte[] metadata) {
        try {
            return mapper.readValue(metadata, new TypeReference<Map<String, String>>(){});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static EventMetadata readMetadata(byte[] metadataBytes) {
        Map<String, String> metadata = read(metadataBytes);
        return new EventMetadata(
                metadata.get(AGGREGATE),
                metadata.get(AGGREGATE_ID),
                Long.parseLong(metadata.get(OCCURRED_ON)),
                metadata.get(SERIALIZATION_ID),
                metadata.get(USER),
                Long.parseLong(metadata.get(MONGO_JOURNAL_ID)),
                metadata.get(CORRELATION_ID)
        );
    }

    public static Messages.EventWrapper.Builder readMetadataAsWrapper(byte[] metadataBytes) {
        EventMetadata eventMetadata = readMetadata(metadataBytes);
        Messages.EventWrapper.Builder builder = Messages.EventWrapper.newBuilder()
                .setAggregateType(eventMetadata.getAggregateType())
                .setAggregateRootId(eventMetadata.getAggregateRootId())
                .setProtoSerializationType(eventMetadata.getProtoSerializationType())
                .setOccurredOn(eventMetadata.getOccuredOn())
                .setJournalid(eventMetadata.getJournalId());

        if (eventMetadata.getCreatedByUser() != null) {
            builder = builder.setCreatedByUser(eventMetadata.getCreatedByUser());
        }

        if (eventMetadata.getCorrelationId() != null) {
            builder = builder.setCorrelationId(eventMetadata.getCorrelationId());
        }

        return builder;
    }
}
