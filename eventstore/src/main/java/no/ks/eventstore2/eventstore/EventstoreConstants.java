package no.ks.eventstore2.eventstore;

public final class EventstoreConstants {

    private EventstoreConstants() {}

    private static final String SVARUT_EVENTS_PREFIX = "no.ks.events.svarut";

    public static String getAggregateStreamId(String aggregateType, String aggregateId) {
        return String.format("%s.%s-%s", SVARUT_EVENTS_PREFIX, aggregateType, aggregateId);
    }

    public static String getCategoryStreamId(String aggregateType) {
        return String.format("$ce-%s.%s", SVARUT_EVENTS_PREFIX, aggregateType);
    }

    public static String getSystemCategoryStreamId(String aggregateType) {
        return String.format("ce-%s.%s", SVARUT_EVENTS_PREFIX, aggregateType);
    }
}
