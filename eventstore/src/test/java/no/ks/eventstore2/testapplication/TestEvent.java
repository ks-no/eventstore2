package no.ks.eventstore2.testapplication;

import no.ks.eventstore2.Event;

public class TestEvent extends Event {

    @Override
    public String getLogMessage() {
        return "This is a test event";
    }

    @Override
    public String getAggregateRootId() {
        return null;
    }

    @Override
    public String getAggregateType() {
        return AggregateType.TEST_AGGREGATE;
    }
}