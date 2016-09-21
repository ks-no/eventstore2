package no.ks.eventstore2.events;

import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.ProtobufHelper;

import java.util.ArrayList;

public class OldEventShouldBeUpgradedToOrderSearchResult extends Event {

    String rootId;

    ArrayList<String> results = new ArrayList<>();

    public OldEventShouldBeUpgradedToOrderSearchResult(String rootId, ArrayList<String> results) {
        this.rootId = rootId;
        this.results = results;
    }

    @Override
    public String getAggregateType() {
        return "ORDER";
    }

    @Override
    public String getLogMessage() {
        return null;
    }

    @Override
    public String getAggregateRootId() {
        return rootId;
    }

    @Override
    public Messages.EventWrapper upgradeToProto() {
        return ProtobufHelper.newEventWrapper(getAggregateType(), getAggregateRootId(), Long.valueOf(getJournalid()),
                Order.SearchResult.newBuilder().addAllResult(results).build());
    }
}
