package no.ks.eventstore2.eventstore;

import eventstore.Messages;
import scala.concurrent.Future;

import java.util.List;
import java.util.function.Consumer;

public interface JournalStorage {
    void open();
    void close();
    boolean loadEventsAndHandle(String aggregateType, Consumer<Messages.EventWrapper> handleEvent);
    boolean loadEventsAndHandle(String aggregateType, Consumer<Messages.EventWrapper> handleEvent, long fromKey);
    Messages.EventWrapper saveEvent(Messages.EventWrapper eventWrapper);
    List<Messages.EventWrapper> saveEventsBatch(List<Messages.EventWrapper> events);
    Future<Messages.EventWrapperBatch> loadEventWrappersForAggregateIdAsync(String aggregateType, String aggregateRootId, long fromJournalId);
    Future<Messages.EventWrapperBatch> loadEventWrappersForCorrelationIdAsync(String aggregateType, String correlationId, long fromJournalId);
    Messages.EventWrapperBatch loadEventWrappersForAggregateId(String aggregateType, String aggregateRootId, long fromJournalId);
}
