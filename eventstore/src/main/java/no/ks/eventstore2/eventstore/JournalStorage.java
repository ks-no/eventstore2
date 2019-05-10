package no.ks.eventstore2.eventstore;

import eventstore.EventNumber;
import eventstore.Messages;
import scala.Option;
import scala.concurrent.Future;

import java.util.List;
import java.util.function.Consumer;

public interface JournalStorage {
    Option<EventNumber.Range> saveEvent(Messages.EventWrapper eventWrapper);
    Option<EventNumber.Range> saveEventsBatch(List<Messages.EventWrapper> events);
    boolean loadEventsAndHandle(String category, Consumer<Messages.EventWrapper> handleEvent) throws Exception;
    boolean loadEventsAndHandle(String category, Consumer<Messages.EventWrapper> handleEvent, long fromKey) throws Exception;
    Messages.EventWrapperBatch loadEventWrappersForAggregateId(String aggregateType, String aggregateRootId, long fromJournalId) throws Exception;
    Future<Messages.EventWrapperBatch> loadEventWrappersForAggregateIdAsync(String aggregateType, String aggregateRootId, long fromJournalId);
    Future<Messages.EventWrapperBatch> loadEventWrappersForCorrelationIdAsync(String aggregateType, String correlationId, long fromJournalId);
}
