package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;
import akka.actor.Status;
import akka.dispatch.Futures;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import eventstore.*;
import eventstore.j.EventDataBuilder;
import eventstore.j.ReadStreamEventsBuilder;
import eventstore.j.WriteEventsBuilder;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.svarut.events.EventUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.Duration;
import scala.runtime.AbstractFunction1;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static akka.pattern.Patterns.ask;

public class EventstoreJournalStorage implements JournalStorage {

    private static final Logger log = LoggerFactory.getLogger(EventstoreJournalStorage.class);

    private int eventLimit = 500;
    private static final int EVENTSTORE_TIMEOUT_MILLIS = 15000;

    private final ActorRef connection;
    private final ExecutionContextExecutor context;

    public EventstoreJournalStorage(ActorRef connection, ExecutionContextExecutor context) {
        this.connection = connection;
        this.context = context;
    }

    public EventstoreJournalStorage(ActorRef connection, ExecutionContextExecutor context, int eventLimit) {
        this.connection = connection;
        this.context = context;
        this.eventLimit = eventLimit;
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }

    @Override
    public Messages.EventWrapper saveEvent(Messages.EventWrapper eventWrapper) {
        return saveEventsBatch(Collections.singletonList(eventWrapper)).get(0);
    }

    @Override
    public List<Messages.EventWrapper> saveEventsBatch(List<Messages.EventWrapper> events) {
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            if (events == null || events.isEmpty()) {
                return Collections.emptyList();
            } else if (events.size() > eventLimit) {
                throw new RuntimeException(String.format("Max batch size is %s, but received %s", eventLimit, events.size()));
            }

            Messages.EventWrapper firstEvent = events.get(0);

            if (!events.stream().allMatch(e -> e.getAggregateRootId().equals(firstEvent.getAggregateRootId()))) {
                throw new RuntimeException("Can't save batch with multiple aggregate root ids");
            }

            String streamId = EventUtil.getStreamId(
                    ProtobufHelper.unPackAny(firstEvent.getProtoSerializationType(), firstEvent.getEvent()),
                    firstEvent.getAggregateRootId());
            WriteEventsBuilder builder = new WriteEventsBuilder(streamId);

            if (firstEvent.getVersion() != -1) {
                builder.expectVersion(firstEvent.getVersion());
            }

            events.forEach(event -> {
                Message message = ProtobufHelper.unPackAny(event.getProtoSerializationType(), event.getEvent());
                builder.addEvent(new EventDataBuilder(EventUtil.getEventType(message))
                        .data(event.getEvent().toByteArray())
                        .jsonMetadata(JsonMetadataBuilder.build(event, event.getAggregateType()))
                        .eventId(UUID.randomUUID())
                        .build());
            });
            Object result = Await.result(ask(connection, builder.build(), EVENTSTORE_TIMEOUT_MILLIS), Duration.create(EVENTSTORE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));

            if (result instanceof WriteEventsCompleted) {
                final WriteEventsCompleted completed = (WriteEventsCompleted) result;
                log.info("Saved {} events (range: {}, position: {}) in {}", events.size(), completed.numbersRange(), completed.position(), stopWatch);

                return Await.result(
                        readEvents(streamId, completed.numbersRange().get().start().value(), events.size()),
                        Duration.apply(EVENTSTORE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
            } else if (result instanceof Status.Failure) {
                final Status.Failure failure = ((Status.Failure) result);
                throw new RuntimeException("Failure while saving events", failure.cause());
            } else {
                throw new RuntimeException(String.format("Got unknown response while saving events: %s", result.getClass()));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean loadEventsAndHandle(String category, Consumer<Messages.EventWrapper> handleEvent) throws Exception {
        List<Messages.EventWrapper> events = Await.result(
                readEvents("$ce-" + category, 0, eventLimit),
                Duration.apply(EVENTSTORE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        for (Messages.EventWrapper event : events) {
            handleEvent.accept(event);
        }
        return events.size() < eventLimit;
    }

    @Override
    public boolean loadEventsAndHandle(String category, Consumer<Messages.EventWrapper> handleEvent, long fromKey) throws Exception {
        List<Messages.EventWrapper> events = Await.result(
                readEvents("$ce-" + category, fromKey, eventLimit),
                Duration.apply(EVENTSTORE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        for (Messages.EventWrapper event : events) {
            handleEvent.accept(event);
        }
        return events.size() < eventLimit;
    }

    private Future<List<Messages.EventWrapper>> readEvents(String streamId, long fromKey, int limit) {
        StopWatch stopWatch = StopWatch.createStarted();
        try {
            ReadStreamEventsBuilder builder = new ReadStreamEventsBuilder(streamId)
                    .resolveLinkTos(true)
                    .fromNumber(fromKey)
                    .maxCount(limit);

            Promise<List<Messages.EventWrapper>> promise = Futures.promise();
            Future<Object> ask = ask(connection, builder.build(), EVENTSTORE_TIMEOUT_MILLIS);
            ask.onSuccess(new OnSuccess<Object>() {
                @Override
                public void onSuccess(Object result) {
                    ReadStreamEventsCompleted completed = (ReadStreamEventsCompleted) result;
                    List<Messages.EventWrapper> events = completed.eventsJava().stream()
                            .map(e -> {
                                try {
                                    return JsonMetadataBuilder.readMetadataAsWrapper(e.data().metadata().value().toArray())
                                            .setVersion(e.number().value()) // Event number of aggregate stream
                                            .setJournalid(getEventJournalId(e)) // Event number of link event, or -1
                                            .setEvent(Any.parseFrom(e.data().data().value().toArray()))
                                            .build();
                                } catch (InvalidProtocolBufferException e1) {
                                    throw new RuntimeException(e1);
                                }
                            })
                            .collect(Collectors.toList());
                    log.info("Read {} events from \"{}\" (fromKey: {}, limit: {}) in {}", events.size(), streamId, fromKey, limit, stopWatch);
                    promise.success(events);
                }
            }, context);
            ask.onFailure(new OnFailure() {
                @Override
                public void onFailure(Throwable throwable) {
                    promise.failure(new RuntimeException("Failure while reading events", throwable));
                }
            }, context);

            return promise.future();
        } catch (StreamNotFoundException e) {
            log.warn("Got StreamNotFoundException while reading events, returning empty list");
            return Futures.<List<Messages.EventWrapper>>promise().success(Collections.emptyList()).future();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long getEventJournalId(eventstore.Event event) {
        if (event instanceof ResolvedEvent) {
            return ((ResolvedEvent) event).linkEvent().number().value();
        }
        return -1;
    }

    @Override
    public Messages.EventWrapperBatch loadEventWrappersForAggregateId(String aggregateType, String aggregateRootId, long fromJournalId) throws Exception {
        log.debug("Loading events for type \"{}\" and aggregate id \"{}\" from {}", aggregateType, aggregateRootId, fromJournalId);
        List<Messages.EventWrapper> events = Await.result(
                readEvents(aggregateType + "-" + aggregateRootId, fromJournalId, eventLimit),
                Duration.apply(EVENTSTORE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        return Messages.EventWrapperBatch.newBuilder()
                .setAggregateType(aggregateType)
                .setAggregateRootId(aggregateRootId)
                .addAllEvents(events)
                .setReadAllEvents(events.size() != eventLimit)
                .build();
    }

    @Override
    public Future<Messages.EventWrapperBatch> loadEventWrappersForAggregateIdAsync(String aggregateType, String aggregateRootId, long fromJournalId) {
        log.debug("Loading events async for type \"{}\" and aggregate id \"{}\" from {}", aggregateType, aggregateRootId, fromJournalId);
        return readEvents(aggregateType + "-" + aggregateRootId, fromJournalId, eventLimit)
                .map(new AbstractFunction1<List<Messages.EventWrapper>, Messages.EventWrapperBatch>() {
                    @Override
                    public Messages.EventWrapperBatch apply(List<Messages.EventWrapper> eventWrappers) {
                        return Messages.EventWrapperBatch.newBuilder()
                                .setAggregateType(aggregateType)
                                .setAggregateRootId(aggregateRootId)
                                .addAllEvents(eventWrappers)
                                .setReadAllEvents(eventWrappers.size() != eventLimit)
                                .build();
                    }
                }, context);
    }

    @Override
    public Future<Messages.EventWrapperBatch> loadEventWrappersForCorrelationIdAsync(String aggregateType, String correlationId, long fromJournalId) {
        log.debug("Loading events async for type \"{}\" and correlation id \"{}\" from {}", aggregateType, correlationId, fromJournalId);
        return readEvents("$bc-" + correlationId, fromJournalId, eventLimit)
                .map(new AbstractFunction1<List<Messages.EventWrapper>, Messages.EventWrapperBatch>() {
                    @Override
                    public Messages.EventWrapperBatch apply(List<Messages.EventWrapper> eventWrappers) {
                        return Messages.EventWrapperBatch.newBuilder()
                                .addAllEvents(eventWrappers)
                                .setReadAllEvents(eventWrappers.size() != eventLimit)
                                .build();
                    }
                }, context);
    }
}
