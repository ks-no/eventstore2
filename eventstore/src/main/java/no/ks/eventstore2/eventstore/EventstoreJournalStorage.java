package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Status;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import eventstore.*;
import eventstore.j.EventDataBuilder;
import eventstore.j.ReadStreamEventsBuilder;
import eventstore.j.SettingsBuilder;
import eventstore.j.WriteEventsBuilder;
import eventstore.tcp.ConnectionActor;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.svarut.events.EventUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static akka.pattern.Patterns.ask;

public class EventstoreJournalStorage implements JournalStorage {

    private static final Logger log = LoggerFactory.getLogger(EventstoreJournalStorage.class);

    private final ActorSystem system;

    private ActorRef connection;

    public EventstoreJournalStorage(ActorSystem system) {
        this.system = system;
    }

    @Override
    public void saveEvent(Event event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Messages.EventWrapper> saveEventsBatch(List<Messages.EventWrapper> events) {
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            if (events == null || events.isEmpty()) {
                return Collections.emptyList();
            } else if (events.size() > 500) {
                throw new RuntimeException(String.format("Max batch size is 500, but received %s", events.size()));
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
            Object result = Await.result(ask(connection, builder.build(), 15000), Duration.create(15, TimeUnit.SECONDS));

            if (result instanceof WriteEventsCompleted) {
                final WriteEventsCompleted completed = (WriteEventsCompleted) result;
                log.info("Saved {} events (range: {}, position: {}) in {}", events.size(), completed.numbersRange(), completed.position(), stopWatch);

                return readEvents(streamId, completed.numbersRange().get().start().value(), events.size());
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
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEventMetadata handleEvent) {
        List<Messages.EventWrapper> events = readEvents("$ce-" + aggregateType, 0, 500);
        for (Messages.EventWrapper event : events) {
            handleEvent.handleEvent(event);
        }
        return events.size() < 500;
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEventMetadata handleEvent, long fromKey) {
        List<Messages.EventWrapper> events = readEvents("$ce-" + aggregateType, fromKey, 500);
        for (Messages.EventWrapper event : events) {
            handleEvent.handleEvent(event);
        }
        return events.size() < 500;
    }

    private List<Messages.EventWrapper> readEvents(String streamId, long fromKey, int limit) {
        StopWatch stopWatch = StopWatch.createStarted();
        try {
            ReadStreamEventsBuilder builder = new ReadStreamEventsBuilder(streamId)
                    .resolveLinkTos(true)
                    .fromNumber(fromKey)
                    .maxCount(limit);

            Object result = Await.result(ask(connection, builder.build(), 15000), Duration.create(15, TimeUnit.SECONDS));

            if (result instanceof ReadStreamEventsCompleted) {
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
                return events;

            } else if (result instanceof Status.Failure) {
                Status.Failure failure = ((Status.Failure) result);
                throw new RuntimeException("Failure while reading events", failure.cause());
            } else {
                throw new RuntimeException(String.format("Got unknown response while reading events: %s", result.getClass()));
            }
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
    public void open() {
        final Settings settings = new SettingsBuilder()
                .address(new InetSocketAddress("127.0.0.1", 1113))
                .defaultCredentials("admin", "changeit")
                .build();

        connection = system.actorOf(ConnectionActor.getProps(settings));
    }

    @Override
    public void close() {
    }

    @Override
    public void upgradeFromOldStorage(String aggregateType, JournalStorage storage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doBackup(String backupDirectory, String backupfilename) {
        // Do nothing
    }

    @Override
    public EventBatch loadEventsForAggregateId(String aggregateType, String aggregateId, String fromJournalId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<EventBatch> loadEventsForAggregateIdAsync(String aggregateType, String aggregateId, String fromJournalId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void saveEvents(List<? extends Event> events) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Messages.EventWrapper saveEvent(Messages.EventWrapper eventWrapper) {
        return saveEventsBatch(Collections.singletonList(eventWrapper)).get(0);
    }

    @Override
    public Future<Messages.EventWrapperBatch> loadEventWrappersForAggregateIdAsync(String aggregateType, String aggregateRootId, long fromJournalId) {
        return null;
    }

    @Override
    public Future<Messages.EventWrapperBatch> loadEventWrappersForCorrelationIdAsync(String aggregateType, String correlationId, long fromJournalId) {
        throw new UnsupportedOperationException(); // TODO: Er denne i bruk?
    }

    @Override
    public Messages.EventWrapperBatch loadEventWrappersForAggregateId(String aggregateType, String aggregateRootId, long fromJournalId) {
        return null;
    }
}
