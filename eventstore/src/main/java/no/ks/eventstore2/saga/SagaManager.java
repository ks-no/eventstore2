package no.ks.eventstore2.saga;

import akka.actor.*;
import akka.cluster.pubsub.DistributedPubSubMediator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import eventstore.*;
import eventstore.j.SettingsBuilder;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TakeBackup;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.eventstore.EventMetadata;
import no.ks.eventstore2.eventstore.EventStoreUtil;
import no.ks.eventstore2.eventstore.JsonMetadataBuilder;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.reflection.HandlerFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import scala.Option;
import scala.concurrent.duration.Duration;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static no.ks.eventstore2.eventstore.EventstoreConstants.getSystemCategoryStreamId;

public class SagaManager extends UntypedActor {
    private static Logger log = LoggerFactory.getLogger(SagaManager.class);

    private static Set<String> aggregates = new HashSet<>();
    private static Map<Class<?>, ArrayList<SagaEventMapping>> eventToSagaMap = new HashMap<>();
    private Map<String, Long> latestJournalidReceived = new HashMap<>();
    private Map<String, Boolean> inSubscribe = new HashMap<>();
    private Map<ActorRef, String> subscriptions = new HashMap<>();
    private LoadingCache<SagaCompositeId, ActorRef> sagas = null;

    private final ActorRef commandDispatcher;
    private final SagaRepository repository;
    private ActorRef eventstoreConnection;
    private String packageScanPath;

    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

    private final Cancellable snapshotSchedule = getContext().system().scheduler().schedule(
            Duration.create(1, TimeUnit.HOURS),
            Duration.create(2, TimeUnit.HOURS),
            getSelf(), new TakeSnapshot(), getContext().dispatcher(), null);
    private ActorRef timeoutstore;

    public static Props mkProps(ActorSystem system, ActorRef commandDispatcher, SagaInMemoryRepository repository, ActorRef eventstoreConnection) {
        return mkProps(system, commandDispatcher, repository, eventstoreConnection, "no");
    }
    public static Props mkProps(ActorSystem system, ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstoreConnection, String packageScanPath) {
        return mkProps(system, commandDispatcher, repository, eventstoreConnection, packageScanPath, null);
    }
    public static Props mkProps(ActorSystem system, ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstoreConnection, String packageScanPath, String dispatcher) {
        Props singletonProps = Props.create(SagaManager.class, commandDispatcher, repository, eventstoreConnection, packageScanPath);
        if(dispatcher != null){
            singletonProps = singletonProps.withDispatcher(dispatcher);
        }
        return singletonProps;
    }

    public SagaManager(ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstoreConnection, String packageScanPath) {
        sagas = CacheBuilder.newBuilder().maximumSize(100000)
                .removalListener((RemovalListener<SagaCompositeId, ActorRef>) removalNotification -> {
                    log.debug("Removing actor {} because {}", removalNotification.getKey(), removalNotification.getCause());
                    removalNotification.getValue().tell(PoisonPill.getInstance(), null);
                }).build(new CacheLoader<SagaCompositeId, ActorRef>() {
                    @Override
                    public ActorRef load(SagaCompositeId k1) throws Exception {
                        return getContext().actorOf(Props.create(k1.getClz(), k1.getId(), commandDispatcher, repository));
                    }
                });



        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
        this.eventstoreConnection = eventstoreConnection;
        this.packageScanPath = packageScanPath;
    }

    @Override
    public void postStop() {
        timeoutstore.tell(PoisonPill.getInstance(), self());
        snapshotSchedule.cancel();
        repository.close();
    }

    @Override
    public void preStart() {
        registerSagas();
        repository.open();
        subscribe();
        timeoutstore = getContext().actorOf(TimeOutStore.mkProps(repository));
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        log.warn("Restarted sagamanager, restarting storage");
        repository.close();
        repository.open();
    }

    @Override
    public void onReceive(Object o) throws Exception {
        if (o instanceof LiveProcessingStarted$) {
            String aggregate = subscriptions.get(getSender());
            inSubscribe.put(aggregate, false);
            log.info("Live processing started for \"{}\"", aggregate);
        } else if (o instanceof Messages.SendAwake) {
            timeoutstore.tell(Messages.ClearAwake.newBuilder().setSagaid(((Messages.SendAwake) o).getSagaid()).build(), self());
            getOrCreateSaga((Class<? extends Saga>) Class.forName(((Messages.SendAwake) o).getSagaid().getClazz()), ((Messages.SendAwake) o).getSagaid().getId())
                    .tell("awake",self());
        } else if (o instanceof Messages.ClearAwake || o instanceof Messages.ScheduleAwake){
            timeoutstore.tell(o,sender());
        } else if (o instanceof ResolvedEvent) {
            ResolvedEvent event = (ResolvedEvent) o;
            Any anyEvent = Any.parseFrom(event.data().data().value().toArray());
            EventMetadata metadata = JsonMetadataBuilder.readMetadata(event.data().metadata().value().toArray());
            latestJournalidReceived.put(metadata.getAggregateType(), event.linkEvent().number().value());

            Set<SagaEventMapping> sagaClassesForEvent = getSagaClassesForType(metadata.getProtoSerializationType());
            for (SagaEventMapping mapping : sagaClassesForEvent) {
                String sagaId;
                if(mapping.isUseAggregateRootID()){
                    sagaId = metadata.getAggregateRootId();
                } else {
                    sagaId = (String) mapping.getPropertyMethod().invoke(ProtobufHelper.unPackAny(metadata.getProtoSerializationType(), anyEvent));
                }
                ActorRef sagaRef = getOrCreateSaga(mapping.getSagaClass(), sagaId);
                sagaRef.tell(ProtobufHelper.unPackAny(metadata.getProtoSerializationType(), anyEvent), self());
            }
        } else if (o instanceof UpgradeSagaRepoStore) {
            repository.open();
            if (repository.getState("Saga", "upgradedH2Db") != (byte) 1) {
                log.info("Upgrading sagaRepository");
                ((UpgradeSagaRepoStore) o).getSagaRepository().readAllStatesToNewRepository(repository);
                repository.saveState("Saga", "upgradedH2Db", (byte) 1);
            }
        }
        else if (o instanceof TakeBackup) {
            repository.doBackup(((TakeBackup) o).getBackupdir(), "backupSagaRepo" + FORMAT.format(new Date()));
        } else if (o instanceof Messages.AcknowledgePreviousEventsProcessed) {
            sender().tell(Messages.Success.getDefaultInstance(), self());
        } else if (o instanceof TakeSnapshot) {
            for (String aggregate : latestJournalidReceived.keySet()) {
                log.info("Saving latestJournalId {} for sagaManager aggregate {}", latestJournalidReceived.get(aggregate), aggregate);
                repository.saveLatestJournalId(aggregate, latestJournalidReceived.get(aggregate));
            }
        } else if("shutdown".equals(o)){
            log.info("shutting down sagamanager");
            removeOldActorsWithWrongState();
        }
    }

    private ActorRef getOrCreateSaga(Class<? extends Saga> clz, String sagaId) throws ExecutionException {
        SagaCompositeId compositeId = new SagaCompositeId(clz, sagaId);
        return sagas.get(compositeId);
    }

    private void registerSagas() {
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(SagaEventIdProperty.class));
        for (BeanDefinition bd : scanner.findCandidateComponents(packageScanPath)) {
            if (!bd.isAbstract()) {
                register(bd.getBeanClassName());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void register(String className) {
        try {
            handleNewStyleAnnotations((Class<? extends Saga>) Class.forName(className));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleNewStyleAnnotations(Class<? extends Saga> sagaClass) {
        Subscriber subscriberAnnotation = sagaClass.getAnnotation(Subscriber.class);
        if (subscriberAnnotation == null) {
            throw new InvalidSagaConfigurationException("Missing aggregate annotation, please annotate " + sagaClass + " with @Aggregate to specify subscribed aggregate");
        }

        registerAggregate(subscriberAnnotation.value());

        SagaEventIdProperty sagaEventIdProperty = sagaClass.getAnnotation(SagaEventIdProperty.class);

        if (sagaEventIdProperty == null) {
            throw new InvalidSagaConfigurationException("Missing @SagaEventIdProperty annotation, please annotate " + sagaClass + " with @SagaEventIdProperty to specify id-properties");
        }

        String eventPropertyMethodName = sagaEventIdProperty.value();
        Map<Class<? extends Message>, Method> eventHandlersProto = HandlerFinder.getEventHandlers(sagaClass);
        for (Class<? extends Message> eventClass : eventHandlersProto.keySet()) {
            try {
                if(sagaEventIdProperty.useAggregateRootId()){
                    registerEventToSaga(eventClass, sagaClass, sagaEventIdProperty.useAggregateRootId());
                } else {
                    Method eventPropertyMethod = eventClass.getMethod(propertyfy(eventPropertyMethodName));
                    if (!String.class.equals(eventPropertyMethod.getReturnType())) {
                        throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + "s " + eventPropertyMethodName + " eventPropertyMethod does not return String, which is required for saga " + sagaClass);
                    }
                    registerEventToSaga(eventClass, sagaClass, eventPropertyMethod);
                }
            } catch (NoSuchMethodException e) {
                throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + " does not implement the java property " + eventPropertyMethodName + " which is required for saga " + sagaClass, e);
            }
        }
    }

    private String propertyfy(String propName) {
        return propName == null || propName.length() < 1 ? null : "get" + propName.substring(0, 1).toUpperCase() + propName.substring(1);

    }

    private void registerAggregate(String aggregate) {
        registerAggregates(new String[]{aggregate});
    }

    private void registerAggregates(String[] aggregates) {
        Collections.addAll(SagaManager.aggregates, aggregates);
    }

    private Set<SagaEventMapping> getSagaClassesForType(String protoSerializationType) {
        Set<SagaEventMapping> handlingSagas = new HashSet<>();

        Class<?> clazz = ProtobufHelper.getClassForSerialization(protoSerializationType);

        if (eventToSagaMap.containsKey(clazz)) {
            handlingSagas.addAll(eventToSagaMap.get(clazz));
        }

        return handlingSagas;
    }

    private void registerEventToSaga(Class<? extends Message> eventClass, Class<? extends Saga> sagaClass, Method propertyMethod) {
        eventToSagaMap.putIfAbsent(eventClass, new ArrayList<>());
        eventToSagaMap.get(eventClass).add(new SagaEventMapping(sagaClass, propertyMethod));
    }

    private void registerEventToSaga(Class<? extends Message> eventClass, Class<? extends Saga> sagaClass, boolean useAggregateRootID) {
        eventToSagaMap.putIfAbsent(eventClass, new ArrayList<>());
        eventToSagaMap.get(eventClass).add(new SagaEventMapping(sagaClass, useAggregateRootID));
    }

    private void subscribe() {
        if (inSubscribe.size() == 0) {
            for (String aggregate : aggregates) {
                subscribeForAggregate(aggregate);
                inSubscribe.put(aggregate, true);
            }
        }
    }

    private void subscribeForAggregate(String aggregate) {
        log.debug("Starting subscription for \"{}\"", aggregate);
        latestJournalidReceived.put(aggregate, repository.loadLatestJournalID(aggregate));
        ActorRef subscription = getContext().actorOf(EventStoreUtil.getCategorySubscriptionsProps(
                eventstoreConnection,
                getSelf(),
                aggregate,
                latestJournalidReceived.get(aggregate)));
        subscriptions.put(subscription, aggregate);
    }

    private void removeOldActorsWithWrongState() {
        sagas.invalidateAll();
    }

    public boolean isLive() {
        return inSubscribe.values().stream().noneMatch(t -> t);
    }
}
