package no.ks.eventstore2.saga;

import akka.ConfigurationException;
import akka.actor.*;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import com.google.protobuf.Message;
import eventstore.Messages;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.TakeBackup;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.eventstore.*;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.reflection.HandlerFinder;
import no.ks.eventstore2.reflection.HandlerFinderProtobuf;
import no.ks.eventstore2.response.Success;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import scala.concurrent.duration.Duration;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SagaManager extends UntypedActor {
    private static Logger log = LoggerFactory.getLogger(SagaManager.class);

    private final ActorRef commandDispatcher;
    private final SagaRepository repository;
    private ActorRef eventstore;
    private String packageScanPath;


    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

    private Map<SagaCompositeId, ActorRef> sagas = new HashMap<SagaCompositeId, ActorRef>();

    private Map<String, Long> latestJournalidReceived = new HashMap<>();
    private Map<String, Boolean> inSubscribe = new HashMap<>();

    private final Cancellable snapshotSchedule = getContext().system().scheduler().schedule(
            Duration.create(1, TimeUnit.HOURS),
            Duration.create(2, TimeUnit.HOURS),
            getSelf(), new TakeSnapshot(), getContext().dispatcher(), null);

    public static Props mkProps(ActorSystem system, ActorRef commandDispatcher, SagaInMemoryRepository repository, ActorRef eventStore) {
        return mkProps(system, commandDispatcher, repository, eventStore, "no");
    }
    public static Props mkProps(ActorSystem system, ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstore, String packageScanPath) {
        return mkProps(system, commandDispatcher, repository, eventstore, packageScanPath, null);
    }
    public static Props mkProps(ActorSystem system, ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstore, String packageScanPath, String dispatcher) {
        Props singletonProps = Props.create(SagaManager.class, commandDispatcher, repository, eventstore, packageScanPath);
        if(dispatcher != null){
            singletonProps = singletonProps.withDispatcher(dispatcher);
        }
        try {
            final ClusterSingletonManagerSettings settings =
                    ClusterSingletonManagerSettings.create(system);

            system.actorOf(ClusterSingletonManager.props(
                    singletonProps,
                    "shutdown", settings), "sagamanagersingelton");

            ClusterSingletonProxySettings proxySettings =
                    ClusterSingletonProxySettings.create(system);
            proxySettings.withBufferSize(10000);

            return ClusterSingletonProxy.props("/user/eventstoresingelton", proxySettings);
        } catch (ConfigurationException e) {
            log.info("not cluster system");
            return singletonProps;
        }

    }

    public SagaManager(ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstore, String packageScanPath) {
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
        this.eventstore = eventstore;
        this.packageScanPath = packageScanPath;
    }

    @Override
    public void postStop() {
        snapshotSchedule.cancel();
        repository.close();
    }

    @Override
    public void preStart() {
        registerSagas();
        try {
            ActorRef mediator =
                    DistributedPubSub.get(getContext().system()).mediator();

            mediator.tell(new DistributedPubSubMediator.Subscribe(EventStore.EVENTSTOREMESSAGES, getSelf()),
                    getSelf());
        } catch (ConfigurationException e){
            log.info("Not subscribing to eventstore event, no cluster system");
        }
        repository.open();
        subscribe();
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
        if (log.isDebugEnabled() && o instanceof Event) {
            log.debug("SagaManager received event {}", o);
        }
        if (o instanceof Event) {
            final String journalid = ((Event) o).getJournalid();
            latestJournalidReceived.put(((Event) o).getAggregateType(), Long.valueOf(journalid != null? journalid : "0"));
        }
        if (o instanceof Event) {
            log.debug("SagaManager processing event {}", o);
            Event event = (Event) o;
            Set<SagaEventMapping> sagaClassesForEvent = getSagaClassesForEvent(event.getClass());
            for (SagaEventMapping mapping : sagaClassesForEvent) {
                String sagaId = (String) mapping.getPropertyMethod().invoke(event);
                ActorRef sagaRef = getOrCreateSaga(mapping.getSagaClass(), sagaId);
                sagaRef.tell(event, self());
            }
        } else if(o instanceof Messages.EventWrapper){
            latestJournalidReceived.put(((Messages.EventWrapper) o).getAggregateType(), ((Messages.EventWrapper) o).getJournalid());
            log.debug("SagaManager processing event {}", o);
            Set<SagaEventMapping> sagaClassesForEvent = getSagaClassesForEventWrapper((Messages.EventWrapper)o);
            for (SagaEventMapping mapping : sagaClassesForEvent) {
                String sagaId;
                if(mapping.isUseAggregateRootID()){
                    sagaId = ((Messages.EventWrapper) o).getAggregateRootId();
                } else {
                    sagaId = (String) mapping.getPropertyMethod().invoke(ProtobufHelper.unPackAny(((Messages.EventWrapper) o).getProtoSerializationType(), ((Messages.EventWrapper) o).getEvent()));
                }
                ActorRef sagaRef = getOrCreateSaga(mapping.getSagaClass(), sagaId);
                sagaRef.tell(o, self());
            }

        } else if (o instanceof NewEventstoreStarting) {
            subscribe();
        } else if (o instanceof UpgradeSagaRepoStore) {
            repository.open();
            if (repository.getState("Saga", "upgradedH2Db") != (byte) 1) {
                log.info("Upgrading sagaRepository");
                ((UpgradeSagaRepoStore) o).getSagaRepository().readAllStatesToNewRepository(repository);
                repository.saveState("Saga", "upgradedH2Db", (byte) 1);
            }
        } else if (o instanceof Messages.IncompleteSubscriptionPleaseSendNew) {
            String aggregateType = ((Messages.IncompleteSubscriptionPleaseSendNew) o).getAggregateType();
            log.debug("Sending new subscription on '{}' from latest journalid '{}'", aggregateType, latestJournalidReceived);
            if (latestJournalidReceived.get(aggregateType) == null) {
                throw new RuntimeException("Missing latestJournalidReceived but got IncompleteSubscriptionPleaseSendNew");
            }
            Messages.Subscription subscription = Messages.Subscription.newBuilder()
                    .setAggregateType(aggregateType)
                    .setFromJournalId(latestJournalidReceived.get(aggregateType)).build();
            eventstore.tell(subscription, self());
        } else if (o instanceof IncompleteSubscriptionPleaseSendNew) {
            String aggregateType = ((IncompleteSubscriptionPleaseSendNew) o).getAggregateType();
            log.debug("Sending new subscription on '{}' from latest journalid '{}'", aggregateType, latestJournalidReceived);
            if (latestJournalidReceived.get(aggregateType) == null) {
                throw new RuntimeException("Missing latestJournalidReceived but got IncompleteSubscriptionPleaseSendNew");
            }
            Messages.Subscription subscription = Messages.Subscription.newBuilder()
                    .setAggregateType(aggregateType)
                    .setFromJournalId(latestJournalidReceived.get(aggregateType)).build();
            eventstore.tell(subscription, self());
        }
        else if (o instanceof TakeBackup) {
                repository.doBackup(((TakeBackup) o).getBackupdir(), "backupSagaRepo" + format.format(new Date()));
        } else if (o instanceof AcknowledgePreviousEventsProcessed) {
                sender().tell(new Success(), self());
        } else if (o instanceof Messages.AcknowledgePreviousEventsProcessed) {
            sender().tell(Messages.Success.getDefaultInstance(), self());
        } else if (o instanceof TakeSnapshot) {
            for (String aggregate : latestJournalidReceived.keySet()) {
                log.info("Saving latestJournalId {} for sagaManager aggregate {}", latestJournalidReceived.get(aggregate), aggregate);
                repository.saveLatestJournalId(aggregate, latestJournalidReceived.get(aggregate));
            }
        } else if (o instanceof DistributedPubSubMediator.SubscribeAck){
            log.info("Subscribing for eventstore restartmessages");
        } else if( o instanceof Messages.CompleteSubscriptionRegistered) {
            inSubscribe.remove(((Messages.CompleteSubscriptionRegistered) o).getAggregateType());
        } else if( o instanceof CompleteSubscriptionRegistered) {
            inSubscribe.remove(((CompleteSubscriptionRegistered) o).getAggregateType());
        } else if("shutdown".equals(o)){
            log.info("shutting down sagamanager");
            removeOldActorsWithWrongState();
        }
    }

    private ActorRef getOrCreateSaga(Class<? extends Saga> clz, String sagaId) {
        SagaCompositeId compositeId = new SagaCompositeId(clz, sagaId);
        if (!sagas.containsKey(compositeId)) {
            ActorRef sagaRef = getContext().actorOf(Props.create(clz, sagaId, commandDispatcher, repository));
            sagas.put(compositeId, sagaRef);
        }
        return sagas.get(compositeId);
    }

    private static Set<String> aggregates = new HashSet<String>();

    private static Map<Class<?>, ArrayList<SagaEventMapping>> eventToSagaMap = new HashMap<>();

    private void registerSagas() {
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(ListensTo.class));
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
            Class<? extends Saga> sagaClass = (Class<? extends Saga>) Class.forName(className);

            boolean oldStyle = handleOldStyleAnnotations(sagaClass);

            if (!oldStyle) {
                handleNewStyleAnnotations(sagaClass);
            }
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
        Map<Class<? extends Event>, Method> eventHandlers = HandlerFinder.getEventHandlers(sagaClass);
        for (Class<? extends Event> eventClass : eventHandlers.keySet()) {
            try {
                Method eventPropertyMethod = null;
                if(sagaEventIdProperty.useAggregateRootId()){
                    eventPropertyMethod = eventClass.getMethod(propertyfy("aggregateRootId"));
                } else {
                    eventPropertyMethod = eventClass.getMethod(propertyfy(eventPropertyMethodName));
                }
                if (!String.class.equals(eventPropertyMethod.getReturnType())) {
                    throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + "s " + eventPropertyMethodName + " eventPropertyMethod does not return String, which is required for saga " + sagaClass);
                }
                registerEventToSaga(eventClass, sagaClass, eventPropertyMethod);
            } catch (NoSuchMethodException e) {
                throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + " does not implement the java property " + eventPropertyMethodName + " which is required for saga " + sagaClass, e);
            }
        }

        Map<Class<? extends Message>, Method> eventHandlersProto = HandlerFinderProtobuf.getEventHandlers(sagaClass);
        for (Class<? extends Message> eventClass : eventHandlersProto.keySet()) {
            try {
                if(sagaEventIdProperty.useAggregateRootId()){
                    registerEventProtoToSaga(eventClass, sagaClass, sagaEventIdProperty.useAggregateRootId());
                } else {
                    Method eventPropertyMethod = eventClass.getMethod(propertyfy(eventPropertyMethodName));
                    if (!String.class.equals(eventPropertyMethod.getReturnType())) {
                        throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + "s " + eventPropertyMethodName + " eventPropertyMethod does not return String, which is required for saga " + sagaClass);
                    }
                    registerEventProtoToSaga(eventClass, sagaClass, eventPropertyMethod);
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

    private boolean handleOldStyleAnnotations(Class<? extends Saga> sagaClass) throws IntrospectionException {
        ListensTo annotation = sagaClass.getAnnotation(ListensTo.class);
        if (null != annotation) {
            registerAggregates(annotation.aggregates());
            for (EventIdBind eventIdBind : annotation.value()) {
                Method getter = new PropertyDescriptor(eventIdBind.idProperty(), eventIdBind.eventClass()).getReadMethod();
                registerEventToSaga(eventIdBind.eventClass(), sagaClass, getter);
            }
            return true;
        } else {
            return false;
        }
    }

    private void registerAggregates(String[] aggregates) {
        Collections.addAll(SagaManager.aggregates, aggregates);
    }

    private Set<SagaEventMapping> getSagaClassesForEventWrapper(Messages.EventWrapper o) {
        Set<SagaEventMapping> handlingSagas = new HashSet<SagaEventMapping>();

        Class<?> clazz = ProtobufHelper.getClassForSerialization(o.getProtoSerializationType());

        if (eventToSagaMap.containsKey(clazz)) {
            handlingSagas.addAll(eventToSagaMap.get(clazz));
        }

        return handlingSagas;
    }

    private Set<SagaEventMapping> getSagaClassesForEvent(Class<? extends Event> eventClass) {
        Set<SagaEventMapping> handlingSagas = new HashSet<SagaEventMapping>();

        Class<?> clazz = eventClass;

        while (clazz != Object.class) {
            if (eventToSagaMap.containsKey(clazz)) {
                handlingSagas.addAll(eventToSagaMap.get(clazz));
            }
            clazz = clazz.getSuperclass();
        }

        return handlingSagas;
    }

    private void registerEventToSaga(Class<? extends Event> eventClass, Class<? extends Saga> sagaClass, Method propertyMethod) {
        if (eventToSagaMap.get(eventClass) == null) {
            eventToSagaMap.put(eventClass, new ArrayList<>());
        }
        eventToSagaMap.get(eventClass).add(new SagaEventMapping(sagaClass, propertyMethod));
    }

    private void registerEventProtoToSaga(Class<? extends Message> eventClass, Class<? extends Saga> sagaClass, Method propertyMethod) {
        if (eventToSagaMap.get(eventClass) == null) {
            eventToSagaMap.put(eventClass, new ArrayList<>());
        }
        eventToSagaMap.get(eventClass).add(new SagaEventMapping(sagaClass, propertyMethod));
    }

    private void registerEventProtoToSaga(Class<? extends Message> eventClass, Class<? extends Saga> sagaClass, boolean useAggregateRootID) {
        if (eventToSagaMap.get(eventClass) == null) {
            eventToSagaMap.put(eventClass, new ArrayList<>());
        }
        eventToSagaMap.get(eventClass).add(new SagaEventMapping(sagaClass, useAggregateRootID));
    }

    private void subscribe() {
        if(inSubscribe.size() == 0) {
            for (String aggregate : aggregates) {
                latestJournalidReceived.put(aggregate, Long.valueOf(repository.loadLatestJournalID(aggregate)));
                log.info("SagaManager loaded aggregate {} latestJournalid {}", aggregate, latestJournalidReceived.get(aggregate));
            }
            for (String aggregate : aggregates) {
                inSubscribe.put(aggregate, true);
                eventstore.tell(Messages.Subscription.newBuilder().setAggregateType(aggregate).setFromJournalId(latestJournalidReceived.get(aggregate)).build(), self());
            }
        }
    }

    private void removeOldActorsWithWrongState() {
        for (SagaCompositeId sagaCompositeId : sagas.keySet()) {
            log.debug("Removing actor {}", sagas.get(sagaCompositeId).path());
            sagas.get(sagaCompositeId).tell(PoisonPill.getInstance(), null);
        }
        sagas = new HashMap<SagaCompositeId, ActorRef>();
    }
}
