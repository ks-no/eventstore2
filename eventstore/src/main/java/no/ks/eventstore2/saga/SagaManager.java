package no.ks.eventstore2.saga;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.ClusterEvent;
import no.ks.eventstore2.AkkaClusterInfo;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.TakeBackup;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.eventstore.AcknowledgePreviousEventsProcessed;
import no.ks.eventstore2.eventstore.EventstoreRestarting;
import no.ks.eventstore2.eventstore.IncompleteSubscriptionPleaseSendNew;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.reflection.HandlerFinder;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SagaManager extends UntypedActor {
    private static Logger log = LoggerFactory.getLogger(SagaManager.class);

    private final ActorRef commandDispatcher;
    private final SagaRepository repository;
    private ActorRef eventstore;
    private String packageScanPath;

    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

    private Map<SagaCompositeId, ActorRef> sagas = new HashMap<SagaCompositeId, ActorRef>();
    private AkkaClusterInfo akkaClusterInfo;
    private Map<String, String> latestJournalidReceived = new HashMap<String, String>();

    private final Cancellable snapshotSchedule = getContext().system().scheduler().schedule(
            Duration.create(1, TimeUnit.HOURS),
            Duration.create(2, TimeUnit.HOURS),
            getSelf(), new TakeSnapshot(), getContext().dispatcher(), null);

    public static Props mkProps(ActorRef commandDispatcher, SagaInMemoryRepository repository, ActorRef eventStore) {
        return mkProps(commandDispatcher, repository, eventStore, "no");
    }

    public static Props mkProps(ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstore, String packageScanPath) {
        return Props.create(SagaManager.class, commandDispatcher, repository, eventstore, packageScanPath);
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
        akkaClusterInfo = new AkkaClusterInfo(getContext().system());
        akkaClusterInfo.subscribeToClusterEvents(self());
        updateLeaderState(null);

    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        log.warn("Restarted sagamanager, restarting storage");
        repository.close();
        if (akkaClusterInfo.isLeader()) {
            // sleep so we are reasonably sure the other node has closed the storage
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
            repository.open();
        }
    }

    @Override
    public void onReceive(Object o) throws Exception {
        if (log.isDebugEnabled() && o instanceof Event) {
            log.debug("SagaManager received event {} is leader {}", o, akkaClusterInfo.isLeader());
        }
        if (o instanceof Event) {
            latestJournalidReceived.put(((Event) o).getAggregateType(), ((Event) o).getJournalid());
        }
        if (o instanceof Event && akkaClusterInfo.isLeader()) {
            log.debug("SagaManager processing event {}", o);
            Event event = (Event) o;
            Set<SagaEventMapping> sagaClassesForEvent = getSagaClassesForEvent(event.getClass());
            for (SagaEventMapping mapping : sagaClassesForEvent) {
                String sagaId = (String) mapping.getPropertyMethod().invoke(event);
                ActorRef sagaRef = getOrCreateSaga(mapping.getSagaClass(), sagaId);
                sagaRef.tell(event, self());
            }
        } else if (o instanceof EventstoreRestarting) {
            updateLeaderState(null);
        } else if (o instanceof ClusterEvent.LeaderChanged) {
            updateLeaderState((ClusterEvent.LeaderChanged) o);
        } else if (o instanceof UpgradeSagaRepoStore && akkaClusterInfo.isLeader()) {
            repository.open();
            if (repository.getState("Saga", "upgradedH2Db") != (byte) 1) {
                log.info("Upgrading sagaRepository");
                ((UpgradeSagaRepoStore) o).getSagaRepository().readAllStatesToNewRepository(repository);
                repository.saveState("Saga", "upgradedH2Db", (byte) 1);
            }
        } else if (o instanceof IncompleteSubscriptionPleaseSendNew) {
            String aggregateType = ((IncompleteSubscriptionPleaseSendNew) o).getAggregateType();
            log.debug("Sending new subscription on '{}' from latest journalid '{}'", aggregateType, latestJournalidReceived);
            if (latestJournalidReceived.get(aggregateType) == null) {
                throw new RuntimeException("Missing latestJournalidReceived but got IncompleteSubscriptionPleaseSendNew");
            }
            Subscription subscription = new Subscription(aggregateType, latestJournalidReceived.get(aggregateType));
            eventstore.tell(subscription, self());
        } else if (o instanceof TakeBackup) {
            if (akkaClusterInfo.isLeader()) {
                repository.doBackup(((TakeBackup) o).getBackupdir(), "backupSagaRepo" + format.format(new Date()));
            }
        } else if (o instanceof AcknowledgePreviousEventsProcessed) {
            if (akkaClusterInfo.isLeader()) {
                sender().tell(new Success(), self());
            } else {
                getLeaderSagaManager().tell(o, sender());
            }
        } else if (o instanceof TakeSnapshot && akkaClusterInfo.isLeader()) {
            for (String aggregate : latestJournalidReceived.keySet()) {
                log.info("Saving latestJournalId {} for sagaManager aggregate {}", latestJournalidReceived.get(aggregate), aggregate);
                repository.saveLatestJournalId(aggregate, latestJournalidReceived.get(aggregate));
            }
        }
    }

    private ActorRef getLeaderSagaManager() {
        return getContext().actorFor(akkaClusterInfo.getLeaderAdress() + "/user/sagaManager");
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

    private static Map<Class<? extends Event>, ArrayList<SagaEventMapping>> eventToSagaMap = new HashMap<Class<? extends Event>, ArrayList<SagaEventMapping>>();

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
                Method eventPropertyMethod = eventClass.getMethod(propertyfy(eventPropertyMethodName));
                if (!String.class.equals(eventPropertyMethod.getReturnType())) {
                    throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + "s " + eventPropertyMethodName + " eventPropertyMethod does not return String, which is required for saga " + sagaClass);
                }
                registerEventToSaga(eventClass, sagaClass, eventPropertyMethod);
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
            eventToSagaMap.put(eventClass, new ArrayList<SagaEventMapping>());
        }
        eventToSagaMap.get(eventClass).add(new SagaEventMapping(sagaClass, propertyMethod));
    }

    private void updateLeaderState(ClusterEvent.LeaderChanged leaderChanged) {
        try {
            boolean oldLeader = akkaClusterInfo.isLeader();
            akkaClusterInfo.updateLeaderState(leaderChanged);
            if (oldLeader && !akkaClusterInfo.isLeader()) {
                removeOldActorsWithWrongState();
            }
            if (akkaClusterInfo.isLeader()) {
                log.info("Opening repository for sagaManager");
                // sleep so we are reasonably sure the other node has closed the storage
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
                repository.open();
                for (String aggregate : aggregates) {
                    latestJournalidReceived.put(aggregate, repository.loadLatestJournalID(aggregate));
                    log.info("SagaManager loaded aggregate {} latestJournalid {}", aggregate, latestJournalidReceived.get(aggregate));
                }
                for (String aggregate : aggregates) {
                    eventstore.tell(new Subscription(aggregate, latestJournalidReceived.get(aggregate)), self());
                }
            } else {
                log.info("Closing repository for sagaManager");
                repository.close();
            }
        } catch (ConfigurationException e) {
            log.debug("Not cluster system");
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
