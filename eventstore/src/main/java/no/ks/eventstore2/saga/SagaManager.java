package no.ks.eventstore2.saga;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.ClusterEvent;
import no.ks.eventstore2.AkkaClusterInfo;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.reflection.HandlerFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;

public class SagaManager extends UntypedActor {

	static final Logger log = LoggerFactory.getLogger(SagaManager.class);

    private final ActorRef commandDispatcher;
    private final SagaRepository repository;
    private ActorRef eventstore;
    private String packageScanPath;

    private Map<SagaCompositeId, ActorRef> sagas = new HashMap<SagaCompositeId, ActorRef>();
    private AkkaClusterInfo akkaClusterInfo;

    public SagaManager(ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstore, String packageScanPath) {
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
        this.eventstore = eventstore;
        this.packageScanPath = packageScanPath;
    }


    @Override
    public void preStart() {
        registerSagas();
        akkaClusterInfo = new AkkaClusterInfo(getContext().system());
        akkaClusterInfo.subscribeToClusterEvents(self());
        updateLeaderState(null);
        for (String aggregate : aggregates) {
            eventstore.tell(new Subscription(aggregate), self());
        }
    }

    @Override
    public void onReceive(Object o) throws Exception {
		if(log.isDebugEnabled() && o instanceof  Event)
			log.debug("Sagamanager Received Event {} is leader {}", o, akkaClusterInfo.isLeader());
        if (o instanceof Event && akkaClusterInfo.isLeader()){
			log.debug("Sagamanager processing Event {}", o);
            Event event = (Event) o;
            for (Class<? extends Saga> clz : getSagaClassesForEvent(event.getClass())) {
                String sagaId = (String) propertyMap.get(new SagaEventId(clz, event.getClass())).invoke(event);
                ActorRef sagaRef = getOrCreateSaga(clz, sagaId);
                sagaRef.tell(event, self());
            }
        } else if( o instanceof ClusterEvent.LeaderChanged){
			updateLeaderState((ClusterEvent.LeaderChanged)o);
		}
    }

    private ActorRef getOrCreateSaga(Class<? extends Saga> clz, String sagaId) {
        SagaCompositeId compositeId = new SagaCompositeId(clz, sagaId);
        if (!sagas.containsKey(compositeId)){
            ActorRef sagaRef = getContext().actorOf(new Props(new SagaFactory(clz, commandDispatcher, repository, sagaId)));
            sagas.put(compositeId, sagaRef);
        }
        return sagas.get(compositeId);
    }

    private static Set<String> aggregates = new HashSet<String>();
    private static Map<SagaEventId, Method> propertyMap = new HashMap<SagaEventId, Method>();
    private static Map<Class<? extends Event>, ArrayList<Class<? extends Saga>>> eventToSagaMap = new HashMap<Class<? extends Event>, ArrayList<Class<? extends Saga>>>();

    private void registerSagas(){
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(ListensTo.class));
        scanner.addIncludeFilter(new AnnotationTypeFilter(SagaEventIdProperty.class));
        for (BeanDefinition bd : scanner.findCandidateComponents(packageScanPath))
            if (!bd.isAbstract())
                register(bd.getBeanClassName());
    }

    @SuppressWarnings("unchecked")
    private void register(String className) {
        try {
            Class<? extends Saga> sagaClass = (Class<? extends Saga>) Class.forName(className);

            boolean oldStyle = handleOldStyleAnnotations(sagaClass);

            if (!oldStyle)
                handleNewStyleAnnotations(sagaClass);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleNewStyleAnnotations(Class<? extends Saga> sagaClass) {
        Subscriber subscriberAnnotation = sagaClass.getAnnotation(Subscriber.class);
        if (subscriberAnnotation == null)
            throw new InvalidSagaConfigurationException("Missing aggregate annotation, please annotate " + sagaClass + " with @Aggregate to specify subscribed aggregate");

        registerAggregate(subscriberAnnotation.value());

        SagaEventIdProperty sagaEventIdProperty = sagaClass.getAnnotation(SagaEventIdProperty.class);

        if (sagaEventIdProperty == null)
            throw new InvalidSagaConfigurationException("Missing @SagaEventIdProperty annotation, please annotate " + sagaClass + " with @SagaEventIdProperty to specify id-properties");

        String eventPropertyMethodName = sagaEventIdProperty.value();
        HashMap<Class<? extends Event>, Method> eventHandlers = HandlerFinder.getEventHandlers(sagaClass);
        for (Class<? extends Event> eventClass : eventHandlers.keySet()) {
            try {
                Method eventPropertyMethod = eventClass.getMethod(propertyfy(eventPropertyMethodName));
                if (!String.class.equals(eventPropertyMethod.getReturnType()))
                    throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + "s " + eventPropertyMethodName + " eventPropertyMethod does not return String, which is required for saga " + sagaClass);
                registerEventToSaga(eventClass, sagaClass);
                registerSagaIdPropertyMethod(sagaClass, eventClass, eventPropertyMethod);

            } catch (NoSuchMethodException e) {
                throw new InvalidSagaConfigurationException("Event " + eventClass.getName() + " does not implement the java property " + eventPropertyMethodName + " which is required for saga " + sagaClass, e);
            }
        }

    }

    private String propertyfy(String propName) {
        return propName == null || propName.length() < 1 ? null : "get" + propName.substring(0,1).toUpperCase() + propName.substring(1);

    }

    private void registerAggregate(String aggregate) {
        registerAggregates(new String[]{aggregate});
    }

    private boolean handleOldStyleAnnotations(Class<? extends Saga> sagaClass) throws IntrospectionException {
        ListensTo annotation = sagaClass.getAnnotation(ListensTo.class);
        if (null != annotation) {
            registerAggregates(annotation.aggregates());
            for (EventIdBind eventIdBind : annotation.value()) {
                registerEventToSaga(eventIdBind.eventClass(), sagaClass);
                Method getter = new PropertyDescriptor(eventIdBind.idProperty(), eventIdBind.eventClass()).getReadMethod();
                registerSagaIdPropertyMethod(sagaClass, eventIdBind.eventClass(), getter);

            }
            return true;
        } else {
            return false;
        }
    }

    private void registerAggregates(String[] aggregates) {
        Collections.addAll(SagaManager.aggregates, aggregates);
    }

    private void registerSagaIdPropertyMethod(Class<? extends Saga> sagaClass, Class<?extends Event> eventclass, Method getter) {
        propertyMap.put(new SagaEventId(sagaClass, eventclass), getter);
    }

    private List<Class<? extends Saga>> getSagaClassesForEvent(Class<? extends Event> eventClass) {
        if (!eventToSagaMap.containsKey(eventClass))
            eventToSagaMap.put(eventClass, new ArrayList<Class<? extends Saga>>());

        return eventToSagaMap.get(eventClass);
    }

    private void registerEventToSaga(Class<? extends Event> eventClass, Class<? extends Saga> saga) {
        getSagaClassesForEvent(eventClass).add(saga);
    }

	private void updateLeaderState(ClusterEvent.LeaderChanged leaderChanged) {
		try {
            boolean oldLeader = akkaClusterInfo.isLeader();
            akkaClusterInfo.updateLeaderState(leaderChanged);
			if(oldLeader && !akkaClusterInfo.isLeader()){
				removeOldActorsWithWrongState();
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
