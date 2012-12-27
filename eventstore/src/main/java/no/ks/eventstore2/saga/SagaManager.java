package no.ks.eventstore2.saga;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.MemberStatus;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.Subscription;
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

    private Map<SagaCompositeId, ActorRef> sagas = new HashMap<SagaCompositeId, ActorRef>();
	private boolean leader;

	public SagaManager(ActorRef commandDispatcher, SagaRepository repository, ActorRef eventstore) {
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
        this.eventstore = eventstore;
    }


    @Override
    public void preStart() {
		updateLeaderState();
		subscribeToClusterEvents();
        for (String aggregate : aggregates) {
            eventstore.tell(new Subscription(aggregate), self());
        }
    }

    @Override
    public void onReceive(Object o) throws Exception {
		if(log.isDebugEnabled() && o instanceof  Event)
			log.debug("Sagamanager Received Event {} is leader {}", o, leader);
        if (o instanceof Event && leader){
			log.debug("Sagamanager processing Event {}", o);
            Event event = (Event) o;
            for (Class<? extends Saga> clz : getSagaClassesForEvent(event.getClass())) {
                String sagaId = (String) propertyMap.get(new SagaEventId(clz, event.getClass())).invoke(event);
                ActorRef sagaRef = getOrCreateSaga(clz, sagaId);
                sagaRef.tell(event, self());
            }
        } else if( o instanceof ClusterEvent.LeaderChanged){
			updateLeaderState();
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

    static {
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(true);
        scanner.addIncludeFilter(new AnnotationTypeFilter(ListensTo.class));
        for (BeanDefinition bd : scanner.findCandidateComponents("no"))
            register(bd.getBeanClassName());
    }

    @SuppressWarnings("unchecked")
    private static void register(String className) {
        try {
            Class<? extends Saga> sagaClass = (Class<? extends Saga>) Class.forName(className);
            ListensTo annotation = sagaClass.getAnnotation(ListensTo.class);
            if (null != annotation) {
                registerAggregates(annotation.aggregates());
                for (EventIdBind eventIdBind : annotation.value()) {
                    registerEventToSaga(eventIdBind.eventClass(), sagaClass);
                    registerEventSagaIdGetterMethod(sagaClass, eventIdBind);

                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void registerAggregates(String[] aggregates) {
        Collections.addAll(SagaManager.aggregates, aggregates);
    }

    private static void registerEventSagaIdGetterMethod(Class<? extends Saga> sagaClass, EventIdBind eventIdBind) throws IntrospectionException {
        Method getter = new PropertyDescriptor(eventIdBind.idProperty(), eventIdBind.eventClass()).getReadMethod();
        propertyMap.put(new SagaEventId(sagaClass, eventIdBind.eventClass()), getter);
    }

    private static List<Class<? extends Saga>> getSagaClassesForEvent(Class<? extends Event> eventClass) {
        if (!eventToSagaMap.containsKey(eventClass))
            eventToSagaMap.put(eventClass, new ArrayList<Class<? extends Saga>>());

        return eventToSagaMap.get(eventClass);
    }

    private static void registerEventToSaga(Class<? extends Event> eventClass, Class<? extends Saga> saga) {
        getSagaClassesForEvent(eventClass).add(saga);
    }

	private void subscribeToClusterEvents() {
		try {
			Cluster cluster = Cluster.get(getContext().system());
			cluster.subscribe(self(), ClusterEvent.ClusterDomainEvent.class);
		} catch (ConfigurationException e) {

		}
	}

	private void updateLeaderState() {
		try {
			Cluster cluster = Cluster.get(getContext().system());
			boolean notReady = true;
			while(!cluster.readView().self().status().equals(MemberStatus.up())){
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {

				}
			}
			log.debug("SagaManager was leader? {}", leader);
			boolean oldleader = leader;
			leader = cluster.readView().isLeader();
			log.debug("SagaManager is leader? {}", leader);
			if(oldleader && !leader){
				removeOldActorsWithWrongState();
			}
		} catch (ConfigurationException e) {
			log.debug("Not cluster system");
			leader = true;
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
