package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import no.ks.eventstore2.Event;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;

public class SagaManager extends UntypedActor {
    private final ActorRef commandDispatcher;
    private final SagaRepository repository;

    private Map<SagaCompositeId, ActorRef> sagas = new HashMap<SagaCompositeId, ActorRef>();

    public SagaManager(ActorRef commandDispatcher, SagaRepository repository) {
        this.commandDispatcher = commandDispatcher;
        this.repository = repository;
    }

    @Override
    public void onReceive(Object o) throws Exception {
        if (o instanceof Event){
            Event event = (Event) o;
            for (Class<? extends Saga> clz : getSagaClassesForEvent(event.getClass())) {
                String sagaId = (String) propertyMap.get(new SagaEventId(clz, event.getClass())).invoke(event);
                ActorRef sagaRef = getOrCreateSaga(clz, sagaId);
                sagaRef.tell(event, self());
            }
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
}
