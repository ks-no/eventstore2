package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.reflection.HandlerFinder;
import no.ks.eventstore2.response.NoResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class Projection extends UntypedActor {

	static final Logger log = LoggerFactory.getLogger(Projection.class);
    protected ActorRef eventStore;
    private Map<Class<? extends Event>, Method> handleEventMap = null;


    //TODO; constructor vs preStart, and how do we handle faling actor creations? Pass exception to parent and shutdown actor system?
    public Projection(ActorRef eventStore) {
        this.eventStore = eventStore;
        init();
    }

    @Override
    public final void preStart(){
        System.out.println(getSelf().path().toString());
        subscribe(eventStore);
    }

    @Override
    public final void onReceive(Object o) throws Exception{
        try{
            if (o instanceof Event)
                dispatchToCorrectEventHandler((Event) o);
            else if (o instanceof Call)
                handleCall((Call) o);
        } catch (Exception e){
            getContext().parent().tell(new ProjectionFailedError(self(), e, o), self());
            log.error("Projection threw exception while handling message: ", e);
            throw new RuntimeException("Projection threw exception while handling message: ", e);
        }
    }

    public final void dispatchToCorrectEventHandler(Event event) {
        Method method = HandlerFinder.findHandlingMethod(handleEventMap, event);

        if (method != null)
            try {
                method.invoke(this, event);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }

    public final void handleCall(Call call) {
        log.debug("handling call: {}", call);
        try {
            Method method = getCallMethod(call);
        	Object result = method.invoke(this, call.getArgs());
        	if(result != null){
        		sender().tell(result, self());
        	} else {
        		sender().tell(new NoResult(), self());
        	}
        } catch (Exception e) {
            throw new RuntimeException("Error handling projection call!", e);
        }
    }

    private Method getCallMethod(Call call) throws NoSuchMethodException {
        Class<?>[] classes = new Class<?>[call.getArgs().length];
        for (int i = 0; i < call.getArgs().length; i++) {
            classes[i] = call.getArgs()[i].getClass();
        }
        Method[] allMethods = this.getClass().getDeclaredMethods();
        for (Method m : allMethods) {
            if (methodAssignable(call.getMethodName(), classes, m))
                return m;
        }
        throw new NoSuchMethodException("method " + call.getMethodName() + "(" + Arrays.toString(classes) +") not found in " + this.getClass().getSimpleName());
    }

    private boolean methodAssignable(String callName, Class<?>[] callParams, Method candidateMethod) {
        Class[] methodParams = candidateMethod.getParameterTypes();

        if (callName.equals(candidateMethod.getName()) && (methodParams.length == callParams.length)){
            boolean assignable = true;
            for (int i = 0; i < callParams.length; i++) {
                if (!methodParams[i].isAssignableFrom(callParams[i]))
                    assignable = false;
            }
            return assignable;
        } else {
            return false;
        }
    }

    private void init() {
        handleEventMap = new HashMap<Class<? extends Event>, Method>();
        try {
            Class<? extends Projection> projectionClass = this.getClass();
            ListensTo listensTo = projectionClass.getAnnotation(ListensTo.class);
            if (listensTo != null) {
                Class[] handledEventClasses = listensTo.value();

                for (Class<? extends Event> handledEventClass : handledEventClasses) {
                    Method handleEventMethod = projectionClass.getMethod("handleEvent", handledEventClass);
                    handleEventMap.put(handledEventClass, handleEventMethod);
                }
            } else {
                handleEventMap.putAll(HandlerFinder.getEventHandlers(projectionClass));
            }
        } catch (Exception e) {
            log.error("Exception during creation of projection: ", e);
            throw new RuntimeException(e);
        }
    }

    protected final void subscribe(ActorRef eventStore){
        ListensTo annotation = getClass().getAnnotation(ListensTo.class);
        if (annotation != null)
            for (String aggregate : annotation.aggregates())
                eventStore.tell(new Subscription(aggregate), self());

        Subscriber subscriberAnnotation = getClass().getAnnotation(Subscriber.class);

        if (subscriberAnnotation != null)
            eventStore.tell(new Subscription(subscriberAnnotation.value()), self());
    }
}