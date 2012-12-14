package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.response.NoResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class Projection extends UntypedActor {

	static final Logger log = LoggerFactory.getLogger(Projection.class);
    protected ActorRef eventStore;
    private Map<Class<? extends Event>, Method> handleEventMap = null;

    public Projection(ActorRef eventStore) {
        this.eventStore = eventStore;
        init();
    }

    @Override
    public void preStart(){
        System.out.println(getSelf().path().toString());
        subscribe(eventStore);
    }

    @Override
    public void onReceive(Object o) throws Exception{
        if (o instanceof Event)
            handleEvent((Event) o);
        else if (o instanceof Call)
            handleCall((Call) o);
    }

    public void handleEvent(Event event) {
        Method method = handleEventMap.get(event.getClass());
        if (method != null)
            try {
                method.invoke(this, event);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }

    public void handleCall(Call call) {
        log.debug("handling call: {}", call);
        Method method = getCallMethod(call);
        try {
        	Object result = method.invoke(this, call.getArgs());
        	if(result != null){
        		sender().tell(result, self());
        	} else {
        		sender().tell(new NoResult(), self());
        	}
        } catch (Exception e) {
            throw new RuntimeException("Error calling method!", e);
        }
    }

    private Method getCallMethod(Call call) {
        java.lang.reflect.Method method;
        try {
            Class<?>[] classes = new Class<?>[call.getArgs().length];
            for (int i = 0; i < call.getArgs().length; i++) {
                classes[i] = call.getArgs()[i].getClass();
            }
            method = this.getClass().getMethod(call.getMethodName(), classes);
            return method;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("No valid method to call!", e);
        }

    }

    private void init() {
        handleEventMap = new HashMap<Class<? extends Event>, Method>();
        try {
            Class<? extends Projection> projectionClass = this.getClass();
            ListensTo annotation = projectionClass.getAnnotation(ListensTo.class);
            if (annotation != null) {
                Class[] handledEventClasses = annotation.value();
                for (Class<? extends Event> handledEventClass : handledEventClasses) {
                    Method handleEventMethod = projectionClass.getMethod("handleEvent", handledEventClass);
                    handleEventMap.put(handledEventClass, handleEventMethod);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void subscribe(ActorRef eventStore){
        ListensTo annotation = getClass().getAnnotation(ListensTo.class);
        if (annotation != null)
            for (String aggregate : annotation.aggregates())
                eventStore.tell(new Subscription(aggregate), self());
    }
}