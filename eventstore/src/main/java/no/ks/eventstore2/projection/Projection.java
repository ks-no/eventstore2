package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.CompleteSubscriptionRegistered;
import no.ks.eventstore2.eventstore.IncompleteSubscriptionPleaseSendNew;
import no.ks.eventstore2.eventstore.Subscription;
import no.ks.eventstore2.reflection.HandlerFinder;
import no.ks.eventstore2.response.NoResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

public abstract class Projection extends UntypedActor {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected ActorRef eventStore;
    private Map<Class<? extends Event>, Method> handleEventMap = null;

    protected String latestJournalidReceived;

    private boolean subscribePhase = true;

    private List<PendingCall> pendingCalls = new ArrayList<PendingCall>();
    //TODO; constructor vs preStart, and how do we handle faling actor creations? Pass exception to parent and shutdown actor system?
    public Projection(ActorRef eventStore) {
        this.eventStore = eventStore;
        init();
    }

    @Override
    public void preStart(){
    	log.debug(getSelf().path().toString());
        eventStore.tell(getSubscribe(), self());
    }

    @Override
    public void onReceive(Object o) {
        try{
            if (o instanceof Event) {
                latestJournalidReceived = ((Event) o).getJournalid();
                dispatchToCorrectEventHandler((Event) o);
            } else if (o instanceof Call && !subscribePhase) {
            	handleCall((Call) o);
            } else if(o instanceof IncompleteSubscriptionPleaseSendNew){
                log.debug("Sending new subscription on {} from {}",((IncompleteSubscriptionPleaseSendNew) o).getAggregateType(),latestJournalidReceived);
                if(latestJournalidReceived == null) {
                	throw new RuntimeException("Missing latestJournalidReceived but got IncompleteSubscriptionPleaseSendNew");
                }
                eventStore.tell(new Subscription(((IncompleteSubscriptionPleaseSendNew) o).getAggregateType(),latestJournalidReceived),self());
            } else if(o instanceof CompleteSubscriptionRegistered){
                log.info("Subscription on {} is complete", ((CompleteSubscriptionRegistered) o).getAggregateType());
                subscribePhase = false;
                for (PendingCall pendingCall : pendingCalls) {
                    self().tell(pendingCall.getCall(), pendingCall.getSender());
                }
                pendingCalls.clear();
            } else if(o instanceof Call && subscribePhase){
                log.debug("Adding call {} to pending calls", o);
                pendingCalls.add(new PendingCall((Call) o,sender()));
            }
        } catch (Exception e){
            getContext().parent().tell(new ProjectionFailedError(self(), e, o), self());
            log.error("Projection threw exception while handling message: ", e);
            throw new RuntimeException("Projection threw exception while handling message: ", e);
        }
    }

    public final void dispatchToCorrectEventHandler(Event event) {
        Method method = HandlerFinder.findHandlingMethod(handleEventMap, event);

        if (method != null) {
            try {
                method.invoke(this, event);
            } catch (Exception e) {
                log.error("Failed to call method " + method + " with event " + event,e);
                throw new RuntimeException(e);
            }
        }
    }

    public final void handleCall(Call call) {
        log.debug("handling call: {}", call);
        try {
            Method method = getCallMethod(call);
        	Object result = method.invoke(this, call.getArgs());

            if (!method.getReturnType().equals(Void.TYPE)) {
                if(result != null){
                    sender().tell(result, self());
                } else {
                    sender().tell(new NoResult(), self());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error handling projection call!", e);
        }
    }

    private Method getCallMethod(Call call) throws NoSuchMethodException {
        if(call == null) {
        	throw new IllegalArgumentException("Call can't be null");
        }

        Class<?>[] classes = new Class<?>[call.getArgs().length];
        for (int i = 0; i < call.getArgs().length; i++) {
            classes[i] = call.getArgs()[i].getClass();
        }
        Method[] allMethods = this.getClass().getMethods();
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

    protected Subscription getSubscribe(){
        ListensTo annotation = getClass().getAnnotation(ListensTo.class);
        if (annotation != null) {
            for (String aggregate : annotation.aggregates()) {
                return new Subscription(aggregate);
            }
        }
        Subscriber subscriberAnnotation = getClass().getAnnotation(Subscriber.class);

        if (subscriberAnnotation != null) {
        	return new Subscription(subscriberAnnotation.value());
        }
        throw new RuntimeException("No subscribe annotation");
    }

    public boolean isSubscribePhase(){
        return subscribePhase;
    }

    private class PendingCall {
        private Call call;
        private ActorRef sender;

        private PendingCall(Call call, ActorRef sender) {
            this.call = call;
            this.sender = sender;
        }

        public Call getCall() {
            return call;
        }

        public void setCall(Call call) {
            this.call = call;
        }

        public ActorRef getSender() {
            return sender;
        }

        public void setSender(ActorRef sender) {
            this.sender = sender;
        }
    }
}