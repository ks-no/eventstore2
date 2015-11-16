package no.ks.eventstore2.projection;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Status;
import akka.actor.UntypedActor;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.*;
import no.ks.eventstore2.reflection.HandlerFinder;
import no.ks.eventstore2.response.NoResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.util.Failure;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class Projection extends UntypedActor {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected ActorRef eventStore;
    private Map<Class<? extends Event>, Method> handleEventMap = null;

    protected String latestJournalidReceived;

    private boolean subscribePhase = false;

    private List<PendingCall> pendingCalls = new ArrayList<PendingCall>();
    private Cancellable subscribeTimeout;

    //TODO; constructor vs preStart, and how do we handle faling actor creations? Pass exception to parent and shutdown actor system?
    public Projection(ActorRef eventStore) {
        this.eventStore = eventStore;
        init();
    }

    @Override
    public void preStart() {
        log.debug(getSelf().path().toString());
        try {
            ActorRef mediator =
                    DistributedPubSub.get(getContext().system()).mediator();

            mediator.tell(new DistributedPubSubMediator.Subscribe(EventStore.EVENTSTOREMESSAGES, getSelf()),
                    getSelf());
        } catch (ConfigurationException e){
            log.info("Not subscribing to eventstore event, no cluster system");
        }
        subscribe();
    }

    @Override
    public void onReceive(Object o) {
        try {
            if (o instanceof Event) {
                latestJournalidReceived = ((Event) o).getJournalid();
                dispatchToCorrectEventHandler((Event) o);
            } else if (o instanceof NewEventstoreStarting) {
                preStart();
            } else if (o instanceof Call && !subscribePhase) {
                handleCall((Call) o);
            }else if (o instanceof RefreshSubscription){
                subscribe();
            } else if (o instanceof IncompleteSubscriptionPleaseSendNew) {
                log.debug("Sending new subscription on {} from {}", ((IncompleteSubscriptionPleaseSendNew) o).getAggregateType(), latestJournalidReceived);
                resetSubscribeTimeout();
                if (latestJournalidReceived == null) {
                    throw new RuntimeException("Missing latestJournalidReceived but got IncompleteSubscriptionPleaseSendNew");
                }
                eventStore.tell(new AsyncSubscription(((IncompleteSubscriptionPleaseSendNew) o).getAggregateType(), latestJournalidReceived), self());
            } else if (o instanceof CompleteSubscriptionRegistered) {
                if (o instanceof CompleteAsyncSubscriptionPleaseSendSyncSubscription) {
                    log.info("AsyncSubscription complete, sending sync subscription");
                    resetSubscribeTimeout();
                    eventStore.tell(new Subscription(((CompleteAsyncSubscriptionPleaseSendSyncSubscription) o).getAggregateType(), latestJournalidReceived), self());
                } else {
                    log.info("Subscription on {} is complete", ((CompleteSubscriptionRegistered) o).getAggregateType());
                    setSubscribeFinished();
                    for (PendingCall pendingCall : pendingCalls) {
                        self().tell(pendingCall.getCall(), pendingCall.getSender());
                    }
                    pendingCalls.clear();
                }
            } else if (o instanceof Call && subscribePhase) {
                log.debug("Adding call {} to pending calls", o);
                pendingCalls.add(new PendingCall((Call) o, sender()));
            }else if (o instanceof DistributedPubSubMediator.SubscribeAck){
                log.info("Subscribing for eventstore restartmessages");
            } else if("stillInSubscribe?".equals(o)){
                log.error("We are still in subscribe somethings wrong, resubscribing");
                preStart();
            }
        } catch (Exception e) {
            getContext().parent().tell(new ProjectionFailedError(self(), e, o), self());
            log.error("Projection threw exception while handling message: ", e);
            throw new RuntimeException("Projection threw exception while handling message: ", e);
        }
    }

    private void setSubscribeFinished() {
        subscribePhase = false;
        context().parent().tell(ProjectionManager.SUBSCRIBE_FINISHED, self());
        cancelSubscribeTimeout();
    }

    private void cancelSubscribeTimeout() {
        if(subscribeTimeout != null) subscribeTimeout.cancel();
        subscribeTimeout = null;
    }


    protected void setInSubscribe() {
        subscribePhase = true;
        context().parent().tell(ProjectionManager.IN_SUBSCRIBE, self());
        resetSubscribeTimeout();
    }

    private void resetSubscribeTimeout() {
        if(subscribeTimeout != null) subscribeTimeout.cancel();
        subscribeTimeout = getContext().system().scheduler().scheduleOnce(Duration.create(5, TimeUnit.MINUTES), self(), "stillInSubscribe?", getContext().system().dispatcher(),self());
    }

    public final void dispatchToCorrectEventHandler(Event event) {
        Method method = HandlerFinder.findHandlingMethod(handleEventMap, event);

        if (method != null) {
            try {
                method.invoke(this, event);
            } catch (Exception e) {
                log.error("Failed to call method " + method + " with event " + event, e);
                throw new RuntimeException(e);
            }
        }
    }

    public final void handleCall(Call call) {
        log.debug("handling call: {}", call);
        try {
            Method method = getCallMethod(call);
            Object result = method.invoke(this, call.getArgs());

            if(method.getReturnType().equals(Future.class)){
                final ActorRef sender = sender();
                final ActorRef self = self();
                ((Future<Object>) result).onSuccess(new OnSuccess<Object>(){
                    @Override
                    public void onSuccess(Object result) throws Throwable {
                        sender.tell(result,self);
                    }
                },getContext().dispatcher());
                ((Future<Object>) result).onFailure(new OnFailure(){

                    @Override
                    public void onFailure(Throwable failure) throws Throwable {
                        sender.tell(new Failure<>(failure), self);
                    }
                }, getContext().dispatcher());
            } else if (!method.getReturnType().equals(Void.TYPE)) {
                if (result != null) {
                    sender().tell(result, self());
                } else {
                    sender().tell(new NoResult(), self());
                }
            }
        } catch (Exception e) {
            RuntimeException runtimeException = new RuntimeException("Error handling projection call! " + call, e);
            sender().tell(new Status.Failure(runtimeException), self());
            throw runtimeException;

        }
    }

    private Method getCallMethod(Call call) throws NoSuchMethodException {
        if (call == null) {
            throw new IllegalArgumentException("Call can't be null");
        }

        Class<?>[] classes = new Class<?>[call.getArgs().length];
        for (int i = 0; i < call.getArgs().length; i++) {
            classes[i] = call.getArgs()[i].getClass();
        }
        Method[] allMethods = this.getClass().getMethods();
        for (Method m : allMethods) {
            if (methodAssignable(call.getMethodName(), classes, m)) {
                return m;
            }
        }
        throw new NoSuchMethodException("method " + call.getMethodName() + "(" + Arrays.toString(classes) + ") not found in " + this.getClass().getSimpleName());
    }

    private boolean methodAssignable(String callName, Class<?>[] callParams, Method candidateMethod) {
        Class[] methodParams = candidateMethod.getParameterTypes();

        if (callName.equals(candidateMethod.getName()) && (methodParams.length == callParams.length)) {
            boolean assignable = true;
            for (int i = 0; i < callParams.length; i++) {
                if (!methodParams[i].isAssignableFrom(callParams[i])) {
                    assignable = false;
                }
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

    protected Subscription getSubscribe() {
        ListensTo annotation = getClass().getAnnotation(ListensTo.class);
        if (annotation != null) {
            for (String aggregate : annotation.aggregates()) {
                return new AsyncSubscription(aggregate);
            }
        }
        Subscriber subscriberAnnotation = getClass().getAnnotation(Subscriber.class);

        if (subscriberAnnotation != null) {
            return new AsyncSubscription(subscriberAnnotation.value());
        }
        throw new RuntimeException("No subscribe annotation");
    }

    public boolean isSubscribePhase() {
        return subscribePhase;
    }

    private final class PendingCall {
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

    protected void subscribe() {
        if(!subscribePhase) {
            setInSubscribe();
            eventStore.tell(new AsyncSubscription(getSubscribe().getAggregateType(), latestJournalidReceived), self());
        } else {
            log.warn("Trying to subscribe but is already in subscribe phase.");
        }
    }
}