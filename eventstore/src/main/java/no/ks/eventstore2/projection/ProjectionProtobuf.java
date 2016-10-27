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
import akka.japi.Procedure;
import com.google.protobuf.Message;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.RestartActorException;
import no.ks.eventstore2.eventstore.EventStore;
import no.ks.eventstore2.eventstore.NewEventstoreStarting;
import no.ks.eventstore2.eventstore.RefreshSubscription;
import no.ks.eventstore2.reflection.HandlerFinderProtobuf;
import no.ks.eventstore2.response.NoResult;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.util.Failure;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class ProjectionProtobuf extends UntypedActor {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected ActorRef eventStore;
    private Map<Class<? extends Message>, Method> handleEventMap = null;
    protected long latestJournalidReceived;

    private boolean subscribePhase = false;

    private List<ProjectionProtobuf.PendingCall> pendingCalls = new ArrayList<ProjectionProtobuf.PendingCall>();
    private Cancellable subscribeTimeout;
    private Cancellable removeSubscriptionTimeout;
    private Messages.EventWrapper currentMessage;

    protected Messages.EventWrapper currentMessage() {
        return currentMessage;
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
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        removeSubscriptionTimeout.cancel();
    }
    private Procedure<Object> restarting = message -> {
        if (message instanceof Messages.SubscriptionRemoved) {
            throw new RestartActorException("Restaring actor");
        } else {
            log.debug("Got message {} while restarting", message);
        }
    };

    //TODO; constructor vs preStart, and how do we handle faling actor creations? Pass exception to parent and shutdown actor system?
    public ProjectionProtobuf(ActorRef eventStore) {
        this.eventStore = eventStore;
        init();
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


    public final void dispatchToCorrectEventHandler(Message message) {
        Method method = HandlerFinderProtobuf.findHandlingMethod(handleEventMap, message);

        if (method != null) {
            try {
                method.invoke(this, message);
            } catch (Exception e) {
                log.error("Failed to call method " + method + " with event " + message, e);
                throw new RuntimeException(e);
            }
        }
    }


    private void startRemoveSubscriptionTimeout() {
        removeSubscriptionTimeout = getContext().system().scheduler().scheduleOnce(Duration.create(10, TimeUnit.SECONDS), self(), "stillInSubscribe?", getContext().system().dispatcher(),self());
    }

    private void setSubscribeFinished() {
        subscribePhase = false;
        context().parent().tell(ProjectionManager.SUBSCRIBE_FINISHED, self());
        cancelSubscribeTimeout();
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

    private boolean methodAssignable(String callName, Class<?>[] callParams, Method candidateMethod) {
        Class[] methodParams = candidateMethod.getParameterTypes();

        if (callName.equals(candidateMethod.getName()) && (methodParams.length == callParams.length)) {
            boolean assignable = true;
            for (int i = 0; i < callParams.length; i++) {
                if (!ClassUtils.isAssignable(callParams[i],methodParams[i])) {
                    assignable = false;
                }
            }
            return assignable;
        } else {
            return false;
        }
    }

    private void init() {
        handleEventMap = new HashMap<Class<? extends Message>, Method>();
        try {
            Class<? extends ProjectionProtobuf> projectionClass = this.getClass();
            ListensTo listensTo = projectionClass.getAnnotation(ListensTo.class);
            if (listensTo != null) {
                Class[] handledEventClasses = listensTo.value();

                for (Class<? extends Message> handledEventClass : handledEventClasses) {
                    Method handleEventMethod = projectionClass.getMethod("handleEvent", handledEventClass);
                    handleEventMap.put(handledEventClass, handleEventMethod);
                }
            } else {
                handleEventMap.putAll(HandlerFinderProtobuf.getEventHandlers(projectionClass));
            }
        } catch (Exception e) {
            log.error("Exception during creation of projection: ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onReceive(Object o) {
        if("restart".equals(o)){
            //eventStore.tell(new RemoveSubscription(getSubscribe().getAggregateType()), getSelf());
            eventStore.tell(Messages.RemoveSubscription.newBuilder().setAggregateType(getSubscribe().getAggregateType()).build(),getSelf());
            startRemoveSubscriptionTimeout();
            getContext().become(restarting);

        }
        try {
            if (o instanceof Messages.EventWrapper) {
                latestJournalidReceived = ((Messages.EventWrapper) o).getJournalid();
                currentMessage = (Messages.EventWrapper) o;
                dispatchToCorrectEventHandler(ProtobufHelper.unPackAny(((Messages.EventWrapper) o).getProtoSerializationType(), ((Messages.EventWrapper) o).getEvent()));
            } else if (o instanceof NewEventstoreStarting) {
                preStart();
            } else if (o instanceof Call && !subscribePhase) {
                handleCall((Call) o);
            }else if (o instanceof RefreshSubscription){ //TODO
                subscribe();
            } else if (o instanceof Messages.IncompleteSubscriptionPleaseSendNew) {
                log.debug("Sending new subscription on {} from {}", ((Messages.IncompleteSubscriptionPleaseSendNew) o).getAggregateType(), latestJournalidReceived);
                resetSubscribeTimeout();
                if (latestJournalidReceived < 0) {
                    throw new RuntimeException("Missing latestJournalidReceived but got IncompleteSubscriptionPleaseSendNew");
                }
                eventStore.tell(Messages.AsyncSubscription.newBuilder().setAggregateType(((Messages.IncompleteSubscriptionPleaseSendNew) o).getAggregateType()).build(), self());

            }else if (o instanceof Messages.CompleteAsyncSubscriptionPleaseSendSyncSubscription){
                log.info("AsyncSubscription complete, sending sync subscription");
                resetSubscribeTimeout();
                eventStore.tell(Messages.Subscription.newBuilder().setAggregateType( ((Messages.CompleteAsyncSubscriptionPleaseSendSyncSubscription)o).getAggregateType()).setFromJournalId(latestJournalidReceived).build(),self());

            } else if (o instanceof Messages.CompleteSubscriptionRegistered) {
                log.info("Subscription on {} is complete", ((Messages.CompleteSubscriptionRegistered) o).getAggregateType());
                setSubscribeFinished();
                for (ProjectionProtobuf.PendingCall pendingCall : pendingCalls) {
                        self().tell(pendingCall.getCall(), pendingCall.getSender());
                }
                pendingCalls.clear();

            } else if (o instanceof Call && subscribePhase) {
                log.debug("Adding call {} to pending calls", o);
                pendingCalls.add(new ProjectionProtobuf.PendingCall((Call) o, sender()));
            }else if (o instanceof DistributedPubSubMediator.SubscribeAck){
                log.info("Subscribing for eventstore restartmessages");
            } else if("stillInSubscribe?".equals(o)){
                log.error("We are still in subscribe somethings wrong, resubscribing");
                subscribePhase = false;
                preStart();
            }
        } catch (Exception e) {
            getContext().parent().tell(new ProjectionFailedError(self(), e, o), self());
            log.error("Projection threw exception while handling message: ", e);
            throw new RuntimeException("Projection threw exception while handling message: ", e);
        }
    }


    protected Messages.Subscription getSubscribe() {
        ListensTo annotation = getClass().getAnnotation(ListensTo.class);
        if (annotation != null) {
            for (String aggregate : annotation.aggregates()) {
                //return new AsyncSubscription(aggregate);
                return Messages.Subscription.newBuilder().setAggregateType(aggregate).build();
            }
        }
        Subscriber subscriberAnnotation = getClass().getAnnotation(Subscriber.class);

        if (subscriberAnnotation != null) {
            //return new AsyncSubscription(subscriberAnnotation.value());
            return Messages.Subscription.newBuilder().setAggregateType(subscriberAnnotation.value()).build();
        }
        throw new RuntimeException("No subscribe annotation");
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

    public boolean isSubscribePhase() {
        return subscribePhase;
    }

    protected void subscribe() {
        if(!subscribePhase) {
            setInSubscribe();
            //eventStore.tell(new AsyncSubscription(getSubscribe().getAggregateType(), latestJournalidReceived), self());
            eventStore.tell(Messages.AsyncSubscription.newBuilder().setAggregateType(getSubscribe().getAggregateType()).setFromJournalId(latestJournalidReceived).build(),self());
        } else {
            log.warn("Trying to subscribe but is already in subscribe phase.");
        }
    }
}
