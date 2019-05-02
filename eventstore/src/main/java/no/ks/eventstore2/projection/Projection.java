package no.ks.eventstore2.projection;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Status;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import eventstore.LiveProcessingStarted$;
import eventstore.Messages;
import eventstore.ResolvedEvent;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.eventstore.EventStoreUtil;
import no.ks.eventstore2.eventstore.JsonMetadataBuilder;
import no.ks.eventstore2.reflection.HandlerFinder;
import no.ks.eventstore2.response.NoResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;
import scala.PartialFunction;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;
import scala.util.Failure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public abstract class Projection extends AbstractActor {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ActorRef eventStoreConnection;
    private Map<Class<? extends Message>, Method> handleEventMap = null;

    protected Long latestJournalidReceived;
    private Messages.EventWrapper currentMessage;
    private boolean subscribePhase = false;

    private List<PendingCall> pendingCalls = new ArrayList<>();

    public Projection(ActorRef eventStoreConnection) {
        this.eventStoreConnection = eventStoreConnection;
        init();
    }

    @Override
    public void preStart() {
        log.debug(getSelf().path().toString());
        subscribe();
    }



    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("preRestart");
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        try {
            super.aroundReceive(receive, msg);
        } catch (Exception e) {
            log.error("Projection threw exception while handling message: ", e);
            throw new ProjectionFailedException(new ProjectionFailedError(self(), e, msg), e);
        }
    }

    @Override
    public Receive createReceive() {
        return createReceiveBuilder().build();
    }

    protected ReceiveBuilder createReceiveBuilder() {
        return receiveBuilder()
                .match(ResolvedEvent.class, this::handleResolvedEvent)
                .match(Call.class, () -> subscribePhase, this::handlePendingCall)
                .match(Call.class, () -> !subscribePhase, this::handleCall)
                .match(LiveProcessingStarted$.class, this::handleLiveProcessingStarted);
    }


    @SuppressWarnings("unchecked")
    private void handleCall(Call call) {
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

    private void handlePendingCall(Call call) {
        log.debug("Adding call {} to pending calls", call);
        pendingCalls.add(new PendingCall(call, sender()));
    }

    private void handleResolvedEvent(ResolvedEvent event) throws InvalidProtocolBufferException {
        log.trace("Handling ResolvedEvent: {}", event);
        latestJournalidReceived = event.linkEvent().number().value(); // TODO: Før eller etter håndtering av event?

        Messages.EventWrapper eventWrapper = JsonMetadataBuilder.readMetadataAsWrapper(event.data().metadata().value().toArray())
                .setEvent(Any.parseFrom(event.data().data().value().toArray()))
                .build();

        currentMessage = eventWrapper;
        dispatchToCorrectEventHandler(ProtobufHelper.unPackAny(eventWrapper));
    }

    private void handleLiveProcessingStarted(LiveProcessingStarted$ live) {
        setSubscribeFinished();
        for (PendingCall pendingCall : pendingCalls) {
            self().tell(pendingCall.getCall(), pendingCall.getSender());
        }
        pendingCalls.clear();
        log.info("Live processing started!");
    }

    private void setSubscribeFinished() {
        subscribePhase = false;
        context().parent().tell(ProjectionManager.SUBSCRIBE_FINISHED, self());
    }

    protected void setInSubscribe() {
        subscribePhase = true;
        context().parent().tell(ProjectionManager.IN_SUBSCRIBE, self());
    }

    public final void dispatchToCorrectEventHandler(Message message) {
        Method method = HandlerFinder.findHandlingMethod(handleEventMap, message);

        if (method != null) {
            try {
                method.invoke(this, message);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("Failed to call method " + method + " with event " + message, e);
                throw new RuntimeException(e);
            }
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
                if (!ClassUtils.isAssignable(methodParams[i], callParams[i])) {
                    assignable = false;
                }
            }
            return assignable;
        } else {
            return false;
        }
    }

    private void init() {
        handleEventMap = new HashMap<>();
        handleEventMap.putAll(HandlerFinder.getEventHandlers(this.getClass()));
    }

    private String getSubscribe() {
        Subscriber subscriberAnnotation = getClass().getAnnotation(Subscriber.class);

        if (subscriberAnnotation != null) {
            return subscriberAnnotation.value();
        }
        throw new RuntimeException("No subscribe annotation");
    }

    public boolean isSubscribePhase() {
        return subscribePhase;
    }

    private static final class PendingCall {
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

    private void subscribe() {
        if (!subscribePhase) {
            setInSubscribe();
            String category = getSubscribe();
            log.debug("Subscribing to category \"{}\" from {}", category, latestJournalidReceived);
            getContext().actorOf(EventStoreUtil.getCategorySubscriptionsProps(
                    eventStoreConnection,
                    getSelf(),
                    category,
                    latestJournalidReceived));
        } else {
            log.warn("Trying to subscribe but is already in subscribe phase.");
        }
    }

    public Messages.EventWrapper currentMessage() {
        return currentMessage;
    }
}