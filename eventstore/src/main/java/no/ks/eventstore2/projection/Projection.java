package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Status;
import akka.actor.UntypedActor;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import eventstore.*;
import eventstore.j.SettingsBuilder;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.eventstore.JsonMetadataBuilder;
import no.ks.eventstore2.reflection.HandlerFinderProtobuf;
import no.ks.eventstore2.response.NoResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;
import scala.Option;
import scala.concurrent.Future;
import scala.util.Failure;

import java.lang.reflect.Method;
import java.util.*;

public abstract class Projection extends UntypedActor {

    private static final String SVARUT_CATEGORY_PREFIX = "ce-no.ks.events.svarut.";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ActorRef connection;
    private Map<Class<? extends Message>, Method> handleEventMap = null;

    protected Long latestJournalidReceived;
    private Messages.EventWrapper currentMessage;
    private boolean subscribePhase = false;

    private List<PendingCall> pendingCalls = new ArrayList<>();

    //TODO; constructor vs preStart, and how do we handle faling actor creations? Pass exception to parent and shutdown actor system?
    public Projection(ActorRef connection) {
        this.connection = connection;
        init();
    }

    @Override
    public void preStart() {
        log.debug(getSelf().path().toString());
        subscribe();
    }

    @Override
    public void preRestart(Throwable reason, Option<Object> message) {

    }

    @Override
    public void onReceive(Object o) {
        try {
            if (o instanceof ResolvedEvent) {
                ResolvedEvent event = (ResolvedEvent) o;
                log.debug("Handling event: {}", o);
                latestJournalidReceived = event.linkEvent().number().value();

                Any anyEvent = Any.parseFrom(event.data().data().value().toArray());
                Messages.EventWrapper eventWrapper = JsonMetadataBuilder.readMetadataAsWrapper(event.data().metadata().value().toArray())
                        .setEvent(anyEvent)
                        .build();
                currentMessage = eventWrapper;

                dispatchToCorrectEventHandler(ProtobufHelper.unPackAny(eventWrapper.getProtoSerializationType(), anyEvent));
            } else if (o instanceof Call && !subscribePhase) {
                handleCall((Call) o);
            } else if (o instanceof LiveProcessingStarted$) {
//                if (o instanceof CompleteAsyncSubscriptionPleaseSendSyncSubscription) {
//                    log.info("AsyncSubscription complete, sending sync subscription");
//                    eventStore.tell(new Subscription(((CompleteAsyncSubscriptionPleaseSendSyncSubscription) o).getAggregateType(), latestJournalidReceived), self());
//                } else {
//                    log.info("Subscription on {} is complete", ((CompleteSubscriptionRegistered) o).getAggregateType());
//                    setSubscribeFinished();
//                }
                setSubscribeFinished();
                for (PendingCall pendingCall : pendingCalls) {
                    self().tell(pendingCall.getCall(), pendingCall.getSender());
                }
                pendingCalls.clear();
                log.info("Live processing started!");
            } else if (o instanceof Call && subscribePhase) {
                log.debug("Adding call {} to pending calls", o);
                pendingCalls.add(new PendingCall((Call) o, sender()));
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
    }

    protected void setInSubscribe() {
        subscribePhase = true;
        context().parent().tell(ProjectionManager.IN_SUBSCRIBE, self());
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
        try {
            Class<? extends Projection> projectionClass = this.getClass();
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

    protected String getSubscribe() { // TODO: Brukes fra subscribe
        Subscriber subscriberAnnotation = getClass().getAnnotation(Subscriber.class);

        if (subscriberAnnotation != null) {
            return subscriberAnnotation.value();
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
            EventStream.Id streamId = new EventStream.System(SVARUT_CATEGORY_PREFIX + getSubscribe());
            EventNumber.Exact eventNumber = new EventNumber.Exact(Optional.ofNullable(latestJournalidReceived).orElse(0L));
            log.info("Subscribing to {} from {}", streamId, eventNumber);
            setInSubscribe();
            getContext().actorOf(StreamSubscriptionActor.props(
                    connection,
                    getSelf(),
                    streamId,
                    Option.<EventNumber>apply(eventNumber),
                    Option.<UserCredentials>empty(),
                    new SettingsBuilder().resolveLinkTos(true).build()));
        } else {
            log.warn("Trying to subscribe but is already in subscribe phase.");
        }
    }

    protected Messages.EventWrapper currentMessage() {
        return currentMessage;
    }
}