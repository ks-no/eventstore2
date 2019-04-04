package no.ks.eventstore2.command;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.collect.ImmutableSet;
import no.ks.eventstore2.reflection.HandlerFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class CommandHandler extends AbstractActor {

    private static Logger log= LoggerFactory.getLogger(CommandHandler.class);

    protected ActorRef eventStore;
    private Map<Class<? extends Command>, Method> handleCommandMap = null;

    public CommandHandler(ActorRef eventStore) {
        this.eventStore = eventStore;
        log.debug("created commandhandler");
    }

    @Override
    public void preStart() throws Exception {
        log.debug("PrestartCalled");
        super.preStart();
        init();
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        try {
            super.aroundReceive(receive, msg);
        } catch (Exception e) {
            log.error("Command handler threw exception when handling message: ", e);
            throw new RuntimeException("Command handler threw exception when handling message: ", e);
        }
    }

    @Override
    public Receive createReceive() {
        return createReceiveBuilder().build();
    }

    protected ReceiveBuilder createReceiveBuilder() {
        return receiveBuilder()
                .match(Command.class, this::handleCommand)
                .matchEquals("HandlesClasses", this::handleHandlesClasses);
    }

    private void handleCommand(Command command) {
        log.debug("Received command {}", command);
        Method method = handleCommandMap.get(command.getClass());
        try {
            method.invoke(this, command);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleHandlesClasses(Object o) {
        log.debug("Handles classes received sending map to " + sender());
        sender().tell(ImmutableSet.copyOf(handleCommandMap.keySet()), self());
    }

    private void init() {
        handleCommandMap = new HashMap<>();
        try {
            handleCommandMap.putAll(getNewStyleHandlers());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Class<? extends Command>, Method> getNewStyleHandlers() {
        return  HandlerFinder.getCommandHandlers(this.getClass());
    }
}
