package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class CommandHandler extends UntypedActor{

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
    public void onReceive(Object o) throws Exception{
        if (o instanceof Command)
            handleCommand((Command) o);
        if("HandlesClasses".equals(o)){
            log.debug("Handles classes received sending map to " + sender());
            sender().tell(ImmutableSet.copyOf(handleCommandMap.keySet()), self());
        }
    }

    public void handleCommand(Command command) {
        Method method = handleCommandMap.get(command.getClass());
        try {
            method.invoke(this, command);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void init() {
        handleCommandMap = new HashMap<Class<? extends Command>, Method>();
        try {
            Class<? extends CommandHandler> handlerClass = this.getClass();
            HandlesCommand annotation = handlerClass.getAnnotation(HandlesCommand.class);
            if (annotation != null) {
                Class[] handledCommandClasses = annotation.value();
                for (Class<? extends Command> handledEventClass : handledCommandClasses) {
                    Method handleCommandMethod = handlerClass.getMethod("handleCommand", handledEventClass);
                    handleCommandMap.put(handledEventClass, handleCommandMethod);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
