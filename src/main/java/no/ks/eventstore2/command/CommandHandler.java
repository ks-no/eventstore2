package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class CommandHandler extends UntypedActor{

    protected ActorRef eventStore;

    public CommandHandler(ActorRef eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public void preStart() {
        super.preStart();
        init();
    }

    @Override
    public void onReceive(Object o) throws Exception{
        if (o instanceof Command)
            handleCommand((Command) o);
        if("HandlesClasses".equals(o)){
            sender().tell(ImmutableSet.copyOf(handleCommandMap.keySet()),self());
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

    private Map<Class<? extends Command>, Method> handleCommandMap = null;

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
