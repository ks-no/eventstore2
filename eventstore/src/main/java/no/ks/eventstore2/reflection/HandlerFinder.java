package no.ks.eventstore2.reflection;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import eventstore.EventRecord;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.SubscriberConfigurationException;
import no.ks.eventstore2.command.Command;
import no.ks.eventstore2.command.CommandHandler;
import akka.actor.UntypedActor;

public class HandlerFinder {

    private HandlerFinder() {}

    public static Method findHandlingMethod(Map<Class<? extends Event>, Method> handlers, Event event) {
        Method method = null;

        Class<?> theclass = event.getClass();
        while (method == null && theclass != Object.class){
            method = handlers.get(theclass);
            theclass = theclass.getSuperclass();
        }
        return method;
    }

    public static Method findHandlingMethod(Map<Class<? extends Event>, Method> handlers, EventRecord event) {
        Method method = null;

        Class<?> theclass = event.getClass();
        while (method == null && theclass != Object.class){
            method = handlers.get(theclass);
            theclass = theclass.getSuperclass();
        }
        return method;
    }

    public static Map<Class<? extends Event>, Method> getEventHandlers(Class<? extends UntypedActor> clazz) {
        return getHandlers(clazz, Event.class);
    }

    public static Map<Class<? extends Command>, Method> getCommandHandlers(Class<? extends CommandHandler> clazz) {
        return getHandlers(clazz, Command.class);
    }

    public static <T> Map<Class<? extends T>, Method> getHandlers(Class<? extends UntypedActor> clazz, Class<T> handlesClass) {
        HashMap<Class<? extends T>, Method> handlers = new HashMap<Class<? extends T>, Method>();
        for (Method method : clazz.getMethods()) {
            Handler handlerAnnotation = method.getAnnotation(Handler.class);

            if (handlerAnnotation != null) {
                Class<?>[] types = method.getParameterTypes();
                if (types.length != 1) {
                	throw new SubscriberConfigurationException("Invalid handler signature on " + clazz.getName() + "." + method.getName() + ". Handler should have one, and only one, parameter");
                } else {
                    if (handlesClass.isAssignableFrom(types[0])) {
                        Class<? extends T> handledType = (Class<? extends T>) types[0];
                        if (handlers.get(handledType) != null) {
                            throw new SubscriberConfigurationException("More than one handler with parameter " + handledType.getName() + " in subscriber " + clazz.getName() + ". Handlers should be non-ambiguous");
                        } else {
                            handlers.put(handledType, method);
                        }
                    }

                }
            }
        }
        return handlers;
    }
}