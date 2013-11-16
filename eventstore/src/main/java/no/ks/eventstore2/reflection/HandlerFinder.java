package no.ks.eventstore2.reflection;

import no.ks.eventstore2.Event;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.command.Command;
import no.ks.eventstore2.command.CommandHandler;

import java.lang.reflect.Method;
import java.util.HashMap;

public class HandlerFinder {

    public static HashMap<Class<? extends Event>, Method> getEventHandlers(Class clazz) {
        return getHandlers(clazz, Event.class);
    }

    public static HashMap<Class<? extends Command>, Method> getCommandHandlers(Class<? extends CommandHandler> clazz) {
        return getHandlers(clazz, Command.class);
    }

    public static <T> HashMap<Class<? extends T>, Method> getHandlers(Class clazz, Class<T> handlesClass) {
        HashMap<Class<? extends T>, Method> handlers = new HashMap<Class<? extends T>, Method>();
        for (Method method : clazz.getMethods()) {
            Handler handlerAnnotation = method.getAnnotation(Handler.class);

            if (handlerAnnotation != null) {
                Class<?>[] types = method.getParameterTypes();
                if (types.length != 1)
                    throw new RuntimeException("Invalid handler signature " + method.getName());
                else {
                    if (!handlesClass.isAssignableFrom(types[0])) {
                        throw new RuntimeException("Invalid handler signature " + method.getName());
                    } else {
                        handlers.put((Class<? extends T>) types[0], method);
                    }

                }
            }
        }
        return handlers;
    }
}