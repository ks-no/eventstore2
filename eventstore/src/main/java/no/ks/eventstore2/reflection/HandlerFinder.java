package no.ks.eventstore2.reflection;

import no.ks.eventstore2.Event;
import no.ks.eventstore2.projection.EventHandler;
import no.ks.eventstore2.projection.Projection;

import java.lang.reflect.Method;
import java.util.HashMap;

public class HandlerFinder {

    public static HashMap<Class<? extends Event>, Method> getEventHandlers(Class clazz) {
        HashMap<Class<? extends Event>, Method> handlers = new HashMap<Class<? extends Event>, Method>();
        for (Method method : clazz.getMethods()) {
            EventHandler eventHandlerAnnotation = method.getAnnotation(EventHandler.class);

            if (eventHandlerAnnotation != null) {
                Class<?>[] types = method.getParameterTypes();
                if (types.length != 1)
                    throw new RuntimeException("Invalid handler signature " + method.getName());
                else {
                    if (!Event.class.isAssignableFrom(types[0])) {
                        throw new RuntimeException("Invalid handler signature " + method.getName());
                    } else {
                        handlers.put((Class<? extends Event>) types[0], method);
                    }

                }
            }
        }
        return handlers;
    }
}