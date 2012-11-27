package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.eventstore.Subscription;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class Projection extends UntypedActor {

    protected ActorRef eventStore;

    protected Projection() {
        init();
    }

    @Override
    public void preStart(){
        System.out.println(getSelf().path().toString());
        eventStore = getContext().actorFor("akka://default/user/eventStore");
        subscribe(eventStore);
    }

    @Override
    public void onReceive(Object o) throws Exception{
        if (o instanceof Event)
            handleEvent((Event) o);
    }
    public void handleEvent(Event event) {
        Method method = handleEventMap.get(new ProjectionEventBind(getClass(), event.getClass()));
        if (method != null)
            try {
                method.invoke(this, event);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }

    private Map<ProjectionEventBind, Method> handleEventMap = null;

    private void init() {
        handleEventMap = new HashMap<ProjectionEventBind, Method>();
        try {
            Class<? extends Projection> projectionClass = (Class<? extends Projection>) this.getClass();
            ListensTo annotation = projectionClass.getAnnotation(ListensTo.class);
            if (annotation != null) {    // Compatibility with 'old' ebc code
                Class[] handledEventClasses = annotation.value();
                for (Class<? extends Event> handledEventClass : handledEventClasses) {
                    Method handleEventMethod = projectionClass.getMethod("handleEvent", handledEventClass);
                    handleEventMap.put(new ProjectionEventBind(projectionClass, handledEventClass), handleEventMethod);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

        protected void subscribe(ActorRef eventStore){
            ListensTo annotation = getClass().getAnnotation(ListensTo.class);
            if (annotation != null)
                for (String aggregate : annotation.aggregates())
                    eventStore.tell(new Subscription(aggregate), self());
        }



        private static class ProjectionEventBind {
            public final Class<? extends Projection> projectionClass;
            public final Class<? extends Event> eventClass;

            private ProjectionEventBind(Class<? extends Projection> projectionClass, Class<? extends Event> eventClass) {
                this.projectionClass = projectionClass;
                this.eventClass = eventClass;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof ProjectionEventBind)) return false;

                ProjectionEventBind that = (ProjectionEventBind) o;

                return !(eventClass != null ? !eventClass.equals(that.eventClass) : that.eventClass != null)
                        && !(projectionClass != null ? !projectionClass.equals(that.projectionClass) : that.projectionClass != null);
            }

            @Override
            public int hashCode() {
                int result = projectionClass != null ? projectionClass.hashCode() : 0;
                result = 31 * result + (eventClass != null ? eventClass.hashCode() : 0);
                return result;
            }
        }
    }










