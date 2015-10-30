package no.ks.eventstore2.ask;


import akka.actor.ActorRef;
import no.ks.eventstore2.command.Command;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.util.Failure;

import static akka.pattern.Patterns.ask;
import static no.ks.eventstore2.projection.CallProjection.call;

public class Asker {

    private static final int THREE_SECONDS = 3000;
    private static final int TEN_SECONDS = 10000;

    private static final String THREE_SECONDS_STR = "3 seconds";
    private static final String TEN_SECONDS_STR = "10 seconds";

    private Asker(){}

    public static ObjectConverter askProjection(ActorRef projection, String method, Object... args) throws Exception {
        if(isNotNull(projection, method, args)) {
            Future<Object> future = ask(projection, call(method, args), THREE_SECONDS);
            return new ObjectConverter(Await.result(future, Duration.create(THREE_SECONDS_STR)));
        }
        throw new RuntimeException("Unexpected error occurred");
    }

    public static void dispatchCommand(ActorRef commandDispatcher, Command command) {
        Future<Object> future = ask(commandDispatcher, command, TEN_SECONDS);
        try {
            Object result = Await.result(future, Duration.create(TEN_SECONDS_STR));
            if (result instanceof Failure) {
                throw ((Failure) result).exception();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException("Could not dispatch command", e);
        }
    }

    private static boolean isNotNull(ActorRef projection, String method, Object... args) {
        if(projection == null) {
            throw new RuntimeException("Projection actor-ref can't be null when trying to call method '" + method + "'");
        } else if(method == null) {
            throw new RuntimeException("Name of method can't be null when calling the projection actor-ref '" + projection + "'");
        } else if(args != null && isOneArgumentNull(args)) {
            throw new RuntimeException("Arguments for the method '" + method + "' can't be null " + getErrorMessage(args));
        }
        return true;
    }

    private static boolean isOneArgumentNull(Object[] args) {
        boolean isNull = false;
        for(Object arg : args) {
            if(arg == null) {
                isNull = true;
            }
        }
        return isNull;
    }

    private static String getErrorMessage(Object[] args) {
        String error = "args=[";
        for(Object arg : args) {
            error += " '" + arg + "' ";
        }
        error += "]";
        return error;
    }
}