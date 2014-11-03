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

    public static ObjectConverter askProjection(ActorRef projection, String method, Object... args) throws Exception {
        Future<Object> future = ask(projection, call(method, args), THREE_SECONDS);
        return new ObjectConverter(Await.result(future, Duration.create(THREE_SECONDS_STR)));
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
}