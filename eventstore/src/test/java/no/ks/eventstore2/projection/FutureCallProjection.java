package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import no.ks.eventstore2.eventstore.Subscription;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.Callable;

import static akka.dispatch.Futures.future;

public class FutureCallProjection extends Projection {

    public FutureCallProjection(ActorRef eventStore) {
        super(eventStore);
    }

    @Override
    protected Subscription getSubscribe() {
        return new Subscription("agg");
    }

    public Future<String> getString(){
        ExecutionContext ec = getContext().system().dispatcher();
        return future(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "OK";
            }
        }, ec);
    }

    public Future<String> getFailure(){
        ExecutionContext ec = getContext().system().dispatcher();
        return future(new Callable<String>() {
            @Override
            public String call() throws Exception {
                throw new RuntimeException("Failing");
            }
        }, ec);
    }

    public int getInt(final int test){
        return test;
    }
}

