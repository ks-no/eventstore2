package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import no.ks.eventstore2.eventstore.Subscription;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

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
        return future(() -> "OK", ec);
    }

    public Future<String> getFailure(){
        ExecutionContext ec = getContext().system().dispatcher();
        return future(() -> {throw new RuntimeException("Failing");}, ec);
    }
}
