package no.ks.eventstore2.saga;

import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TimeOutStore extends AbstractActor {

    private static Logger log = LoggerFactory.getLogger(TimeOutStore.class);
    private SagaRepository repository;
    private Cancellable cancellable;

    public TimeOutStore(SagaRepository repository) {
        this.repository = repository;
    }

    @Override
    public void preStart() {
        cancellable = startScheduledAwake();
    }

    private Cancellable startScheduledAwake() {
        return getContext().system().scheduler().schedule(Duration.create(10, TimeUnit.SECONDS), Duration.create(3, TimeUnit.SECONDS), self(), "awake", getContext().system().dispatcher(), self());
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
        super.preRestart(reason, message);
        cancellable.cancel();
        cancellable = null;
    }

    @Override
    public void aroundPostRestart(Throwable reason) {
        super.aroundPostRestart(reason);
        if(cancellable == null || cancellable.isCancelled()){
            cancellable = startScheduledAwake();
        }
    }

    @Override
    public void postStop() {
        cancellable.cancel();
        cancellable = null;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.ScheduleAwake.class, this::handleScheduleAwake)
                .match(Messages.ClearAwake.class, this::handleClearAwake)
                .matchEquals("awake", this::handleAwake)
                .build();
    }

    private void handleScheduleAwake(Messages.ScheduleAwake message) {
        repository.storeScheduleAwake(message.getSagaid().getId(), message.getSagaid().getClazz(), ProtobufHelper.fromTimestamp(message.getAwake()));
    }

    private void handleClearAwake(Messages.ClearAwake message) {
        repository.clearAwake(message.getSagaid().getId(), message.getSagaid().getClazz());
    }

    private void handleAwake(Object o) {
        final List<SagaCompositeId> sagaCompositeIds = repository.whoNeedsToWake();
        log.info("Awoke {} sagas", sagaCompositeIds.size());
        sagaCompositeIds.forEach((v) -> {
                    final Messages.SagaCompositeId.Builder builder = Messages.SagaCompositeId.newBuilder();
                    builder.setClazz(v.getClz().getName());
                    builder.setId(v.getId());
                    final Messages.SagaCompositeId sagaid = builder.build();
                    final Messages.SendAwake sendAwake = Messages.SendAwake.newBuilder().setSagaid(sagaid).build();
                    repository.clearAwake(v.getId(), v.getClz().getName());
                    getContext().parent().tell(sendAwake, self());
                }
        );
    }

    public static Props mkProps(SagaRepository repository) {
        return Props.create(TimeOutStore.class, repository);
    }
}
