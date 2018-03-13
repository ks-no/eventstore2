package no.ks.eventstore2.saga;

import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.UntypedActor;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TimeOutStore extends UntypedActor {

    private static Logger log = LoggerFactory.getLogger(TimeOutStore.class);
    private SagaRepository repository;
    private Cancellable cancellable;

    public TimeOutStore(SagaRepository repository) {
        this.repository = repository;
    }

    @Override
    public void preStart() throws Exception {
        cancellable = startScheduledAwake();
    }

    private Cancellable startScheduledAwake() {
        return getContext().system().scheduler().schedule(Duration.create(10, TimeUnit.SECONDS), Duration.create(3, TimeUnit.SECONDS), self(), "awake", getContext().system().dispatcher(), self());
    }

    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
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
    public void postStop() throws Exception {
        cancellable.cancel();
        cancellable = null;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Messages.ScheduleAwake) {
            repository.storeScheduleAwake(((Messages.ScheduleAwake) message).getSagaid().getId(), ((Messages.ScheduleAwake) message).getSagaid().getClazz(), ProtobufHelper.fromTimestamp(((Messages.ScheduleAwake) message).getAwake()));
        } else if (message instanceof Messages.ClearAwake) {
            repository.clearAwake(((Messages.ClearAwake) message).getSagaid().getId(), ((Messages.ClearAwake) message).getSagaid().getClazz());
        } else if ("awake".equals(message)) {
            final List<SagaCompositeId> sagaCompositeIds = repository.whoNeedsToWake();
            log.info("Awoke {} sagas", sagaCompositeIds.size());
            sagaCompositeIds.stream().forEach((v) -> {
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
    }

    public static Props mkProps(SagaRepository repository) {
        return Props.create(TimeOutStore.class, repository);
    }
}
