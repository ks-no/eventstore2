package no.ks.eventstore2.eventstore;

import akka.actor.ActorRef;
import akka.actor.Props;
import eventstore.EventNumber;
import eventstore.EventStream;
import eventstore.StreamSubscriptionActor;
import eventstore.j.SettingsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public final class EventStoreUtil {

    private static final Logger log = LoggerFactory.getLogger(EventStoreUtil.class);

    private EventStoreUtil() {}

    public static Props getCategorySubscriptionsProps(ActorRef connection, ActorRef client, String category) {
        return getCategorySubscriptionsProps(connection, client, category, null);
    }

    public static Props getCategorySubscriptionsProps(ActorRef connection, ActorRef client, String category, Long fromId) {
        EventStream.Id streamId = new EventStream.System("ce-" + category);
        Option<EventNumber> eventNumber = getEventNumber(fromId);
        log.debug("Built StreamSubscriptionActor for stream id \"{}\" from event number {}", streamId.streamId(), eventNumber);
        return StreamSubscriptionActor.props(
                connection,
                client,
                streamId,
                eventNumber,
                Option.empty(),
                new SettingsBuilder().resolveLinkTos(true).build());
    }

    private static Option<EventNumber> getEventNumber(Long fromId) {
        if (fromId == null || fromId < 0) {
            return Option.empty();
        }
        return Option.apply(new EventNumber.Exact(fromId));
    }
}
