package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.cluster.ClusterEvent;
import no.ks.eventstore2.AkkaClusterInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OnlyExecuteOnLeaderCommandHandler extends CommandHandler {

	public static final Logger log = LoggerFactory.getLogger(OnlyExecuteOnLeaderCommandHandler.class);

    private AkkaClusterInfo akkaClusterInfo;

    public OnlyExecuteOnLeaderCommandHandler(ActorRef eventStore) {
		super(eventStore);
	}

	@Override
	public void preStart() throws Exception {
        akkaClusterInfo = new AkkaClusterInfo(getContext().system());
        akkaClusterInfo.subscribeToClusterEvents(self());
		akkaClusterInfo.updateLeaderState(null);
		super.preStart();
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ClusterEvent.LeaderChanged.class, event -> akkaClusterInfo.updateLeaderState(event))
				.matchEquals("HandlesClasses", this::handleHandlesClasses)
				.match(Command.class, command -> {
					if (akkaClusterInfo.isLeader()) {
						this.handleCommand(command);
					} else {
						log.debug("Not handling command {}, because node is not leader", command);
					}
				})
				.build();
	}
}
