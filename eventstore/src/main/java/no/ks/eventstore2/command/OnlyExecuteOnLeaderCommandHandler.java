package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.cluster.ClusterEvent;
import no.ks.eventstore2.AkkaClusterInfo;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public abstract class OnlyExecuteOnLeaderCommandHandler extends CommandHandler {

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

	// TODO: Trenger vi denne? Hvis ja, test at leader greiene fungerer som forventet
	@Override
	public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
		super.aroundReceive(receive, msg);

		if(akkaClusterInfo.isLeader() || "HandlesClasses".equals(msg)) {
			super.createReceive().onMessage().apply(msg);
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ClusterEvent.LeaderChanged.class, o -> akkaClusterInfo.updateLeaderState(o))
				.build();
	}
}
