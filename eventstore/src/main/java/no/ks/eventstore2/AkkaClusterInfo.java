package no.ks.eventstore2;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.MemberStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AkkaClusterInfo {
	private static Logger log = LoggerFactory.getLogger(AkkaClusterInfo.class);

    private ActorSystem system;
    private Address leaderAdress;
    private boolean leader;
    private ActorRef subscriber;

    public AkkaClusterInfo(ActorSystem system) {
        this.system = system;
    }

    public void subscribeToClusterEvents(ActorRef subscriber) {
        this.subscriber = subscriber;
        try {
            Cluster cluster = Cluster.get(system);
            cluster.subscribe(subscriber, ClusterEvent.ClusterDomainEvent.class);
            log.info("{} subscribes to cluster events", subscriber);
        } catch (ConfigurationException e) {
        }
    }

    public void updateLeaderState(ClusterEvent.LeaderChanged leaderChanged) {
        try {
            Cluster cluster = Cluster.get(system);
            boolean oldLeader = leader;
            if (leaderChanged == null ) {
                boolean notReady = true;
                while (!cluster.readView().self().status().equals(MemberStatus.up())) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {}
                }
                leader = cluster.readView().isLeader();
            } else {
                leaderAdress = leaderChanged.getLeader();
                leader = cluster.readView().selfAddress().equals(leaderAdress);
            }
            log.info("{} leader changed from {} to {}",subscriber, oldLeader, leader);
            if(cluster.readView().leader().isDefined()) {
            	leaderAdress = cluster.readView().leader().get();
            }
            log.debug("leader adress {}", leaderAdress);

        } catch (ConfigurationException e) {
            log.debug("Not cluster system");
            leader = true;
        }
    }

    public Address getLeaderAdress() {
        return leaderAdress;
    }

    public boolean isLeader() {
        return leader;
    }

    public boolean amIUp() {
        try {
            Cluster cluster = Cluster.get(system);
            return cluster.readView().self().status().equals(MemberStatus.up());
        } catch (ConfigurationException e) {
            return true;
        }
    }
}
