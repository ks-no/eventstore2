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

/**
 * Created with IntelliJ IDEA.
 * User: idar
 * Date: 1/31/13
 * Time: 7:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class AkkaClusterInfo {

    ActorSystem system;
    private static final Logger log = LoggerFactory.getLogger(AkkaClusterInfo.class);
    private Address leaderAdress;
    private boolean leader;

    public AkkaClusterInfo(ActorSystem system) {
        this.system = system;
    }

    public void subscribeToClusterEvents(ActorRef subscriber) {
        try {
            Cluster cluster = Cluster.get(system);
            cluster.subscribe(subscriber, ClusterEvent.ClusterDomainEvent.class);
            log.info("{} subscribes to cluster events", subscriber);
        } catch (ConfigurationException e) {
        }
    }

    public void updateLeaderState() {
        try {
            Cluster cluster = Cluster.get(system);
            boolean notReady = true;
            while (!cluster.readView().self().status().equals(MemberStatus.up())) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {

                }
            }
            boolean oldLeader = leader;
            leader = cluster.readView().isLeader();
            log.info("leader changed from {} to {}", oldLeader,leader);
            leaderAdress = cluster.readView().leader().get();
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
