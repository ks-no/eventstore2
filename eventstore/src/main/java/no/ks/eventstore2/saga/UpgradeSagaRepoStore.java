package no.ks.eventstore2.saga;

import java.io.Serializable;

public class UpgradeSagaRepoStore implements Serializable{

    private SagaRepository sagaRepository;

    public UpgradeSagaRepoStore() {
    }

    public UpgradeSagaRepoStore(SagaRepository sagaRepository) {
        this.sagaRepository = sagaRepository;
    }

    public SagaRepository getSagaRepository() {
        return sagaRepository;
    }

    public void setSagaRepository(SagaRepository sagaRepository) {
        this.sagaRepository = sagaRepository;
    }
}
