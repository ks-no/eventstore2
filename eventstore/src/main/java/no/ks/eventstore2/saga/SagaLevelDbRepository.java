package no.ks.eventstore2.saga;

import no.ks.eventstore2.store.LevelDbStore;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class SagaLevelDbRepository extends SagaRepository {

    private final LevelDbStore levelDbStore;

    public SagaLevelDbRepository(String directory) {
        levelDbStore = new LevelDbStore(directory, 10);
    }

    public SagaLevelDbRepository(LevelDbStore levelDbStore) {
        this.levelDbStore = levelDbStore;
    }

    private String getKey(Class<? extends Saga> clz, String sagaid){
        return "sagarepo!" + clz.getName() + "!" + sagaid;
    }

    @Override
    public void saveState(Class<? extends Saga> clz, String sagaid, byte state) {
        byte[] statearray = {state};
        levelDbStore.getDb().put(bytes(getKey(clz,sagaid)), statearray);
    }

    @Override
    public byte getState(Class<? extends Saga> clz, String sagaid) {
        return levelDbStore.getDb().get(bytes(getKey(clz, sagaid)))[0];
    }

    @Override
    public void close() {
        levelDbStore.close();
    }

    @Override
    public void open() {
        levelDbStore.openDb();
    }
}
