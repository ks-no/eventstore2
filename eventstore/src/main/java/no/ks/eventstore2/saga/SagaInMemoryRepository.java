package no.ks.eventstore2.saga;

import java.util.HashMap;
import java.util.Map;

public class SagaInMemoryRepository extends SagaRepository{
	Map<SagaCompositeId, Byte> map = new HashMap<SagaCompositeId, Byte>();

    @Override
    public void saveState(Class<? extends Saga> clz, String sagaid, byte state) {
        map.put(new SagaCompositeId(clz, sagaid), state);
    }

    @Override
    public byte getState(Class<? extends Saga> clz, String sagaid) {
        return  (map.containsKey(new SagaCompositeId(clz, sagaid)) ? map.get(new SagaCompositeId(clz, sagaid)) : Saga.STATE_INITIAL);
    }

    @Override
    public void close() {

    }

    @Override
    public void open() {

    }

    @Override
    public void readAllStatesToNewRepository(SagaRepository repository) {

    }
}
