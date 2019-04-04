package no.ks.eventstore2.ask;

import no.ks.eventstore2.response.NoResult;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ObjectConverter {

    private Object object;

    public ObjectConverter(Object object) {
        this.object = object;
    }

    public <T> T single(Class<T> clz) {
        if(clz.isInstance(object)) {
            return (T) object;
        }
        return null;
    }

    public <T> List<T>  list(Class<T> clz) {
        return (List<T>) object;
    }

    public <T> Map<String, T> map(Class<T> clz) {
        if(object instanceof NoResult) {
            return null;
        }
        return (Map<String, T>) object;
    }

    public <K, V> Map<K, V> map(Class<K> keyClz, Class<V> valueClz) {
        return (Map<K, V>) object;
    }
}