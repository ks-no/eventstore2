package no.ks.eventstore2.json;

import java.lang.reflect.Type;

public class Adapter {

    private Type type;
    private Object typeAdapter;

    public Adapter(Type type, Object adapter) {
        this.type = type;
        this.typeAdapter = adapter;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Object getTypeAdapter() {
        return typeAdapter;
    }

    public void setTypeAdapter(Object typeAdapter) {
        this.typeAdapter = typeAdapter;
    }
}
