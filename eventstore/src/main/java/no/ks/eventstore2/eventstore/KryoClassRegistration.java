package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;

public interface KryoClassRegistration {
    void registerClasses(Kryo kryo);
}
