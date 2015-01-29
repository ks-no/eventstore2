package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.joda.time.DateTime;

import java.util.*;

public class EventStoreKryo extends Kryo {

    public EventStoreKryo() {
        super();
        setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        setDefaultSerializer(CompatibleFieldSerializer.class);

        register(ArrayList.class, 100);
        register(HashMap.class, new MapSerializer());
        register(HashMap.class, 101);
        register(DateTime.class, new JodaDateTimeSerializer());
        register(DateTime.class, 102);
        register(Multimap.class, 103);
        register(HashMultimap.class, 104);
        register(Set.class, 105);
        register(Collection.class, 106);
        register(Object[].class, 107);
        register(TreeMap.class, new MapSerializer());
        register(TreeMap.class, 108);
        register(TreeSet.class, 109);
    }
}
