package no.ks.eventstore2.json;

import com.google.gson.*;
import org.joda.time.DateTime;

import java.lang.reflect.Type;

public class DateTimeTypeConverter implements JsonSerializer<DateTime>, JsonDeserializer<DateTime> {

    public JsonElement serialize(DateTime src, Type srcType, JsonSerializationContext context) {
        return new JsonPrimitive(src.toString());
    }

    public DateTime deserialize(JsonElement json, Type type, JsonDeserializationContext context) {
        return new DateTime(json.getAsString());
    }
}