package no.ks.eventstore2.json;

import com.google.gson.*;
import org.joda.time.DateTime;

import java.lang.reflect.Type;
import java.util.Date;

public class DateTimeTypeConverter implements JsonSerializer<DateTime>, JsonDeserializer<DateTime> {

    public JsonElement serialize(DateTime src, Type srcType, JsonSerializationContext context) {
        return new JsonPrimitive(src.toString());
    }

    public DateTime deserialize(JsonElement json, Type type, JsonDeserializationContext context) {
        DateTime dateTime =null;
        try{
            dateTime = new DateTime(json.getAsString());
        } catch(Exception e){
            String str = json.getAsString();
            Date date = new Date(str);
            dateTime = new DateTime(date);
        }
        return dateTime;
    }
}