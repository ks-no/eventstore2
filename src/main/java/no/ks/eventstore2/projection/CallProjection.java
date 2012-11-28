package no.ks.eventstore2.projection;

public class CallProjection {

    public static Call call(String method, Object ... args){
        return new Call(method, args);
    }
}
