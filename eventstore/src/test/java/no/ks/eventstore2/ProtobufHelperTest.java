package no.ks.eventstore2;

import com.google.protobuf.Timestamp;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtobufHelperTest {

    @Test
    void TimestampConversion() {
        final DateTime now = DateTime.now();
        final Timestamp timestamp = ProtobufHelper.toTimestamp(now);
        final DateTime now2 = ProtobufHelper.fromTimestamp(timestamp);
        assertEquals(now, now2);
    }

    @Test
    void DateConversion() {
        final Date now = new Date();
        final Timestamp timestamp = ProtobufHelper.toTimestamp(now);
        final Date now2 = ProtobufHelper.fromTimestampToDate(timestamp);
        assertEquals(now, now2);
    }

    @Test
    void DateAndDateTime() {
        final Date now = new Date();
        final Timestamp timestamp = ProtobufHelper.toTimestamp(now);
        final DateTime now2 = ProtobufHelper.fromTimestamp(timestamp);
        assertEquals(now, now2.toDate());

    }
}
