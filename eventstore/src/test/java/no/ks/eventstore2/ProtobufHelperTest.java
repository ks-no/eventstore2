package no.ks.eventstore2;

import com.google.protobuf.Timestamp;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class ProtobufHelperTest {

    @Test
    public void TimestampConversion() throws Exception {
        final DateTime now = DateTime.now();
        final Timestamp timestamp = ProtobufHelper.toTimestamp(now);
        final DateTime now2 = ProtobufHelper.fromTimestamp(timestamp);
        assertEquals(now, now2);
    }

    @Test
    public void DateConversion() throws Exception {
        final Date now = new Date();
        final Timestamp timestamp = ProtobufHelper.toTimestamp(now);
        final Date now2 = ProtobufHelper.fromTimestampToDate(timestamp);
        assertEquals(now, now2);
    }

    @Test
    public void DateAndDateTime() throws Exception {
        final Date now = new Date();
        final Timestamp timestamp = ProtobufHelper.toTimestamp(now);
        final DateTime now2 = ProtobufHelper.fromTimestamp(timestamp);
        assertEquals(now, now2.toDate());

    }
}
