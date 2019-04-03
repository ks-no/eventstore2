package no.ks.eventstore2.projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;

public class ProjectionError {

    private static Logger log = LoggerFactory.getLogger(ProjectionError.class);

    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String event;
    private String projection;
    private String reason = "";
    private String date;

    public ProjectionError(ProjectionFailedError error) {
        this.event = error.getMessage().toString();
        this.projection = error.getProjection().toString();
        reason = joinStackTrace(error.getReason());
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getProjection() {
        return projection;
    }

    public void setProjection(String projection) {
        this.projection = projection;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    private static String joinStackTrace(Throwable e) {
        StringWriter writer = null;
        try {
            writer = new StringWriter();
            joinStackTrace(e, writer);
            return writer.toString();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e1) {
                    log.warn("Could not close close writer", e);
                }
            }
        }
    }

    private static void joinStackTrace(Throwable e, StringWriter writer) {
        PrintWriter printer = null;
        try {
            printer = new PrintWriter(writer);

            while (e != null) {

                printer.println(e);
                StackTraceElement[] trace = e.getStackTrace();
                for (int i = 0; i < trace.length; i++) {
                    printer.println("\tat " + trace[i]);
                }

                e = e.getCause();
                if (e != null) {
                    printer.println("\r\n Caused by:" + e);
                }
            }
        } finally {
            if (printer != null) {
                printer.close();
            }
        }
    }
}
