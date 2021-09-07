package no.cantara.messi.avro;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.NavigableMap;

public interface AvroMessiUtils {

    NavigableMap<Long, MessiAvroFile> getTopicBlobs(String topic);

    AvroFileMetadata newAvrofileMetadata();

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    static String formatTimestamp(long timestamp) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneOffset.UTC);
        return zonedDateTime.format(dateTimeFormatter);
    }

    static long parseTimestamp(String timestamp) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestamp, dateTimeFormatter);
        return zonedDateTime.toInstant().toEpochMilli();
    }

    /**
     * Code copied from article posted by https://stackoverflow.com/users/276052/aioobe :
     * https://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
     *
     * @param bytes
     * @param si
     * @return
     */
    static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
