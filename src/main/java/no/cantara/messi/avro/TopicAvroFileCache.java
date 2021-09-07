package no.cantara.messi.avro;

import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class TopicAvroFileCache {

    final AvroMessiUtils avroMessiUtils;
    final String topic;
    final int minListingIntervalSeconds;

    final AtomicReference<NavigableMap<Long, MessiAvroFile>> topicBlobsByFromTimestampRef = new AtomicReference<>();
    final AtomicLong timestampOfLastListing = new AtomicLong(0);

    TopicAvroFileCache(AvroMessiUtils avroMessiUtils, String topic, int minListingIntervalSeconds) {
        this.avroMessiUtils = avroMessiUtils;
        this.topic = topic;
        this.minListingIntervalSeconds = minListingIntervalSeconds;
    }

    NavigableMap<Long, MessiAvroFile> blobsByTimestamp() {
        if ((System.currentTimeMillis() - timestampOfLastListing.get()) >= TimeUnit.SECONDS.toMillis(minListingIntervalSeconds)) {
            // refresh entire cache by listing all files in topic
            topicBlobsByFromTimestampRef.set(avroMessiUtils.getTopicBlobs(topic));
            timestampOfLastListing.set(System.currentTimeMillis());
        }
        return topicBlobsByFromTimestampRef.get();
    }

    void clear() {
        topicBlobsByFromTimestampRef.set(null);
        timestampOfLastListing.set(0);
    }
}
