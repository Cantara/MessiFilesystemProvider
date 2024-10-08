package no.cantara.messi.avro;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.protos.MessiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AvroMessiClient implements MessiClient {

    static final Logger LOG = LoggerFactory.getLogger(AvroMessiClient.class);

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    protected final Path tmpFileFolder;
    protected final long avroMaxSeconds;
    protected final long avroMaxBytes;
    protected final int avroSyncInterval;
    protected final int fileListingMinIntervalSeconds;

    protected final Map<String, AvroMessiTopic> topicByName = new ConcurrentHashMap<>();

    protected final AvroMessiUtils readOnlyAvroMessiUtils;
    protected final AvroMessiUtils readWriteAvroMessiUtils;
    protected final String providerTechnology;

    public AvroMessiClient(Path tmpFileFolder,
                           long avroMaxSeconds,
                           long avroMaxBytes,
                           int avroSyncInterval,
                           int fileListingMinIntervalSeconds,
                           AvroMessiUtils readOnlyAvroMessiUtils,
                           AvroMessiUtils readWriteAvroMessiUtils,
                           String providerTechnology) {
        this.tmpFileFolder = tmpFileFolder;
        this.avroMaxSeconds = avroMaxSeconds;
        this.avroMaxBytes = avroMaxBytes;
        this.avroSyncInterval = avroSyncInterval;
        this.fileListingMinIntervalSeconds = fileListingMinIntervalSeconds;
        this.readOnlyAvroMessiUtils = readOnlyAvroMessiUtils;
        this.readWriteAvroMessiUtils = readWriteAvroMessiUtils;
        this.providerTechnology = providerTechnology;
    }

    @Override
    public abstract AvroMessiTopic topicOf(String name);

    @Override
    public AvroMessiCursor.Builder cursorOf() {
        return new AvroMessiCursor.Builder();
    }

    @Override
    public MessiMessage lastMessage(String topic, String shardId) throws MessiClosedException {
        AvroMessiTopic messiTopic = topicOf(topic);
        AvroMessiShard messiShard = messiTopic.shardOf(shardId);
        return messiShard.lastMessage();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (AvroMessiTopic topic : topicByName.values()) {
                topic.close();
            }
            topicByName.clear();
        }
    }
}
