package no.cantara.messi.avro;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AvroMessiTopic implements MessiTopic {

    static final Logger LOG = LoggerFactory.getLogger(AvroMessiTopic.class);

    protected final AvroMessiClient messiClient;
    protected final String name;
    protected final Path tmpFileFolder;
    protected final long avroMaxSeconds;
    protected final long avroMaxBytes;
    protected final int avroSyncInterval;
    protected final AvroMessiUtils readOnlyAvroMessiUtils;
    protected final AvroMessiUtils readWriteAvroMessiUtils;
    protected final int fileListingMinIntervalSeconds;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final List<AvroMessiProducer> producers = new CopyOnWriteArrayList<>();
    protected final AvroMessiShard theShard;
    protected final List<String> shards;
    protected final String providerTechnology;

    protected AvroMessiTopic(AvroMessiClient messiClient, String name, Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, AvroMessiUtils readOnlyAvroMessiUtils, AvroMessiUtils readWriteAvroMessiUtils, int fileListingMinIntervalSeconds, String providerTechnology) {
        this.messiClient = messiClient;
        this.name = name;
        this.tmpFileFolder = tmpFileFolder;
        this.avroMaxSeconds = avroMaxSeconds;
        this.avroMaxBytes = avroMaxBytes;
        this.avroSyncInterval = avroSyncInterval;
        this.readOnlyAvroMessiUtils = readOnlyAvroMessiUtils;
        this.readWriteAvroMessiUtils = readWriteAvroMessiUtils;
        this.fileListingMinIntervalSeconds = fileListingMinIntervalSeconds;
        this.theShard = new AvroMessiShard(this, name, firstShard(), this.readOnlyAvroMessiUtils, this.fileListingMinIntervalSeconds, providerTechnology);
        List<String> shards = new ArrayList<>(1);
        shards.add(firstShard());
        this.shards = shards;
        this.providerTechnology = providerTechnology;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public AvroMessiProducer producer() {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        AvroMessiProducer producer = new AvroMessiProducer(this, readWriteAvroMessiUtils, tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, name, providerTechnology);
        producers.add(producer);
        return producer;
    }

    @Override
    public List<String> shards() {
        return shards;
    }

    @Override
    public AvroMessiShard shardOf(String shardId) {
        return theShard;
    }

    @Override
    public AvroMessiClient client() {
        return messiClient;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        for (AvroMessiProducer producer : producers) {
            producer.close();
        }
        try {
            theShard.close();
        } catch (Exception e) {
            LOG.warn("", e);
        }
        producers.clear();
    }
}
