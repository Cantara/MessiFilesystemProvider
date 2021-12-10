package no.cantara.messi.avro;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AvroMessiTopic implements MessiTopic {

    static final Logger LOG = LoggerFactory.getLogger(AvroMessiTopic.class);

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
    protected final String providerTechnology;

    protected AvroMessiTopic(String name, Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, AvroMessiUtils readOnlyAvroMessiUtils, AvroMessiUtils readWriteAvroMessiUtils, int fileListingMinIntervalSeconds, String providerTechnology) {
        this.name = name;
        this.tmpFileFolder = tmpFileFolder;
        this.avroMaxSeconds = avroMaxSeconds;
        this.avroMaxBytes = avroMaxBytes;
        this.avroSyncInterval = avroSyncInterval;
        this.readOnlyAvroMessiUtils = readOnlyAvroMessiUtils;
        this.readWriteAvroMessiUtils = readWriteAvroMessiUtils;
        this.fileListingMinIntervalSeconds = fileListingMinIntervalSeconds;
        this.theShard = new AvroMessiShard(name, firstShard(), this.readOnlyAvroMessiUtils, this.fileListingMinIntervalSeconds, providerTechnology);
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
        AvroMessiProducer producer = new AvroMessiProducer(readWriteAvroMessiUtils, tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, name, providerTechnology);
        producers.add(producer);
        return producer;
    }

    @Override
    public AvroMessiShard shardOf(String shardId) {
        return theShard;
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
