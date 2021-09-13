package no.cantara.messi.avro;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.protos.MessiMessage;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AvroMessiClient implements MessiClient {

    static final Logger LOG = LoggerFactory.getLogger(AvroMessiClient.class);

    final AtomicBoolean closed = new AtomicBoolean(false);

    final Path tmpFileFolder;
    final long avroMaxSeconds;
    final long avroMaxBytes;
    final int avroSyncInterval;
    final int fileListingMinIntervalSeconds;

    final List<AvroMessiProducer> producers = new CopyOnWriteArrayList<>();
    final List<AvroMessiConsumer> consumers = new CopyOnWriteArrayList<>();
    final AvroMessiUtils readOnlyAvroMessiUtils;
    final AvroMessiUtils readWriteAvroMessiUtils;

    public AvroMessiClient(Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, int fileListingMinIntervalSeconds, AvroMessiUtils readOnlyAvroMessiUtils, AvroMessiUtils readWriteAvroMessiUtils) {
        this.tmpFileFolder = tmpFileFolder;
        this.avroMaxSeconds = avroMaxSeconds;
        this.avroMaxBytes = avroMaxBytes;
        this.avroSyncInterval = avroSyncInterval;
        this.fileListingMinIntervalSeconds = fileListingMinIntervalSeconds;
        this.readOnlyAvroMessiUtils = readOnlyAvroMessiUtils;
        this.readWriteAvroMessiUtils = readWriteAvroMessiUtils;
    }

    @Override
    public MessiProducer producer(String topic) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        AvroMessiProducer producer = new AvroMessiProducer(readWriteAvroMessiUtils, tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public MessiConsumer consumer(String topic, MessiCursor cursor) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        AvroMessiConsumer consumer = new AvroMessiConsumer(readOnlyAvroMessiUtils, topic, (AvroMessiCursor) cursor, fileListingMinIntervalSeconds);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public AvroMessiCursor.Builder cursorOf() {
        return new AvroMessiCursor.Builder();
    }

    @Override
    public MessiMessage lastMessage(String topic, String shardId) throws MessiClosedException {
        NavigableMap<Long, MessiAvroFile> topicBlobs = readOnlyAvroMessiUtils.getTopicBlobs(topic);
        if (topicBlobs.isEmpty()) {
            return null;
        }
        MessiAvroFile messiAvroFile = topicBlobs.lastEntry().getValue();
        LOG.debug("Reading last message from MessiAvroFile: {}", messiAvroFile);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(AvroMessiMessageSchema.schema);
        DataFileReader<GenericRecord> dataFileReader;
        try {
            dataFileReader = new DataFileReader<>(messiAvroFile.seekableInput(), datumReader);
            dataFileReader.seek(messiAvroFile.getOffsetOfLastBlock());
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
            }
            return record == null ? null : AvroMessiMessageSchema.toMessage(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (AvroMessiProducer producer : producers) {
                producer.close();
            }
            producers.clear();
            for (AvroMessiConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
        }
    }
}
