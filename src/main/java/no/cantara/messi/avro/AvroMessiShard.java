package no.cantara.messi.avro;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiProvider;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class AvroMessiShard implements MessiShard {

    static final Logger LOG = LoggerFactory.getLogger(AvroMessiShard.class);

    protected final AvroMessiTopic topic;
    protected final String topicName;
    protected final String shardId;
    protected final AvroMessiUtils readOnlyAvroMessiUtils;
    protected final int fileListingMinIntervalSeconds;
    protected final String providerTechnology;

    protected final List<AvroMessiStreamingConsumer> consumers = new CopyOnWriteArrayList<>();
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public AvroMessiShard(AvroMessiTopic topic, String topicName, String shardId, AvroMessiUtils readOnlyAvroMessiUtils, int fileListingMinIntervalSeconds, String providerTechnology) {
        this.topic = topic;
        this.topicName = topicName;
        this.shardId = shardId;
        this.readOnlyAvroMessiUtils = readOnlyAvroMessiUtils;
        this.fileListingMinIntervalSeconds = fileListingMinIntervalSeconds;
        this.providerTechnology = providerTechnology;
    }

    @Override
    public boolean supportsStreaming() {
        return true;
    }

    @Override
    public MessiStreamingConsumer streamingConsumer(MessiCursor initialPosition) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        AvroMessiStreamingConsumer consumer = new AvroMessiStreamingConsumer(this, readOnlyAvroMessiUtils, topicName, (AvroMessiCursor) initialPosition, fileListingMinIntervalSeconds, providerTechnology);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public AvroMessiCursor.Builder cursorOf() {
        return new AvroMessiCursor.Builder();
    }

    @Override
    public AvroMessiCursor cursorOfCheckpoint(String checkpoint) {
        return new AvroMessiCursor.Builder()
                .checkpoint(checkpoint)
                .build();
    }

    @Override
    public AvroMessiCursor cursorAt(MessiMessage messiMessage) {
        if (!messiMessage.hasUlid()) {
            throw new IllegalArgumentException("Message is missing ulid");
        }
        if (!messiMessage.hasProvider()) {
            throw new IllegalArgumentException("Message is missing provider");
        }
        MessiProvider provider = messiMessage.getProvider();
        if (!provider.hasSequenceNumber()) {
            throw new IllegalArgumentException("Message provider is missing sequence-number");
        }
        String sequenceNumber = provider.getSequenceNumber();
        return new AvroMessiCursor.Builder()
                .providerSequenceNumber(sequenceNumber)
                .ulid(MessiULIDUtils.toUlid(messiMessage.getUlid()))
                .inclusive(true)
                .build();
    }

    @Override
    public AvroMessiCursor cursorAfter(MessiMessage messiMessage) {
        if (!messiMessage.hasUlid()) {
            throw new IllegalArgumentException("Message is missing ulid");
        }
        if (!messiMessage.hasProvider()) {
            throw new IllegalArgumentException("Message is missing provider");
        }
        MessiProvider provider = messiMessage.getProvider();
        if (!provider.hasSequenceNumber()) {
            throw new IllegalArgumentException("Message provider is missing sequence-number");
        }
        String sequenceNumber = provider.getSequenceNumber();
        return new AvroMessiCursor.Builder()
                .providerSequenceNumber(sequenceNumber)
                .ulid(MessiULIDUtils.toUlid(messiMessage.getUlid()))
                .inclusive(false)
                .build();
    }

    @Override
    public AvroMessiCursor cursorAtLastMessage() throws MessiClosedException {
        MessiMessage lastMessage = lastMessage();
        if (lastMessage == null) {
            return null; // stream is empty
        }
        return cursorAt(lastMessage);
    }

    @Override
    public AvroMessiCursor cursorAfterLastMessage() throws MessiClosedException {
        MessiMessage lastMessage = lastMessage();
        if (lastMessage == null) {
            // stream is empty, read from beginning to ensure that next message produced is included
            return cursorAtTrimHorizon();
        }
        return cursorAfter(lastMessage);
    }

    @Override
    public AvroMessiCursor cursorHead() {
        return new AvroMessiCursor.Builder()
                .now()
                .build();
    }

    @Override
    public AvroMessiCursor cursorAtTrimHorizon() {
        return new AvroMessiCursor.Builder()
                .oldest()
                .build();
    }

    @Override
    public AvroMessiTopic topic() {
        return topic;
    }

    @Override
    public void close() {
        closed.set(true);
        for (AvroMessiStreamingConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
    }

    public MessiMessage lastMessage() throws MessiClosedException {
        NavigableMap<Long, MessiAvroFile> topicBlobs = readOnlyAvroMessiUtils.getTopicBlobs(topicName);
        if (topicBlobs.isEmpty()) {
            return null;
        }
        MessiAvroFile messiAvroFile = topicBlobs.lastEntry().getValue();
        LOG.debug("Reading last message from MessiAvroFile: {}", messiAvroFile);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(AvroMessiMessageSchema.schema);
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<>(messiAvroFile.seekableInput(), datumReader);
            dataFileReader.seek(messiAvroFile.getOffsetOfLastBlock());
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
            }
            if (record == null) {
                return null;
            } else {
                MessiMessage.Builder builder = AvroMessiMessageSchema.toMessage(record);
                AvroMessiMessageSchema.deserializePostProcessing(builder, providerTechnology);
                MessiMessage message = builder.build();
                return message;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (dataFileReader != null) {
                try {
                    dataFileReader.close();
                } catch (IOException e) {
                    LOG.warn("", e);
                }
            }
        }
    }
}
