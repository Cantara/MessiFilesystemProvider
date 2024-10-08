package no.cantara.messi.avro;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiNoSuchExternalIdException;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Deque;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class AvroMessiStreamingConsumer implements MessiStreamingConsumer {

    final AvroMessiShard messiShard;
    final String topic;
    final String providerTechnology;
    final TopicAvroFileCache topicAvroFileCache;
    final AtomicReference<Long> activeBlobFromKeyRef = new AtomicReference<>(-1L);
    final AtomicReference<DataFileReader<GenericRecord>> activeBlobDataFileReaderRef = new AtomicReference<>(null);
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Deque<MessiMessage> preloadedMessages = new ConcurrentLinkedDeque<>();

    AvroMessiStreamingConsumer(AvroMessiShard messiShard,
                               AvroMessiUtils avroMessageUtils,
                               String topic,
                               AvroMessiCursor cursor,
                               int minFileListingIntervalSeconds,
                               String providerTechnology) {
        this.messiShard = messiShard;
        this.topic = topic;
        this.providerTechnology = providerTechnology;
        this.topicAvroFileCache = new TopicAvroFileCache(avroMessageUtils, topic, minFileListingIntervalSeconds);
        if (cursor == null) {
            seek(0);
        } else {
            AvroMessiCursor resolvedCursor = resolve(avroMessageUtils, minFileListingIntervalSeconds, cursor);
            seek(resolvedCursor.ulid.timestamp());
            try {
                MessiMessage msg;
                while ((msg = receive(0, TimeUnit.SECONDS)) != null) {
                    ULID.Value ulid = MessiULIDUtils.toUlid(msg.getUlid());
                    if (ulid.equals(resolvedCursor.ulid)) {
                        if (resolvedCursor.inclusive) {
                            preloadedMessages.addFirst(msg);
                        }
                        break; // found match
                    }
                    if (ulid.timestamp() > resolvedCursor.ulid.timestamp()) {
                        // past possible point of match, use this message as starting point
                        preloadedMessages.addFirst(msg);
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public AvroMessiCursor resolve(AvroMessiUtils avroMessageUtils, int minFileListingIntervalSeconds, AvroMessiCursor unresolvedCursor) {
        switch (unresolvedCursor.type) {
            case AT_ULID:
                return unresolvedCursor;
            case OLDEST_RETAINED:
                return new AvroMessiCursor.Builder()
                        .ulid(MessiULIDUtils.beginningOf(0L))
                        .inclusive(true)
                        .build();
            case NOW:
                return new AvroMessiCursor.Builder()
                        .ulid(MessiULIDUtils.beginningOf(System.currentTimeMillis()))
                        .inclusive(true)
                        .build();
            case AT_PROVIDER_SEQUENCE:
                return new AvroMessiCursor.Builder()
                        .ulid(ULID.parseULID(unresolvedCursor.sequenceNumber))
                        .inclusive(true)
                        .build();
            case AT_PROVIDER_TIME:
                return new AvroMessiCursor.Builder()
                        .ulid(MessiULIDUtils.beginningOf(unresolvedCursor.timestamp.toEpochMilli() + (unresolvedCursor.inclusive ? 0 : 1)))
                        .inclusive(true)
                        .build();
            case AT_EXTERNAL_ID:
                ULID.Value ulid = ulidOfExternalId(avroMessageUtils, minFileListingIntervalSeconds, topic, unresolvedCursor.externalId, unresolvedCursor.externalIdTimestamp.toEpochMilli(), unresolvedCursor.externalIdTimestampTolerance);
                if (ulid == null) {
                    throw new MessiNoSuchExternalIdException(String.format("externalId not found: %s", unresolvedCursor.externalId));
                }
                return new AvroMessiCursor.Builder()
                        .ulid(ulid)
                        .inclusive(unresolvedCursor.inclusive)
                        .build();
            default:
                throw new IllegalStateException("Type not implemented: " + unresolvedCursor.type);
        }
    }

    ULID.Value ulidOfExternalId(AvroMessiUtils readOnlyAvroMessiUtils,
                                int fileListingMinIntervalSeconds,
                                String topic, String externalId,
                                long approxTimestamp,
                                Duration tolerance) throws MessiNoSuchExternalIdException {

        ULID.Value lowerBoundUlid = MessiULIDUtils.beginningOf(approxTimestamp - tolerance.toMillis());
        ULID.Value upperBoundUlid = MessiULIDUtils.beginningOf(approxTimestamp + tolerance.toMillis());
        try (AvroMessiStreamingConsumer consumer = new AvroMessiStreamingConsumer(messiShard, readOnlyAvroMessiUtils, topic, new AvroMessiCursor.Builder().ulid(lowerBoundUlid).inclusive(true).build(), fileListingMinIntervalSeconds, providerTechnology)) {
            MessiMessage message;
            while ((message = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                ULID.Value messageUlid = MessiULIDUtils.toUlid(message.getUlid());
                if (messageUlid.timestamp() > upperBoundUlid.timestamp()) {
                    throw new MessiNoSuchExternalIdException(
                            String.format("Unable to find externalId, reached upper-bound. Time-range=[%s,%s), position=%s",
                                    formatTimestamp(lowerBoundUlid.timestamp()),
                                    formatTimestamp(upperBoundUlid.timestamp()),
                                    externalId));
                }
                if (externalId.equals(message.getExternalId())) {
                    return messageUlid; // found matching position
                }
            }
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new MessiNoSuchExternalIdException(
                String.format("Unable to find externalId, reached end-of-stream. Time-range=[%s,%s), position=%s",
                        formatTimestamp(lowerBoundUlid.timestamp()),
                        formatTimestamp(upperBoundUlid.timestamp()),
                        externalId));
    }

    static String formatTimestamp(long timestamp) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss.SSS");
        LocalDateTime dt = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneOffset.UTC);
        return dt.format(dtf);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit unit) throws InterruptedException, MessiClosedException {
        final long start = System.currentTimeMillis();
        MessiMessage preloadedMessage = preloadedMessages.poll();
        if (preloadedMessage != null) {
            return preloadedMessage;
        }
        DataFileReader<GenericRecord> dataFileReader = activeBlobDataFileReaderRef.get();
        if (dataFileReader == null) {
            Map.Entry<Long, MessiAvroFile> nextEntry = findNextBlob(timeout, unit, start);
            if (nextEntry == null) return null; // timeout
            activeBlobFromKeyRef.set(nextEntry.getKey());
            dataFileReader = setDataFileReader(nextEntry.getValue());
        }
        if (!dataFileReader.hasNext()) {
            Map.Entry<Long, MessiAvroFile> nextEntry = findNextBlob(timeout, unit, start);
            if (nextEntry == null) return null; // timeout
            activeBlobFromKeyRef.set(nextEntry.getKey());
            MessiAvroFile messiAvroFile = nextEntry.getValue();
            setDataFileReader(messiAvroFile);
            return receive(timeout, unit);
        }
        GenericRecord record = dataFileReader.next();
        MessiMessage.Builder builder = AvroMessiMessageSchema.toMessage(record);
        AvroMessiMessageSchema.deserializePostProcessing(builder, providerTechnology);
        MessiMessage msg = builder.build();
        return msg;
    }

    private Map.Entry<Long, MessiAvroFile> findNextBlob(int timeout, TimeUnit unit, long start) throws InterruptedException {
        Long currentBlobKey = activeBlobFromKeyRef.get();
        Map.Entry<Long, MessiAvroFile> nextEntry = topicAvroFileCache.blobsByTimestamp().higherEntry(currentBlobKey);
        while (nextEntry == null) {
            // TODO the file-listing poll-loop can be replaced with notifications from another event-source
            // TODO if so, the poll-loop should be a fallback when that other event-source is unavailable
            long duration = System.currentTimeMillis() - start;
            if (duration >= unit.toMillis(timeout)) {
                return null; // timeout
            }
            Thread.sleep(500);
            nextEntry = topicAvroFileCache.blobsByTimestamp().higherEntry(currentBlobKey);
        }
        return nextEntry;
    }

    @Override
    public CompletableFuture<? extends MessiMessage> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public MessiCursor currentPosition() {
        throw new UnsupportedOperationException("TODO"); // TODO
    }

    @Override
    public void seek(long timestamp) {
        preloadedMessages.clear();
        DataFileReader<GenericRecord> previousDataFileReader = activeBlobDataFileReaderRef.getAndSet(null);
        if (previousDataFileReader != null) {
            try {
                previousDataFileReader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        NavigableMap<Long, MessiAvroFile> blobByFrom = topicAvroFileCache.blobsByTimestamp();
        Map.Entry<Long, MessiAvroFile> firstEntryHigherOrEqual = blobByFrom.floorEntry(timestamp);
        if (firstEntryHigherOrEqual == null) {
            firstEntryHigherOrEqual = blobByFrom.ceilingEntry(timestamp);
        }
        if (firstEntryHigherOrEqual == null) {
            activeBlobFromKeyRef.set(-1L);
            return;
        }
        activeBlobFromKeyRef.set(firstEntryHigherOrEqual.getKey());
        MessiAvroFile messiAvroFile = firstEntryHigherOrEqual.getValue();
        DataFileReader<GenericRecord> dataFileReader = setDataFileReader(messiAvroFile);
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            try {
                record = dataFileReader.next(record);
                MessiMessage.Builder builder = AvroMessiMessageSchema.toMessage(record);
                AvroMessiMessageSchema.deserializePostProcessing(builder, providerTechnology);
                MessiMessage message = builder.build();
                long msgTimestamp = MessiULIDUtils.toUlid(message.getUlid()).timestamp();
                if (msgTimestamp >= timestamp) {
                    preloadedMessages.add(message);
                    return; // first match
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public AvroMessiShard shard() {
        return messiShard;
    }

    private DataFileReader<GenericRecord> setDataFileReader(MessiAvroFile messiAvroFile) {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(AvroMessiMessageSchema.schema);
        DataFileReader<GenericRecord> dataFileReader;
        try {
            dataFileReader = new DataFileReader<>(messiAvroFile.seekableInput(), datumReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        activeBlobDataFileReaderRef.set(dataFileReader);
        return dataFileReader;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            activeBlobFromKeyRef.set(null);
            topicAvroFileCache.clear();
            preloadedMessages.clear();
            DataFileReader<GenericRecord> dataFileReader = activeBlobDataFileReaderRef.getAndSet(null);
            if (dataFileReader != null) {
                try {
                    dataFileReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
