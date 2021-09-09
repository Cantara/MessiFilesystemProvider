package no.cantara.messi.avro;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.util.Deque;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class AvroMessiConsumer implements MessiConsumer {

    final String topic;
    final TopicAvroFileCache topicAvroFileCache;
    final AtomicReference<Long> activeBlobFromKeyRef = new AtomicReference<>(-1L);
    final AtomicReference<DataFileReader<GenericRecord>> activeBlobDataFileReaderRef = new AtomicReference<>(null);
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Deque<MessiMessage> preloadedMessages = new ConcurrentLinkedDeque<>();

    AvroMessiConsumer(AvroMessiUtils avroMessageUtils, String topic, AvroMessiCursor cursor, int minFileListingIntervalSeconds) {
        this.topic = topic;
        this.topicAvroFileCache = new TopicAvroFileCache(avroMessageUtils, topic, minFileListingIntervalSeconds);
        if (cursor == null) {
            seek(0);
        } else {
            seek(cursor.ulid.timestamp());
            try {
                MessiMessage msg;
                while ((msg = receive(0, TimeUnit.SECONDS)) != null) {
                    ULID.Value ulid = MessiULIDUtils.toUlid(msg.getUlid());
                    if (ulid.equals(cursor.ulid)) {
                        if (cursor.inclusive) {
                            preloadedMessages.addFirst(msg);
                        }
                        break; // found match
                    }
                    if (ulid.timestamp() > cursor.ulid.timestamp()) {
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
        MessiMessage msg = AvroMessiMessageSchema.toMessage(record);
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
                MessiMessage message = AvroMessiMessageSchema.toMessage(record);
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
