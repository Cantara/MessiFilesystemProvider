package no.cantara.messi.avro;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class AvroMessiProducer implements MessiProducer {

    static final Logger LOG = LoggerFactory.getLogger(AvroMessiProducer.class);

    final AtomicBoolean closed = new AtomicBoolean(false);

    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

    final AvroMessiTopic topic;
    final AvroMessiUtils avroMessiUtils;
    final Path tmpFolder;
    final long avroMaxSeconds;
    final long avroMaxBytes;
    final int avroSyncInterval;
    final String topicName;

    final AtomicReference<DataFileWriter<GenericRecord>> dataFileWriterRef = new AtomicReference<>();
    final Path topicFolder;
    final AtomicReference<Path> pathRef = new AtomicReference<>();
    final AtomicLong lastSequenceWritten = new AtomicLong(0); // first sequence number is 1

    final AtomicLong timestampOfFirstMessageInWindow = new AtomicLong(-1);
    final AvroFileMetadata activeAvrofileMetadata;
    final String providerTechnology;
    final AtomicLong avroBytesWrittenInBlock = new AtomicLong(0);

    final ReentrantLock lock = new ReentrantLock();

    final Thread uploadThread;
    final BlockingQueue<Upload> uploadQueue = new LinkedBlockingQueue<>();

    static class Upload {
        final Path source;
        final MessiAvroFile target;

        Upload(Path source, MessiAvroFile target) {
            this.source = source;
            this.target = target;
        }
    }

    AvroMessiProducer(AvroMessiTopic topic,
                      AvroMessiUtils avroMessiUtils,
                      Path tmpFolder,
                      long avroMaxSeconds,
                      long avroMaxBytes,
                      int avroSyncInterval,
                      String topicName,
                      String providerTechnology) {
        this.topic = topic;
        this.avroMessiUtils = avroMessiUtils;
        this.tmpFolder = tmpFolder;
        this.avroMaxSeconds = avroMaxSeconds;
        this.avroMaxBytes = avroMaxBytes;
        this.avroSyncInterval = avroSyncInterval;
        this.topicName = topicName;
        this.activeAvrofileMetadata = avroMessiUtils.newAvrofileMetadata();
        this.providerTechnology = providerTechnology;
        this.topicFolder = tmpFolder.resolve(topicName);
        try {
            Files.createDirectories(topicFolder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        AvroMessiShard theOnlyShard = topic.shardOf(topic.firstShard());
        MessiMessage lastMessageInStream = theOnlyShard.lastMessage();
        if (lastMessageInStream != null) {
            String lastSequenceNumber = lastMessageInStream.getProvider().getSequenceNumber();
            long lastSequence = Long.parseLong(lastSequenceNumber);
            lastSequenceWritten.set(lastSequence);
        }
        createOrOverwriteLocalAvroFile();
        this.uploadThread = new Thread(() -> {
            for (; ; ) {
                final Upload upload;
                try {
                    upload = uploadQueue.take(); // wait for upload task
                } catch (InterruptedException e) {
                    LOG.warn("Closing producer topic {}", topicName);
                    close();
                    LOG.warn("Upload thread interrupted. Upload thread for producer of topic {} will now die.", topicName);
                    return;
                }
                try {
                    if (upload.source == null) {
                        LOG.info("Upload thread for producer of topic {} received close signal and will now die.", topicName);
                        return;
                    }
                    verifySeekableToLastBlockOffsetAsGivenByFilename(upload.source, upload.target.getOffsetOfLastBlock());
                    String fileSize = AvroMessiUtils.humanReadableByteCount(upload.source.toFile().length(), false);
                    LOG.info("Copying Avro file {} ({}) to target: {}", upload.source.getFileName(), fileSize, upload.target);
                    upload.target.copyFrom(upload.source);
                    Files.delete(upload.source);
                    LOG.info("Copy COMPLETE! Deleted Avro file {}", upload.source.getFileName());
                } catch (Throwable t) {
                    LOG.error(String.format("While uploading file %s to target %s", upload.source.getFileName(), upload.target), t);
                    LOG.warn("Closing producer topic {}", topicName);
                    close();
                    LOG.warn("Upload thread for producer of topic {} will now die.", topicName);
                    return;
                }
            }
        });
        this.uploadThread.start();
    }

    private void createOrOverwriteLocalAvroFile() {
        try {
            if (!lock.tryLock(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Unable to acquire lock within 5 minutes");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            Path path = Files.createTempFile(topicFolder, "", ".avro");
            pathRef.set(path);
            activeAvrofileMetadata.clear();
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(AvroMessiMessageSchema.schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.setSyncInterval(2 * avroSyncInterval);
            dataFileWriter.setFlushOnEveryBlock(true);
            dataFileWriterRef.set(dataFileWriter);
            dataFileWriter.create(AvroMessiMessageSchema.schema, path.toFile());
            long lastSyncPosition = dataFileWriter.sync(); // position of first block
            activeAvrofileMetadata.setSyncOfLastBlock(lastSyncPosition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private void closeAvroFileAndTriggerAsyncUploadToOutputStore() {
        try {
            if (!lock.tryLock(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Unable to acquire lock within 5 minutes");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            DataFileWriter<GenericRecord> dataFileWriter = dataFileWriterRef.getAndSet(null);
            if (dataFileWriter != null) {
                dataFileWriter.flush();
                dataFileWriter.close();
            }
            Path path = pathRef.get();
            if (path != null) {
                if (activeAvrofileMetadata.getCount() > 0) {
                    MessiAvroFile messiAvroFile = activeAvrofileMetadata.toMessiAvroFile(topicName);
                    uploadQueue.add(new Upload(path, messiAvroFile)); // schedule upload asynchronously
                } else {
                    // no records, no need to write file to output-store
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    static void verifySeekableToLastBlockOffsetAsGivenByFilename(Path path, long offsetOfLastBlock) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(AvroMessiMessageSchema.schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableFileInput(path.toFile()), datumReader)) {
            dataFileReader.seek(offsetOfLastBlock);
            dataFileReader.hasNext(); // will throw an exception if offset is wrong
        }
    }

    @Override
    public String topic() {
        return topicName;
    }

    @Override
    public void publish(MessiMessage... messages) throws MessiClosedException {
        if (isClosed()) {
            throw new MessiClosedException();
        }
        try {
            if (!lock.tryLock(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Unable to acquire lock within 5 minutes");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (MessiMessage message : messages) {
                long now = System.currentTimeMillis();
                timestampOfFirstMessageInWindow.compareAndSet(-1, now);

                boolean timeLimitExceeded = timestampOfFirstMessageInWindow.get() + 1000 * avroMaxSeconds < now;
                if (timeLimitExceeded) {
                    closeAvroFileAndTriggerAsyncUploadToOutputStore();
                    createOrOverwriteLocalAvroFile();
                    timestampOfFirstMessageInWindow.set(now);
                }

                ULID.Value ulidValue;
                if (message.hasUlid()) {
                    ulidValue = MessiULIDUtils.toUlid(message.getUlid());
                } else {
                    ulidValue = MessiULIDUtils.nextMonotonicUlid(ulid, prevUlid.get());
                }
                prevUlid.set(ulidValue);

                activeAvrofileMetadata.setIdOfFirstRecord(ulidValue);
                activeAvrofileMetadata.setPositionOfFirstRecord(message.getExternalId());

                try {
                    GenericRecord record = AvroMessiMessageSchema.toAvro(ulidValue, message);
                    long nowMillis = System.currentTimeMillis();
                    String currentSequenceNumber = String.valueOf(lastSequenceWritten.incrementAndGet());
                    if (!message.hasFirstProvider()) {
                        GenericData.Record firstProvider = new GenericData.Record(AvroMessiMessageSchema.providerSchema);
                        firstProvider.put("publishedTimestamp", nowMillis);
                        firstProvider.put("technology", providerTechnology); // must set here in producer
                        firstProvider.put("sequenceNumber", currentSequenceNumber);
                        record.put("firstProvider", firstProvider);
                    }
                    GenericData.Record provider = new GenericData.Record(AvroMessiMessageSchema.providerSchema);
                    provider.put("publishedTimestamp", nowMillis);
                    provider.put("sequenceNumber", currentSequenceNumber);
                    // technology will be set in consumer to save space
                    record.put("provider", provider);

                    DataFileWriter<GenericRecord> dataFileWriter = dataFileWriterRef.get();
                    if (avroBytesWrittenInBlock.get() >= avroSyncInterval) {
                        // start new block in avro file
                        long lastSyncPosition = dataFileWriter.sync();
                        activeAvrofileMetadata.setSyncOfLastBlock(lastSyncPosition);
                        avroBytesWrittenInBlock.set(0);
                    }
                    dataFileWriter.append(record);
                    activeAvrofileMetadata.incrementCounter(1);
                    avroBytesWrittenInBlock.addAndGet(estimateAvroSizeOfMessage(message));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                boolean sizeLimitExceeded = pathRef.get().toFile().length() > avroMaxBytes;
                if (sizeLimitExceeded) {
                    closeAvroFileAndTriggerAsyncUploadToOutputStore();
                    createOrOverwriteLocalAvroFile();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    static long estimateAvroSizeOfMessage(MessiMessage message) {
        return message.getSerializedSize(); // Use size of serialized protobuf-message. This is usually slightly longer than size of avro message, but should be close enough to determine avro sync interval
        /*
        // TODO more accurate length-computation ... best would be to get the actual serialized length
        return 16 // ulid
                + 2 + message.getExternalId().length() // position
                + 2 + Optional.of(message.getPartitionKey()).map(String::length).orElse(0) // partitionKey
                + 6 // sequenceNumber
                + message.getAttributesMap().entrySet().stream()
                .map(e -> 2L + e.getKey().length() + 2 + e.getValue().length())
                .reduce(0L, Long::sum)
                + message.getDataMap().entrySet().stream()
                .map(e -> 2L + e.getKey().length() + 4 + e.getValue().size())
                .reduce(0L, Long::sum)
                + 2 + Optional.of(message.getSource()).map(MessiSource::getClientSourceId).map(String::length).orElse(0) // source
                + 2 + Optional.of(message.getProvider()).map(p -> (p.hasPublishedTimestamp() ? 8 : 2) + (p.hasShardId() ? 2 + p.getShardId().length() : 2) + (p.hasSequenceNumber() ? 2 + p.getSequenceNumber().length() : 2) + (p.hasTechnology() ? 2 + p.getTechnology().length() : 2)).orElse(0) // provider
                + 2 + Optional.of(message.getOrdering()).map(o -> o.getGroup().length() + 8).orElse(0) // orderingGroup
                + 12 // timestamp
                + 2 + Optional.of(message.getFirstProvider()).map(p -> (p.hasPublishedTimestamp() ? 8 : 2) + (p.hasShardId() ? 2 + p.getShardId().length() : 2) + (p.hasSequenceNumber() ? 2 + p.getSequenceNumber().length() : 2) + (p.hasTechnology() ? 2 + p.getTechnology().length() : 2)).orElse(0) // first-provider
                ;
         */
    }

    @Override
    public CompletableFuture<Void> publishAsync(MessiMessage... messages) {
        if (isClosed()) {
            throw new MessiClosedException();
        }
        return CompletableFuture.runAsync(() -> publish(messages));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeAvroFileAndTriggerAsyncUploadToOutputStore();
            uploadQueue.add(new Upload(null, null)); // send close signal to upload-thread.
        }
        try {
            // all callers must wait for all uploads to complete
            uploadThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
