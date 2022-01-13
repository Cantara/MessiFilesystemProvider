package no.cantara.messi.filesystem;

import com.google.protobuf.ByteString;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.api.MessiTimestampUtils;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiOrdering;
import no.cantara.messi.protos.MessiProvider;
import no.cantara.messi.protos.MessiSource;
import no.cantara.messi.protos.MessiUlid;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MessiStreamingConsumerTck {

    MessiClient client;
    MessiTopic topic;
    MessiShard shard;

    @BeforeMethod
    public void createMessiClient() throws IOException {
        ApplicationProperties configuration = ApplicationProperties.builder()
                .values()
                .put("local-temp-folder", "target/_tmp_avro_")
                .put("avro-file.max.seconds", "2")
                .put("avro-file.max.bytes", Long.toString(2 * 1024)) // 2 KiB
                .put("avro-file.sync.interval", Long.toString(200))
                .put("listing.min-interval-seconds", "0")
                .put("filesystem.storage-folder", "target/messi-store")
                .end()
                .build();
        {
            Path folder = Paths.get(configuration.get("local-temp-folder"));
            if (Files.exists(folder)) {
                Files.walk(folder).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
            Files.createDirectories(folder);
        }
        {
            Path folder = Paths.get(configuration.get("filesystem.storage-folder"));
            if (Files.exists(folder)) {
                Files.walk(folder).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
            Files.createDirectories(folder);
        }
        client = ProviderLoader.configure(configuration, "filesystem", MessiClientFactory.class);
        topic = client.topicOf("the-topic");
        shard = topic.shardOf(topic.firstShard());
        assertTrue(shard.supportsStreaming());
    }

    @AfterMethod
    public void closeMessiClient() {
        client.close();
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder()
                            .setUlid(MessiUlid.newBuilder().setMsb(ulid.getMostSignificantBits()).setLsb(ulid.getLeastSignificantBits()).build())
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(1)
                                    .build())
                            .setExternalId("a")
                            .putData("payload1", ByteString.copyFromUtf8("p1"))
                            .putData("payload2", ByteString.copyFromUtf8("p2"))
                            .build(),
                    MessiMessage.newBuilder()
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(2)
                                    .build())
                            .setExternalId("b")
                            .putData("payload1", ByteString.copyFromUtf8("p3"))
                            .putData("payload2", ByteString.copyFromUtf8("p4"))
                            .build(),
                    MessiMessage.newBuilder()
                            .setTimestamp(MessiTimestampUtils.toTimestamp(Instant.parse("2021-12-09T11:59:18.052Z")))
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(3)
                                    .build())
                            .setExternalId("c")
                            .putData("payload1", ByteString.copyFromUtf8("p5"))
                            .putData("payload2", ByteString.copyFromUtf8("p6"))
                            .setFirstProvider(MessiProvider.newBuilder()
                                    .setTechnology("JUNIT")
                                    .setPublishedTimestamp(123)
                                    .setShardId("shardId123")
                                    .setSequenceNumber("three")
                                    .build())
                            .setSource(MessiSource.newBuilder()
                                    .setClientSourceId("client-source-id-123")
                                    .build())
                            .putAttributes("key1", "value1")
                            .putAttributes("some-other-key", "some other value")
                            .putAttributes("iamanattribute", "yes I am")
                            .build()
            );
        }

        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf().ulid(ulid).inclusive(true).build())) {
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(MessiULIDUtils.toUlid(message.getUlid()), ulid);
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 1);
                assertEquals(message.getExternalId(), "a");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p1"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p2"));
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.getUlid());
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 2);
                assertEquals(message.getExternalId(), "b");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p3"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p4"));
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.getUlid());
                assertEquals(message.getTimestamp(), MessiTimestampUtils.toTimestamp(Instant.parse("2021-12-09T11:59:18.052Z")));
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 3);
                assertEquals(message.getExternalId(), "c");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p5"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p6"));
                assertEquals(message.getFirstProvider().getTechnology(), "JUNIT");
                assertEquals(message.getFirstProvider().getPublishedTimestamp(), 123);
                assertEquals(message.getFirstProvider().getShardId(), "shardId123");
                assertEquals(message.getFirstProvider().getSequenceNumber(), "three");
                assertEquals(message.getProvider().getTechnology(), "Filesystem (Avro)");
                assertNotEquals(message.getProvider().getPublishedTimestamp(), 0);
                assertNotEquals(message.getProvider().getPublishedTimestamp(), 123);
                assertEquals(message.getProvider().getShardId(), "");
                assertEquals(message.getProvider().getSequenceNumber(), "3"); // generated by provider
                assertEquals(message.getSource().getClientSourceId(), "client-source-id-123");
                assertEquals(message.getAttributesCount(), 3);
                assertEquals(message.getAttributesOrThrow("key1"), "value1");
                assertEquals(message.getAttributesOrThrow("some-other-key"), "some other value");
                assertEquals(message.getAttributesOrThrow("iamanattribute"), "yes I am");
            }
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorHead())) {

            try (MessiProducer producer = topic.producer()) {
                producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = consumer.receive(5, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorHead())) {

            CompletableFuture<? extends MessiMessage> future = consumer.receiveAsync();

            try (MessiProducer producer = topic.producer()) {
                producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = future.join();
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorHead())) {

            try (MessiProducer producer = topic.producer()) {
                producer.publish(
                        MessiMessage.newBuilder().setUlid(MessiULIDUtils.toMessiUlid(new ULID().nextValue())).setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            MessiMessage message1 = consumer.receive(5, TimeUnit.SECONDS);
            MessiMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message1.getExternalId(), "a");
            assertEquals(message2.getExternalId(), "b");
            assertEquals(message3.getExternalId(), "c");
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorHead())) {

            CompletableFuture<List<MessiMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

            try (MessiProducer producer = topic.producer()) {
                producer.publish(
                        MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            List<MessiMessage> messages = future.join();

            assertEquals(messages.get(0).getExternalId(), "a");
            assertEquals(messages.get(1).getExternalId(), "b");
            assertEquals(messages.get(2).getExternalId(), "c");
        }
    }

    private CompletableFuture<List<MessiMessage>> receiveAsyncAddMessageAndRepeatRecursive(MessiStreamingConsumer consumer, String endExternalId, List<MessiMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endExternalId.equals(message.getExternalId())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endExternalId, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        try (MessiStreamingConsumer consumer1 = shard.streamingConsumer(shard.cursorHead());
             MessiStreamingConsumer consumer2 = shard.streamingConsumer(shard.cursorHead())) {

            CompletableFuture<List<MessiMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
            CompletableFuture<List<MessiMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

            try (MessiProducer producer = topic.producer()) {
                producer.publish(
                        MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            List<MessiMessage> messages1 = future1.join();
            assertEquals(messages1.get(0).getExternalId(), "a");
            assertEquals(messages1.get(1).getExternalId(), "b");
            assertEquals(messages1.get(2).getExternalId(), "c");

            List<MessiMessage> messages2 = future2.join();
            assertEquals(messages2.get(0).getExternalId(), "a");
            assertEquals(messages2.get(1).getExternalId(), "b");
            assertEquals(messages2.get(2).getExternalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorAtTrimHorizon())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf()
                .externalId("a", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf()
                .externalId("b", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "c");
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf()
                .externalId("c", Instant.now(), Duration.ofMinutes(1))
                .inclusive(true)
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf()
                .externalId("c", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf()
                .externalId("d", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }

    @Test
    public void thatCheckpointCanBeCreatedAndUsed() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        String checkpointAt;
        String checkpointAfter;
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorAtTrimHorizon())) {
            MessiMessage a = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(a);
            MessiMessage b = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(b);
            MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(c);
            assertEquals(c.getExternalId(), "c");
            checkpointAt = shard.cursorAt(c).checkpoint();
            checkpointAfter = shard.cursorAfter(c).checkpoint();
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf().checkpoint(checkpointAt).build())) {
            MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(c);
            assertEquals(c.getExternalId(), "c");
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorOf().checkpoint(checkpointAfter).build())) {
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
    }

    @Test
    public void thatCheckpointsCanBeCompared() throws Exception {
        try (MessiProducer producer = topic.producer()) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").setPartitionKey("pk1").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").setPartitionKey("pk1").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").setPartitionKey("pk1").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
            );
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorAtTrimHorizon())) {
            MessiMessage a = consumer.receive(100, TimeUnit.MILLISECONDS);
            MessiCursor cursorAtA = shard.cursorAt(a);
            assertTrue(cursorAtA.isSame(shard.cursorAt(a)));
            MessiCursor cursorAfterA = shard.cursorAfter(a);
            assertTrue(cursorAfterA.isSame(shard.cursorAfter(a)));
            MessiMessage b = consumer.receive(100, TimeUnit.MILLISECONDS);
            MessiCursor cursorAtB = shard.cursorAt(b);
            assertTrue(cursorAtB.isSame(shard.cursorAt(b)));
            assertTrue(cursorAtB.isAfter(cursorAtA));
            assertTrue(cursorAtB.isAfter(cursorAfterA));
            assertTrue(cursorAtA.isBefore(cursorAtB));
            MessiCursor cursorAfterB = shard.cursorAfter(b);
            assertTrue(cursorAfterB.isSame(shard.cursorAfter(b)));
            assertTrue(cursorAfterB.isAfter(cursorAtA));
            assertTrue(cursorAfterB.isAfter(cursorAfterA));
            assertTrue(cursorAtA.isBefore(cursorAfterB));
            MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
            MessiCursor cursorAtC = shard.cursorAt(c);
            assertTrue(cursorAtC.isSame(shard.cursorAt(c)));
            assertTrue(cursorAtC.isAfter(cursorAtA));
            assertTrue(cursorAtC.isAfter(cursorAfterA));
            assertTrue(cursorAtC.isAfter(cursorAtB));
            assertTrue(cursorAtC.isAfter(cursorAfterB));
            assertTrue(cursorAtA.isBefore(cursorAtC));
            assertTrue(cursorAtB.isBefore(cursorAtC));
            assertTrue(cursorAfterA.isBefore(cursorAtC));
            assertTrue(cursorAfterB.isBefore(cursorAtC));
            MessiCursor cursorAfterC = shard.cursorAfter(c);
            assertTrue(cursorAfterC.isSame(shard.cursorAfter(c)));
            assertTrue(cursorAfterC.isAfter(cursorAtA));
            assertTrue(cursorAfterC.isAfter(cursorAfterA));
            assertTrue(cursorAfterC.isAfter(cursorAtB));
            assertTrue(cursorAfterC.isAfter(cursorAfterB));
            assertTrue(cursorAfterC.isAfter(cursorAtC));
            assertTrue(cursorAtC.isBefore(cursorAfterC));
            assertTrue(cursorAtA.isBefore(cursorAfterC));
            assertTrue(cursorAtB.isBefore(cursorAfterC));
            assertTrue(cursorAfterA.isBefore(cursorAfterC));
            assertTrue(cursorAfterB.isBefore(cursorAfterC));
            MessiCursor checkpointedAtC = shard.cursorOfCheckpoint(cursorAtC.checkpoint());
            assertTrue(checkpointedAtC.isSame(cursorAtC));
        }
    }

    @Test
    public void thatSeekToWorks() throws Exception {
        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (MessiProducer producer = topic.producer()) {
            timestampBeforeA = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            Thread.sleep(5);
            timestampBeforeB = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build());
            Thread.sleep(5);
            timestampBeforeC = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build());
            Thread.sleep(5);
            timestampBeforeD = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build());
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorAtTrimHorizon())) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "a");
        }
    }
}
