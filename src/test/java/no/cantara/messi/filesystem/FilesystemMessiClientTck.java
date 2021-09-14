package no.cantara.messi.filesystem;

import com.google.protobuf.ByteString;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiProducer;
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
import java.nio.charset.StandardCharsets;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class FilesystemMessiClientTck {

    MessiClient client;

    @BeforeMethod
    public void createClient() throws IOException {
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
    }

    @AfterMethod
    public void closeMessiClient() {
        client.close();
    }

    @Test
    public void thatLastExternalIdOfEmptyTopicCanBeReadAndIsNull() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() throws InterruptedException {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build()
            );
        }

        assertEquals(client.lastMessage("the-topic").getExternalId(), "b");

        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build());
        }

        assertEquals(client.lastMessage("the-topic").getExternalId(), "c");
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (MessiProducer producer = client.producer("the-topic")) {
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
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(3)
                                    .build())
                            .setExternalId("c")
                            .putData("payload1", ByteString.copyFromUtf8("p5"))
                            .putData("payload2", ByteString.copyFromUtf8("p6"))
                            .setProvider(MessiProvider.newBuilder()
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

        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf().ulid(ulid).inclusive(true).build())) {
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
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 3);
                assertEquals(message.getExternalId(), "c");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p5"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p6"));
                assertEquals(message.getProvider().getPublishedTimestamp(), 123);
                assertEquals(message.getProvider().getShardId(), "shardId123");
                assertEquals(message.getProvider().getSequenceNumber(), "three");
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
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = consumer.receive(5, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<? extends MessiMessage> future = consumer.receiveAsync();

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = future.join();
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            try (MessiProducer producer = client.producer("the-topic")) {
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
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<List<MessiMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

            try (MessiProducer producer = client.producer("the-topic")) {
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

    private CompletableFuture<List<MessiMessage>> receiveAsyncAddMessageAndRepeatRecursive(MessiConsumer consumer, String endExternalId, List<MessiMessage> messages) {
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
        try (MessiConsumer consumer1 = client.consumer("the-topic");
             MessiConsumer consumer2 = client.consumer("the-topic")) {

            CompletableFuture<List<MessiMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
            CompletableFuture<List<MessiMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

            try (MessiProducer producer = client.producer("the-topic")) {
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
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .externalId("a", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .externalId("b", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "c");
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .externalId("c", Instant.now(), Duration.ofMinutes(1))
                .inclusive(true)
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .externalId("c", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .externalId("d", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }

    @Test
    public void thatCheckpointCanBeCreatedAndUsed() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        String checkpointAt;
        String checkpointAfter;
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage a = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(a);
            MessiMessage b = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(b);
            MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(c);
            assertEquals(c.getExternalId(), "c");
            checkpointAt = consumer.cursorAt(c).checkpoint();
            checkpointAfter = consumer.cursorAfter(c).checkpoint();
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf().checkpoint(checkpointAt).build())) {
            MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(c);
            assertEquals(c.getExternalId(), "c");
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf().checkpoint(checkpointAfter).build())) {
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
    }

    @Test
    public void thatSeekToWorks() throws Exception {
        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (MessiProducer producer = client.producer("the-topic")) {
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
        try (MessiConsumer consumer = client.consumer("the-topic")) {
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

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        MessiMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        metadata.put("key-1", "Value-1".getBytes(StandardCharsets.UTF_8));
        metadata.put("key-2", "Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-1"), StandardCharsets.UTF_8), "Value-1");
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Value-2");
        metadata.put("key-2", "Overwritten-Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Overwritten-Value-2");
        metadata.remove("key-1");
        assertEquals(metadata.keys().size(), 1);
        metadata.remove("key-2");
        assertEquals(metadata.keys().size(), 0);
    }
}
