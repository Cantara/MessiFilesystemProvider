package no.cantara.messi.filesystem;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiMessage;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiNoSuchExternalIdException;
import no.cantara.messi.api.MessiProducer;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
    public void closeClient() {
        client.close();
    }

    @Test
    public void thatLastPositionOfEmptyTopicCanBeRead() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build()
            );
        }

        assertEquals(client.lastMessage("the-topic").externalId(), "b");

        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
        }

        assertEquals(client.lastMessage("the-topic").externalId(), "c");
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder()
                            .ulid(ulid)
                            .orderingGroup("og1")
                            .sequenceNumber(1)
                            .externalId("a")
                            .put("payload1", new byte[3])
                            .put("payload2", new byte[7])
                            .build(),
                    MessiMessage.builder()
                            .orderingGroup("og1")
                            .sequenceNumber(2)
                            .externalId("b")
                            .put("payload1", new byte[4])
                            .put("payload2", new byte[8])
                            .build(),
                    MessiMessage.builder()
                            .orderingGroup("og1")
                            .sequenceNumber(3)
                            .externalId("c")
                            .put("payload1", new byte[2])
                            .put("payload2", new byte[5])
                            .providerPublishedTimestamp(123)
                            .providerShardId("shardId123")
                            .providerSequenceNumber("three")
                            .clientSourceId("client-source-id-123")
                            .attribute("key1", "value1")
                            .attribute("some-other-key", "some other value")
                            .attribute("iamanattribute", "yes I am")
                            .build()
            );
        }

        try (MessiConsumer consumer = client.consumer("the-topic", ulid, true)) {
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(message.ulid(), ulid);
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 1);
                assertEquals(message.externalId(), "a");
                assertEquals(message.keys().size(), 2);
                assertEquals(message.get("payload1"), new byte[3]);
                assertEquals(message.get("payload2"), new byte[7]);
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.ulid());
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 2);
                assertEquals(message.externalId(), "b");
                assertEquals(message.keys().size(), 2);
                assertEquals(message.get("payload1"), new byte[4]);
                assertEquals(message.get("payload2"), new byte[8]);
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.ulid());
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 3);
                assertEquals(message.externalId(), "c");
                assertEquals(message.keys().size(), 2);
                assertTrue(message.keys().contains("payload1"));
                assertTrue(message.keys().contains("payload2"));
                assertEquals(message.get("payload1"), new byte[2]);
                assertEquals(message.get("payload2"), new byte[5]);
                assertEquals(message.data().size(), 2);
                assertEquals(message.data().get("payload1"), new byte[2]);
                assertEquals(message.data().get("payload2"), new byte[5]);
                assertEquals(message.providerPublishedTimestamp(), 123);
                assertEquals(message.providerShardId(), "shardId123");
                assertEquals(message.providerSequenceNumber(), "three");
                assertEquals(message.clientSourceId(), "client-source-id-123");
                assertEquals(message.attributes().size(), 3);
                assertTrue(message.attributes().contains("key1"));
                assertTrue(message.attributes().contains("some-other-key"));
                assertTrue(message.attributes().contains("iamanattribute"));
                assertEquals(message.attributes().size(), 3);
                assertEquals(message.attributeMap().size(), 3);
                assertEquals(message.attributeMap().get("key1"), "value1");
                assertEquals(message.attributeMap().get("some-other-key"), "some other value");
                assertEquals(message.attributeMap().get("iamanattribute"), "yes I am");
                assertEquals(message.attribute("key1"), "value1");
                assertEquals(message.attribute("some-other-key"), "some other value");
                assertEquals(message.attribute("iamanattribute"), "yes I am");
            }
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws Exception {
        try (MessiProducer producer = client.producer("a/b/c")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
        }

        try (MessiConsumer consumer = client.consumer("a/b/c")) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "a");
            assertEquals(message.keys().size(), 2);
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<? extends MessiMessage> future = consumer.receiveAsync();

            MessiMessage message = future.join();
            assertEquals(message.externalId(), "a");
            assertEquals(message.keys().size(), 2);
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message1.externalId(), "a");
            assertEquals(message2.externalId(), "b");
            assertEquals(message3.externalId(), "c");
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<List<MessiMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

            List<MessiMessage> messages = future.join();

            assertEquals(messages.get(0).externalId(), "a");
            assertEquals(messages.get(1).externalId(), "b");
            assertEquals(messages.get(2).externalId(), "c");
        }
    }

    private CompletableFuture<List<MessiMessage>> receiveAsyncAddMessageAndRepeatRecursive(MessiConsumer consumer, String endPosition, List<MessiMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endPosition.equals(message.externalId())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endPosition, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }

        try (MessiConsumer consumer1 = client.consumer("the-topic")) {
            CompletableFuture<List<MessiMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
            try (MessiConsumer consumer2 = client.consumer("the-topic")) {
                CompletableFuture<List<MessiMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());
                List<MessiMessage> messages2 = future2.join();
                assertEquals(messages2.get(0).externalId(), "a");
                assertEquals(messages2.get(1).externalId(), "b");
                assertEquals(messages2.get(2).externalId(), "c");
            }
            List<MessiMessage> messages1 = future1.join();
            assertEquals(messages1.get(0).externalId(), "a");
            assertEquals(messages1.get(1).externalId(), "b");
            assertEquals(messages1.get(2).externalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "a", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "b", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "c");
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "c", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.externalId(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", "d", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            MessiMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
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
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            Thread.sleep(5);
            timestampBeforeB = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            Thread.sleep(5);
            timestampBeforeC = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            Thread.sleep(5);
            timestampBeforeD = System.currentTimeMillis();
            producer.publish(MessiMessage.builder().externalId("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).externalId(), "a");
        }
    }

    @Test
    public void thatPositionCursorOfValidPositionIsFound() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        assertNotNull(client.cursorOf("the-topic", "a", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "b", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = MessiNoSuchExternalIdException.class)
    public void thatPositionCursorOfInvalidPositionIsNotFound() {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        assertNull(client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = MessiNoSuchExternalIdException.class)
    public void thatPositionCursorOfEmptyTopicIsNotFound() {
        try (MessiProducer producer = client.producer("the-topic")) {
        }
        client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1));
    }

    @Test
    public void thatMultipleFilesCanBeProducedAndReadBack() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            producer.publish(MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
        }
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("d").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            producer.publish(MessiMessage.builder().externalId("e").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            producer.publish(MessiMessage.builder().externalId("f").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
        }
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("g").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            producer.publish(MessiMessage.builder().externalId("h").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            producer.publish(MessiMessage.builder().externalId("i").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage a = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage b = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage c = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage d = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage e = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage f = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage g = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage h = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage i = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(a.externalId(), "a");
            assertEquals(b.externalId(), "b");
            assertEquals(c.externalId(), "c");
            assertEquals(d.externalId(), "d");
            assertEquals(e.externalId(), "e");
            assertEquals(f.externalId(), "f");
            assertEquals(g.externalId(), "g");
            assertEquals(h.externalId(), "h");
            assertEquals(i.externalId(), "i");
        }
    }

    @Test
    public void thatMultipleFilesCanBeProducedThroughSizeBasedWindowingAndReadBack() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            for (int i = 0; i < 100; i++) {
                producer.publish(MessiMessage.builder().externalId("a" + i)
                        .put("attribute-1", ("a" + i + "_").getBytes(StandardCharsets.UTF_8))
                        .put("payload", repeat("ABC_", i).getBytes(StandardCharsets.UTF_8))
                        .build());
            }
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            for (int i = 0; i < 100; i++) {
                MessiMessage msg = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(msg.externalId(), "a" + i);
                assertEquals(new String(msg.get("attribute-1"), StandardCharsets.UTF_8), "a" + i + "_");
                assertEquals(new String(msg.get("payload"), StandardCharsets.UTF_8), repeat("ABC_", i));
            }
            MessiMessage msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(msg);
        }
    }

    private static String repeat(String value, int n) {
        char[] buf = new char[value.length() * n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < value.length(); j++) {
                buf[(value.length() * i) + j] = value.charAt(j);
            }
        }
        return new String(buf);
    }

    @Test
    public void thatMultipleFilesCanBeProducedThroughTimeBasedWindowingAndReadBack() throws Exception {
        int N = 3;
        try (MessiProducer producer = client.producer("the-topic")) {
            for (int i = 0; i < N; i++) {
                producer.publish(MessiMessage.builder().externalId("a" + i)
                        .put("attribute-1", ("a" + i).getBytes(StandardCharsets.UTF_8))
                        .build());
                if (i < N - 1) {
                    Thread.sleep(1100);
                }
            }
        }

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            for (int i = 0; i < N; i++) {
                MessiMessage msg = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(msg.externalId(), "a" + i);
                assertEquals(new String(msg.get("attribute-1"), StandardCharsets.UTF_8), "a" + i);
            }
            MessiMessage msg = consumer.receive(10, TimeUnit.MILLISECONDS);
            assertNull(msg);
        }
    }

    @Test
    public void thatFilesCreatedAfterConsumerHasSubscribedAreUsed() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            producer.publish(MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
        }
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("d").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            producer.publish(MessiMessage.builder().externalId("e").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            producer.publish(MessiMessage.builder().externalId("f").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
        }

        // start a background task that will wait 1 second, then publish 3 messages
        CompletableFuture.runAsync(() -> {
            try (MessiProducer producer = client.producer("the-topic")) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                producer.publish(MessiMessage.builder().externalId("g").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
                producer.publish(MessiMessage.builder().externalId("h").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
                producer.publish(MessiMessage.builder().externalId("i").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage a = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage b = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage c = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage d = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage e = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage f = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(a.externalId(), "a");
            assertEquals(b.externalId(), "b");
            assertEquals(c.externalId(), "c");
            assertEquals(d.externalId(), "d");
            assertEquals(e.externalId(), "e");
            assertEquals(f.externalId(), "f");

            // until here is easy. After this point we have to wait for more files to appear

            MessiMessage g = consumer.receive(15, TimeUnit.SECONDS);
            MessiMessage h = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage i = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(g.externalId(), "g");
            assertEquals(h.externalId(), "h");
            assertEquals(i.externalId(), "i");
        }
    }

    @Test
    public void thatNonExistentStreamCanBeConsumedFirstAndProducedAfter() throws Exception {
        Thread consumerThread = new Thread(() -> {
            try (MessiConsumer consumer = client.consumer("the-topic")) {
                MessiMessage a = consumer.receive(1, TimeUnit.SECONDS);
                MessiMessage b = consumer.receive(100, TimeUnit.MILLISECONDS);
                MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
                MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
                MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
                MessiMessage f = consumer.receive(100, TimeUnit.MILLISECONDS);
                MessiMessage none = consumer.receive(100, TimeUnit.MILLISECONDS);
                assertEquals(a.externalId(), "a");
                assertEquals(b.externalId(), "b");
                assertEquals(c.externalId(), "c");
                assertEquals(d.externalId(), "d");
                assertEquals(e.externalId(), "e");
                assertEquals(f.externalId(), "f");
                assertNull(none);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        consumerThread.start();

        Thread.sleep(300);

        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            producer.publish(MessiMessage.builder().externalId("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            producer.publish(MessiMessage.builder().externalId("d").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            producer.publish(MessiMessage.builder().externalId("e").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            producer.publish(MessiMessage.builder().externalId("f").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
        }

        consumerThread.join();
    }

    @Test
    public void thatReadLastMessageWorksWithMultipleBlocks() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[50]).put("payload2", new byte[50]).build());
            producer.publish(MessiMessage.builder().externalId("b").put("payload1", new byte[30]).put("payload2", new byte[30]).build());
            producer.publish(MessiMessage.builder().externalId("c").put("payload1", new byte[70]).put("payload2", new byte[70]).build());
            producer.publish(MessiMessage.builder().externalId("d").put("payload1", new byte[50]).put("payload2", new byte[50]).build());
            producer.publish(MessiMessage.builder().externalId("e").put("payload1", new byte[30]).put("payload2", new byte[30]).build());
            producer.publish(MessiMessage.builder().externalId("f").put("payload1", new byte[70]).put("payload2", new byte[70]).build());
            producer.publish(MessiMessage.builder().externalId("g").put("payload1", new byte[50]).put("payload2", new byte[50]).build());
            producer.publish(MessiMessage.builder().externalId("h").put("payload1", new byte[30]).put("payload2", new byte[30]).build());
            producer.publish(MessiMessage.builder().externalId("i").put("payload1", new byte[70]).put("payload2", new byte[70]).build());
        }

        MessiMessage lastMessage = client.lastMessage("the-topic");
        assertEquals(lastMessage.externalId(), "i");
    }

    @Test
    public void thatReadLastMessageWorksWithSingleBlock() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(MessiMessage.builder().externalId("a").put("payload1", new byte[50]).put("payload2", new byte[50]).build());
        }

        MessiMessage lastMessage = client.lastMessage("the-topic");
        assertEquals(lastMessage.externalId(), "a");
    }

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        MessiMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        String key1 = "//./key-1'§!#$%&/()=?";
        String key2 = ".";
        String key3 = "..";
        metadata.put(key1, "Value-1".getBytes(StandardCharsets.UTF_8));
        metadata.put(key2, "Value-2".getBytes(StandardCharsets.UTF_8));
        metadata.put(key3, "Value-3".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 3);
        assertEquals(new String(metadata.get(key1), StandardCharsets.UTF_8), "Value-1");
        assertEquals(new String(metadata.get(key2), StandardCharsets.UTF_8), "Value-2");
        metadata.put(key2, "Overwritten-Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 3);
        assertEquals(new String(metadata.get(key2), StandardCharsets.UTF_8), "Overwritten-Value-2");
        metadata.remove(key3);
        assertEquals(metadata.keys().size(), 2);
    }
}
