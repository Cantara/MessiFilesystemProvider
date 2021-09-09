package no.cantara.messi.avro;

import com.google.protobuf.ByteString;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiOrdering;
import no.cantara.messi.protos.MessiProvider;
import no.cantara.messi.protos.MessiSource;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

public class AvroMessiMessageSchema {

    static final Schema schema = SchemaBuilder.record("MessiMessage")
            .fields()
            .name("id").type().fixed("ulid").size(16).noDefault()
            .name("clientSourceId").type().nullable().stringType().noDefault()
            .name("providerPublishedTimestamp").type().longType().longDefault(-1)
            .name("providerShardId").type().nullable().stringType().noDefault()
            .name("providerSequenceNumber").type().nullable().stringType().noDefault()
            .name("partitionKey").type().nullable().stringType().noDefault()
            .name("orderingGroup").type().nullable().stringType().noDefault()
            .name("sequenceNumber").type().longType().longDefault(0)
            .name("externalId").type().stringType().noDefault()
            .name("attributes").type().map().values().stringType().noDefault()
            .name("data").type().map().values().bytesType().noDefault()
            .endRecord();

    static MessiMessage toMessage(GenericRecord record) {
        GenericData.Fixed id = (GenericData.Fixed) record.get("id");
        ULID.Value ulid = ULID.fromBytes(id.bytes());
        String clientSourceId = ofNullable(record.get("clientSourceId")).map(Object::toString).orElse(null);
        long providerPublishedTimestamp = ofNullable(record.get("providerPublishedTimestamp")).map(v -> (Long) v).orElse(-1l);
        String providerShardId = ofNullable(record.get("providerShardId")).map(Object::toString).orElse(null);
        String providerSequenceNumber = ofNullable(record.get("providerSequenceNumber")).map(Object::toString).orElse(null);
        String partitionKey = ofNullable(record.get("partitionKey")).map(Object::toString).orElse(null);
        String orderingGroup = ofNullable(record.get("orderingGroup")).map(Object::toString).orElse(null);
        long sequenceNumber = (Long) record.get("sequenceNumber");
        String externalId = record.get("externalId").toString();
        Map<Utf8, ByteBuffer> data = (Map<Utf8, ByteBuffer>) record.get("data");
        Map<Utf8, Utf8> attributes = (Map<Utf8, Utf8>) record.get("attributes");

        MessiMessage.Builder builder = MessiMessage.newBuilder()
                .setUlid(MessiULIDUtils.toMessiUlid(ulid))
                .setSource(MessiSource.newBuilder().setClientSourceId(clientSourceId).build())
                .setProvider(MessiProvider.newBuilder()
                        .setPublishedTimestamp(providerPublishedTimestamp)
                        .setShardId(providerShardId)
                        .setSequenceNumber(providerSequenceNumber)
                        .build())
                .setPartitionKey(partitionKey)
                .setOrdering(MessiOrdering.newBuilder()
                        .setGroup(orderingGroup)
                        .setSequenceNumber(sequenceNumber)
                        .build())
                .setExternalId(externalId);
        for (Map.Entry<Utf8, ByteBuffer> entry : data.entrySet()) {
            builder.putData(entry.getKey().toString(), ByteString.copyFrom(entry.getValue()));
        }
        for (Map.Entry<Utf8, Utf8> entry : attributes.entrySet()) {
            builder.putAttributes(entry.getKey().toString(), entry.getValue().toString());
        }
        return builder.build();
    }

    public static GenericRecord toAvro(ULID.Value ulidValue, MessiMessage message) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", new GenericData.Fixed(schema.getField("id").schema(), ulidValue.toBytes()));
        record.put("clientSourceId", message.getSource().getClientSourceId());
        record.put("providerPublishedTimestamp", message.getProvider().getPublishedTimestamp());
        record.put("providerShardId", message.getProvider().getShardId());
        record.put("providerSequenceNumber", message.getProvider().getSequenceNumber());
        record.put("partitionKey", message.getPartitionKey());
        record.put("orderingGroup", message.getOrdering().getGroup());
        record.put("sequenceNumber", message.getOrdering().getSequenceNumber());
        record.put("externalId", message.getExternalId());
        record.put("attributes", message.getAttributesMap());
        record.put("data", message.getDataMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asReadOnlyByteBuffer())));
        return record;
    }
}
