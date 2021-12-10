package no.cantara.messi.avro;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiOrdering;
import no.cantara.messi.protos.MessiProvider;
import no.cantara.messi.protos.MessiSource;
import no.cantara.messi.protos.MessiUlid;
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

    static final Schema ulidSchema = SchemaBuilder.record("MessiUlid")
            .fields()
            .name("msb").type().longType().longDefault(0L)
            .name("lsb").type().longType().longDefault(0L)
            .endRecord();

    static final Schema timestampSchema = SchemaBuilder.record("Timestamp")
            .fields()
            .name("seconds").type().longType().longDefault(0L)
            .name("nanos").type().intType().intDefault(0)
            .endRecord();

    static final Schema providerSchema = SchemaBuilder.record("MessiProvider")
            .fields()
            .name("publishedTimestamp").type().nullable().longType().noDefault()
            .name("shardId").type().nullable().stringType().noDefault()
            .name("sequenceNumber").type().nullable().stringType().noDefault()
            .name("technology").type().nullable().stringType().noDefault()
            .endRecord();

    static final Schema sourceSchema = SchemaBuilder.record("MessiSource")
            .fields()
            .name("clientSourceId").type().stringType().noDefault()
            .endRecord();

    static final Schema orderingSchema = SchemaBuilder.record("MessiOrdering")
            .fields()
            .name("group").type().stringType().noDefault()
            .name("sequenceNumber").type().longType().longDefault(0L)
            .endRecord();

    static final Schema schema = SchemaBuilder.record("MessiMessage")
            .fields()
            .name("ulid").type(ulidSchema).noDefault()
            .name("externalId").type().stringType().noDefault()
            .name("partitionKey").type().optional().stringType()
            .name("attributes").type().map().values().stringType().noDefault()
            .name("data").type().map().values().bytesType().noDefault()
            .name("source").type().optional().type(sourceSchema)
            .name("provider").type().optional().type(providerSchema)
            .name("ordering").type().optional().type(orderingSchema)
            .name("timestamp").type(timestampSchema).noDefault()
            .name("firstProvider").type().optional().type(providerSchema)
            .endRecord();

    public static MessiMessage.Builder toMessage(GenericRecord record) {
        MessiMessage.Builder builder = MessiMessage.newBuilder();
        {
            GenericData.Record ulid = (GenericData.Record) record.get("ulid");
            builder.setUlid(MessiUlid.newBuilder()
                    .setMsb((Long) ulid.get("msb"))
                    .setLsb((Long) ulid.get("lsb"))
                    .build());
        }
        {
            String externalId = record.get("externalId").toString();
            builder.setExternalId(externalId);
        }
        {
            String partitionKey = ofNullable(record.get("partitionKey")).map(Object::toString).orElse(null);
            if (partitionKey != null) {
                builder.setPartitionKey(partitionKey);
            }
        }
        {
            Map<Utf8, Utf8> attributes = (Map<Utf8, Utf8>) record.get("attributes");
            for (Map.Entry<Utf8, Utf8> entry : attributes.entrySet()) {
                builder.putAttributes(entry.getKey().toString(), entry.getValue().toString());
            }
        }
        {
            Map<Utf8, ByteBuffer> data = (Map<Utf8, ByteBuffer>) record.get("data");
            for (Map.Entry<Utf8, ByteBuffer> entry : data.entrySet()) {
                builder.putData(entry.getKey().toString(), ByteString.copyFrom(entry.getValue()));
            }
        }
        {
            GenericData.Record source = (GenericData.Record) record.get("source");
            if (source != null) {
                String clientSourceId = ofNullable(source.get("clientSourceId")).map(Object::toString).orElse(null);
                builder.setSource(MessiSource.newBuilder().setClientSourceId(clientSourceId).build());
            }
        }
        {
            GenericData.Record provider = (GenericData.Record) record.get("provider");
            if (provider != null) {
                MessiProvider.Builder providerBuilder = MessiProvider.newBuilder();
                Long publishedTimestamp = (Long) ofNullable(provider.get("publishedTimestamp")).orElse(null);
                if (publishedTimestamp != null) {
                    providerBuilder.setPublishedTimestamp(publishedTimestamp);
                }
                String shardId = ofNullable(provider.get("shardId")).map(Object::toString).orElse(null);
                if (shardId != null) {
                    providerBuilder.setShardId(shardId);
                }
                String sequenceNumber = ofNullable(provider.get("sequenceNumber")).map(Object::toString).orElse(null);
                if (sequenceNumber != null) {
                    providerBuilder.setSequenceNumber(sequenceNumber);
                }
                String technology = ofNullable(provider.get("technology")).map(Object::toString).orElse(null);
                if (technology != null) {
                    providerBuilder.setTechnology(technology);
                }
                builder.setProvider(providerBuilder.build());
            }
        }
        {
            GenericData.Record ordering = (GenericData.Record) record.get("ordering");
            if (ordering != null) {
                String group = ordering.get("group").toString();
                Long sequenceNumber = (Long) ordering.get("sequenceNumber");
                builder.setOrdering(MessiOrdering.newBuilder()
                        .setGroup(group)
                        .setSequenceNumber(sequenceNumber)
                        .build());
            }
        }
        {
            GenericData.Record timestamp = (GenericData.Record) record.get("timestamp");
            Long seconds = (Long) timestamp.get("seconds");
            Integer nanos = (Integer) timestamp.get("nanos");
            builder.setTimestamp(Timestamp.newBuilder()
                    .setSeconds(seconds)
                    .setNanos(nanos)
                    .build());
        }
        {
            GenericData.Record firstProvider = (GenericData.Record) record.get("firstProvider");
            if (firstProvider != null) {
                MessiProvider.Builder firstProviderBuilder = MessiProvider.newBuilder();
                Long publishedTimestamp = (Long) ofNullable(firstProvider.get("publishedTimestamp")).orElse(null);
                if (publishedTimestamp != null) {
                    firstProviderBuilder.setPublishedTimestamp(publishedTimestamp);
                }
                String shardId = ofNullable(firstProvider.get("shardId")).map(Object::toString).orElse(null);
                if (shardId != null) {
                    firstProviderBuilder.setShardId(shardId);
                }
                String sequenceNumber = ofNullable(firstProvider.get("sequenceNumber")).map(Object::toString).orElse(null);
                if (sequenceNumber != null) {
                    firstProviderBuilder.setSequenceNumber(sequenceNumber);
                }
                String technology = ofNullable(firstProvider.get("technology")).map(Object::toString).orElse(null);
                if (technology != null) {
                    firstProviderBuilder.setTechnology(technology);
                }
                builder.setFirstProvider(firstProviderBuilder.build());
            }
        }
        return builder;
    }

    public static GenericRecord toAvro(ULID.Value ulidValue, MessiMessage message) {
        GenericRecord record = new GenericData.Record(schema);
        {
            GenericData.Record ulid = new GenericData.Record(ulidSchema);
            ulid.put("msb", ulidValue.getMostSignificantBits());
            ulid.put("lsb", ulidValue.getLeastSignificantBits());
            record.put("ulid", ulid);
        }
        record.put("externalId", message.getExternalId());
        if (message.hasPartitionKey()) {
            record.put("partitionKey", message.getPartitionKey());
        }
        record.put("attributes", message.getAttributesMap());
        record.put("data", message.getDataMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asReadOnlyByteBuffer())));
        if (message.hasSource()) {
            GenericData.Record source = new GenericData.Record(sourceSchema);
            source.put("clientSourceId", message.getSource().getClientSourceId());
            record.put("source", source);
        }
        if (message.hasProvider()) {
            GenericData.Record provider = new GenericData.Record(providerSchema);
            provider.put("publishedTimestamp", message.getProvider().getPublishedTimestamp());
            provider.put("shardId", message.getProvider().getShardId());
            provider.put("sequenceNumber", message.getProvider().getSequenceNumber());
            provider.put("technology", message.getProvider().getTechnology());
            record.put("provider", provider);
        }
        if (message.hasOrdering()) {
            GenericData.Record ordering = new GenericData.Record(orderingSchema);
            ordering.put("group", message.getOrdering().getGroup());
            ordering.put("sequenceNumber", message.getOrdering().getSequenceNumber());
            record.put("ordering", ordering);
        }
        {
            GenericData.Record timestamp = new GenericData.Record(timestampSchema);
            timestamp.put("seconds", message.getTimestamp().getSeconds());
            timestamp.put("nanos", message.getTimestamp().getNanos());
            record.put("timestamp", timestamp);
        }
        if (message.hasFirstProvider()) {
            GenericData.Record firstProvider = new GenericData.Record(providerSchema);
            firstProvider.put("publishedTimestamp", message.getFirstProvider().getPublishedTimestamp());
            firstProvider.put("shardId", message.getFirstProvider().getShardId());
            firstProvider.put("sequenceNumber", message.getFirstProvider().getSequenceNumber());
            firstProvider.put("technology", message.getFirstProvider().getTechnology());
            record.put("firstProvider", firstProvider);
        }
        return record;
    }

    public static void deserializePostProcessing(MessiMessage.Builder builder, String providerTechnology) {
        if (builder.hasProvider()) {
            builder.getProviderBuilder().setTechnology(providerTechnology);
        } else {
            builder.setProvider(MessiProvider.newBuilder()
                    .setTechnology(providerTechnology)
                    .build());
        }
    }
}
