package no.cantara.messi.filesystem;

import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.avro.AvroMessiClient;
import no.cantara.messi.avro.AvroMessiTopic;
import no.cantara.messi.avro.AvroMessiUtils;

import java.nio.file.Path;

public class FilesystemMessiClient extends AvroMessiClient {

    static final String PROVIDER_TECHNOLOGY = "Filesystem (Avro)";

    final Path storageFolder;

    public FilesystemMessiClient(Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, int fileListingMinIntervalSeconds, AvroMessiUtils readOnlyAvroMessiUtils, AvroMessiUtils readWriteAvroMessiUtils, Path storageFolder) {
        super(tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, fileListingMinIntervalSeconds, readOnlyAvroMessiUtils, readWriteAvroMessiUtils, PROVIDER_TECHNOLOGY);
        this.storageFolder = storageFolder;
    }

    @Override
    public AvroMessiTopic topicOf(String name) {
        return topicByName.computeIfAbsent(name, topicName -> new FilesystemMessiTopic(this, topicName, tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, readOnlyAvroMessiUtils, readWriteAvroMessiUtils, fileListingMinIntervalSeconds, storageFolder, PROVIDER_TECHNOLOGY));
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        MessiTopic messiTopic = topicOf(topic);
        return messiTopic.metadata();
    }
}
