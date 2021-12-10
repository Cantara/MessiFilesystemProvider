package no.cantara.messi.filesystem;

import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.avro.AvroMessiTopic;
import no.cantara.messi.avro.AvroMessiUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesystemMessiTopic extends AvroMessiTopic {

    final Path storageFolder;

    public FilesystemMessiTopic(FilesystemMessiClient messiClient, String name, Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, AvroMessiUtils readOnlyAvroMessiUtils, AvroMessiUtils readWriteAvroMessiUtils, int fileListingMinIntervalSeconds, Path storageFolder, String providerTechnology) {
        super(messiClient, name, tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, readOnlyAvroMessiUtils, readWriteAvroMessiUtils, fileListingMinIntervalSeconds, providerTechnology);
        this.storageFolder = storageFolder;
    }

    @Override
    public MessiMetadataClient metadata() {
        Path metadataFolder = createMetadataFolderIfNotExists(name);
        return new FilesystemMessiMetadataClient(metadataFolder, name);
    }

    Path createMetadataFolderIfNotExists(String topic) {
        Path metadataFolder = storageFolder.resolve(topic).resolve("metadata");
        try {
            Files.createDirectories(metadataFolder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadataFolder;
    }
}
