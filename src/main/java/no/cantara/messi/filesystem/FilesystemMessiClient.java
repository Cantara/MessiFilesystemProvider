package no.cantara.messi.filesystem;

import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.avro.AvroMessiClient;
import no.cantara.messi.avro.AvroMessiUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesystemMessiClient extends AvroMessiClient {

    final Path storageFolder;

    public FilesystemMessiClient(Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, int fileListingMinIntervalSeconds, AvroMessiUtils readOnlyAvroRawdataUtils, AvroMessiUtils readWriteAvroRawdataUtils, Path storageFolder) {
        super(tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, fileListingMinIntervalSeconds, readOnlyAvroRawdataUtils, readWriteAvroRawdataUtils);
        this.storageFolder = storageFolder;
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        Path metadataFolder = createMetadataFolderIfNotExists(topic);
        return new FilesystemMessiMetadataClient(metadataFolder, topic);
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
