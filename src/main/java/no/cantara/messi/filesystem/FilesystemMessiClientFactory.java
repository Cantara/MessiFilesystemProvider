package no.cantara.messi.filesystem;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.avro.AvroMessiUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FilesystemMessiClientFactory implements MessiClientFactory {

    @Override
    public Class<?> providerClass() {
        return FilesystemMessiClient.class;
    }

    @Override
    public String alias() {
        return "filesystem";
    }

    @Override
    public MessiClient create(ApplicationProperties applicationProperties) {
        String localTempFolderConfig = applicationProperties.get("local-temp-folder");
        if (localTempFolderConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: localTempFolderConfig");
        }
        String avroFileMaxSecondsConfig = applicationProperties.get("avro-file.max.seconds");
        if (avroFileMaxSecondsConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: avroFileMaxSecondsConfig");
        }
        String avroFileMaxBytesConfig = applicationProperties.get("avro-file.max.bytes");
        if (avroFileMaxBytesConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: avroFileMaxBytesConfig");
        }
        String avroFileSyncInterval = applicationProperties.get("avro-file.sync.interval");
        if (avroFileSyncInterval == null) {
            throw new IllegalArgumentException("Missing configuration property: avroFileSyncInterval");
        }
        String listingMinIntervalSeconds = applicationProperties.get("listing.min-interval-seconds");
        if (listingMinIntervalSeconds == null) {
            throw new IllegalArgumentException("Missing configuration property: listingMinIntervalSeconds");
        }
        String filesystemStorageFolderConfig = applicationProperties.get("filesystem.storage-folder");
        if (filesystemStorageFolderConfig == null) {
            throw new IllegalArgumentException("Missing configuration property: filesystemStorageFolderConfig");
        }

        Path localTempFolder = Paths.get(localTempFolderConfig);
        long avroMaxSeconds = Long.parseLong(avroFileMaxSecondsConfig);
        long avroMaxBytes = Long.parseLong(avroFileMaxBytesConfig);
        int avroSyncInterval = Integer.parseInt(avroFileSyncInterval);
        int minListingIntervalSeconds = Integer.parseInt(listingMinIntervalSeconds);
        Path storageFolder = Paths.get(filesystemStorageFolderConfig);
        AvroMessiUtils readOnlyFilesystemRawdataUtils = new FilesystemMessiUtils(storageFolder);
        AvroMessiUtils readWriteFilesystemRawdataUtils = new FilesystemMessiUtils(storageFolder);
        return new FilesystemMessiClient(localTempFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, minListingIntervalSeconds, readOnlyFilesystemRawdataUtils, readWriteFilesystemRawdataUtils, storageFolder);
    }
}
