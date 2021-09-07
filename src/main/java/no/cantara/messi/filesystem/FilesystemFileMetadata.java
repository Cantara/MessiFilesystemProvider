package no.cantara.messi.filesystem;

import no.cantara.messi.avro.AvroFileMetadata;
import no.cantara.messi.avro.MessiAvroFile;

import java.nio.file.Path;

class FilesystemFileMetadata extends AvroFileMetadata {

    final Path storageFolder;

    FilesystemFileMetadata(Path storageFolder) {
        this.storageFolder = storageFolder;
    }

    @Override
    public MessiAvroFile toMessiAvroFile(String topic) {
        return new FilesystemMessiAvroFile(storageFolder.resolve(topic).resolve(toFilename()));
    }
}
