package no.cantara.messi.filesystem;

import no.cantara.messi.avro.MessiAvroFile;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class FilesystemMessiAvroFile implements MessiAvroFile {

    final Path path;

    FilesystemMessiAvroFile(Path path) {
        this.path = path;
    }

    @Override
    public SeekableInput seekableInput() {
        try {
            return new SeekableFileInput(path.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getOffsetOfLastBlock() {
        return FilesystemMessiUtils.getOffsetOfLastBlock(path);
    }

    @Override
    public void copyFrom(Path sourcePath) {
        try {
            Files.createDirectories(path.getParent());
            Files.copy(sourcePath, path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "FilesystemMessiAvroFile{" +
                "path=" + path +
                '}';
    }
}
