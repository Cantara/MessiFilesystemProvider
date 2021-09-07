package no.cantara.messi.avro;

import org.apache.avro.file.SeekableInput;

import java.nio.file.Path;

public interface MessiAvroFile {

    SeekableInput seekableInput();

    long getOffsetOfLastBlock();

    void copyFrom(Path source);
}
