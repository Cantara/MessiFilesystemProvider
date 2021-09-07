package no.cantara.messi.filesystem;

import no.cantara.messi.api.MessiMetadataClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class FilesystemMessiMetadataClient implements MessiMetadataClient {

    final Path metadataFolder;
    final String topic;

    public FilesystemMessiMetadataClient(Path metadataFolder, String topic) {
        this.metadataFolder = metadataFolder;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        try {
            return Files.list(metadataFolder)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .map(this::unescapeFilename)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String escapeFilename(String filename) {
        String escaped = filename;
        if (filename.startsWith(".")) {
            escaped = filename.replaceAll("[.]", "...");
        }
        try {
            escaped = URLEncoder.encode(escaped, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return escaped;
    }

    String unescapeFilename(String filename) {
        String unescaped = null;
        try {
            unescaped = URLDecoder.decode(filename, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        if (unescaped.startsWith("...")) {
            unescaped = unescaped.replaceAll("[.][.][.]", ".");
        }
        return unescaped;
    }

    @Override
    public byte[] get(String key) {
        Path path = metadataFolder.resolve(Paths.get(escapeFilename(key)));
        if (!Files.exists(path)) {
            return null;
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException("Path to metadata-file already exists, but is not a regular file, path: " + path.toString());
        }
        if (!Files.isReadable(path)) {
            throw new IllegalStateException("Not allowed to read metadata-file at path: " + path.toString());
        }
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MessiMetadataClient put(String key, byte[] value) {
        Path path = metadataFolder.resolve(Paths.get(escapeFilename(key)));
        try {
            Files.write(path, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public MessiMetadataClient remove(String key) {
        Path path = metadataFolder.resolve(Paths.get(escapeFilename(key)));
        try {
            Files.deleteIfExists(path);
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
