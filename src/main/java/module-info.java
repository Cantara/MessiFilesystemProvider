module messi.provider.filesystem {
    requires messi.sdk;
    requires property.config;

    requires org.slf4j;
    requires de.huxhorn.sulky.ulid;
    requires org.apache.avro;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.databind;

    provides no.cantara.messi.api.MessiClientFactory with no.cantara.messi.filesystem.FilesystemMessiClientFactory;

    exports no.cantara.messi.avro;
    exports no.cantara.messi.filesystem;
}
