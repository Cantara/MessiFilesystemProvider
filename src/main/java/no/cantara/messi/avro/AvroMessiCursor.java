package no.cantara.messi.avro;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiCursorStartingPointType;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class AvroMessiCursor implements MessiCursor {

    String shardId;
    MessiCursorStartingPointType type;
    Instant timestamp;
    String sequenceNumber;

    /**
     * Need not exactly match an existing ulid-value.
     */
    ULID.Value ulid;

    String externalId;
    Instant externalIdTimestamp;
    Duration externalIdTimestampTolerance;

    /**
     * Whether or not to include the element with ulid-value matching the lower-bound exactly.
     */
    boolean inclusive;

    /**
     * Traversal direction, true signifies forward.
     */
    final boolean forward;

    AvroMessiCursor(String shardId,
                    MessiCursorStartingPointType type,
                    Instant timestamp,
                    String sequenceNumber,
                    ULID.Value ulid,
                    String externalId,
                    Instant externalIdTimestamp,
                    Duration externalIdTimestampTolerance,
                    boolean inclusive,
                    boolean forward) {
        this.shardId = shardId;
        this.type = type;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.ulid = ulid;
        this.externalId = externalId;
        this.externalIdTimestamp = externalIdTimestamp;
        this.externalIdTimestampTolerance = externalIdTimestampTolerance;
        this.inclusive = inclusive;
        this.forward = forward;
    }

    @Override
    public String checkpoint() {
        if (type != MessiCursorStartingPointType.AT_ULID || ulid == null) {
            throw new IllegalStateException("Unable to checkpoint cursor that is not created from a compatible consumer");
        }
        return ulid + ":" + inclusive;
    }

    static class Builder implements MessiCursor.Builder {

        String shardId;
        MessiCursorStartingPointType type;
        Instant timestamp;
        String sequenceNumber;
        ULID.Value ulid;
        String externalId;
        Instant externalIdTimestamp;
        Duration externalIdTimestampTolerance;
        boolean inclusive = false;
        boolean forward = true;

        @Override
        public Builder shardId(String shardId) {
            this.shardId = shardId;
            return this;
        }

        @Override
        public Builder now() {
            this.type = MessiCursorStartingPointType.NOW;
            return this;
        }

        @Override
        public Builder oldest() {
            this.type = MessiCursorStartingPointType.OLDEST_RETAINED;
            return this;
        }

        @Override
        public Builder providerTimestamp(Instant timestamp) {
            this.type = MessiCursorStartingPointType.AT_PROVIDER_TIME;
            this.timestamp = timestamp;
            return this;
        }

        @Override
        public Builder providerSequenceNumber(String sequenceNumber) {
            this.type = MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE;
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        @Override
        public Builder ulid(ULID.Value ulid) {
            this.type = MessiCursorStartingPointType.AT_ULID;
            this.ulid = ulid;
            return this;
        }

        @Override
        public Builder externalId(String externalId, Instant externalIdTimestamp, Duration externalIdTimestampTolerance) {
            this.type = MessiCursorStartingPointType.AT_EXTERNAL_ID;
            this.externalId = externalId;
            this.externalIdTimestamp = externalIdTimestamp;
            this.externalIdTimestampTolerance = externalIdTimestampTolerance;
            return this;
        }

        @Override
        public Builder inclusive(boolean inclusive) {
            this.inclusive = inclusive;
            return this;
        }

        @Override
        public Builder checkpoint(String checkpoint) {
            this.type = MessiCursorStartingPointType.AT_ULID;
            try {
                String[] parts = checkpoint.split(":");
                this.ulid = ULID.parseULID(parts[0]);
                this.inclusive = Boolean.parseBoolean(parts[1]);
                return this;
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("checkpoint is not valid", e);
            }
        }

        @Override
        public AvroMessiCursor build() {
            Objects.requireNonNull(type);
            return new AvroMessiCursor(shardId, type, timestamp, sequenceNumber, ulid, externalId, externalIdTimestamp, externalIdTimestampTolerance, inclusive, forward);
        }
    }
}
