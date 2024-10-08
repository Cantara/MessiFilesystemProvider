package no.cantara.messi.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiCursorStartingPointType;
import no.cantara.messi.api.MessiNotCompatibleCursorException;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class AvroMessiCursor implements MessiCursor {

    private static final ObjectMapper mapper = new ObjectMapper();

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
        if (!((type != MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE && sequenceNumber != null)
                || (type != MessiCursorStartingPointType.AT_ULID && ulid != null))) {
            throw new IllegalStateException("Unable to checkpoint cursor that is not created from a compatible consumer");
        }
        ObjectNode node = mapper.createObjectNode();
        if (ulid != null) {
            node.put("ulid", ulid.toString());
        }
        if (sequenceNumber != null) {
            node.put("sequenceNumber", sequenceNumber);
        }
        node.put("inclusive", inclusive);
        return node.toString();
    }

    @Override
    public int compareTo(MessiCursor _o) throws NullPointerException, MessiNotCompatibleCursorException {
        Objects.requireNonNull(_o);
        if (!getClass().equals(_o.getClass())) {
            throw new MessiNotCompatibleCursorException(String.format("Cursor classes are not compatible. this.getClass(): %s, other.getClass(): %s", getClass(), _o.getClass()));
        }
        AvroMessiCursor o = (AvroMessiCursor) _o;
        long comparison;
        // prefer to use sequence-number if present, as this will not suffer from out-of-order ULIDs which could happen if client generates ULIDs without being careful
        if (sequenceNumber != null && o.sequenceNumber != null) {
            long seqNum = Long.parseLong(sequenceNumber);
            long otherSeqNum = Long.parseLong(o.sequenceNumber);
            comparison = seqNum - otherSeqNum;
        } else if (ulid != null && o.ulid != null) {
            comparison = ulid.compareTo(o.ulid);
        } else {
            throw new MessiNotCompatibleCursorException("Both cursors must have either ulid or sequence-number set to be compatible.");
        }
        if (comparison != 0) {
            return truncate(comparison);
        }
        if (inclusive == o.inclusive) {
            return 0;
        }
        if (inclusive) {
            return -1;
        } else {
            return 1;
        }
    }

    /**
     * Can be used instead of cast to maintain whether value is positive or negative. A normal cast from long to int
     * will toggle the sign-bit if the number of out-of-range for an int and therefore loose the sign.
     *
     * @param l the input value to truncate
     * @return the truncated int value which always have the same sign as the long input value
     */
    static int truncate(long l) {
        return (int) Math.max(Integer.MIN_VALUE, Math.min(l, Integer.MAX_VALUE));
    }

    public static class Builder implements MessiCursor.Builder {

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
            try {
                ObjectNode node = (ObjectNode) mapper.readTree(checkpoint);
                if (node.has("sequenceNumber")) {
                    this.sequenceNumber = node.get("sequenceNumber").textValue();
                    this.type = MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE;
                }
                if (node.has("ulid")) {
                    this.ulid = ULID.parseULID(node.get("ulid").textValue());
                    this.type = MessiCursorStartingPointType.AT_ULID;
                }
                this.inclusive = node.get("inclusive").booleanValue();
                return this;
            } catch (RuntimeException | JsonProcessingException e) {
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
