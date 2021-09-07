package no.cantara.messi.avro;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;

class AvroMessiCursor implements MessiCursor {

    final ULID.Value ulid;
    final boolean inclusive;

    AvroMessiCursor(ULID.Value ulid, boolean inclusive) {
        this.ulid = ulid;
        this.inclusive = inclusive;
    }
}
