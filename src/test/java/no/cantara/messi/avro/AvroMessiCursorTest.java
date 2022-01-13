package no.cantara.messi.avro;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class AvroMessiCursorTest {

    @Test
    public void truncateZero() {
        long l = 0L;
        int a = (int) l;
        assertEquals(a, 0);
        int ta = AvroMessiCursor.truncate(l);
        assertEquals(ta, 0);
    }

    @Test
    public void truncatePositiveSmallNumber() {
        long l = 100L;
        int a = (int) l;
        assertEquals(a, 100);
        int ta = AvroMessiCursor.truncate(l);
        assertEquals(ta, 100);
    }

    @Test
    public void truncateNegativeSmallNumber() {
        long l = -100L;
        int a = (int) l;
        assertEquals(a, -100);
        int ta = AvroMessiCursor.truncate(l);
        assertEquals(ta, -100);
    }

    @Test
    public void truncateTooBigLongToInt() {
        long l = Integer.MAX_VALUE + 5L;
        int a = (int) l;
        assertNotEquals(a, Integer.MAX_VALUE); // normal casting does not work
        int ta = AvroMessiCursor.truncate(l);
        assertEquals(ta, Integer.MAX_VALUE);
    }

    @Test
    public void truncateTooSmallLongToInt() {
        long l = Integer.MIN_VALUE - 5L;
        int a = (int) l;
        assertNotEquals(a, Integer.MIN_VALUE); // normal casting does not work
        int ta = AvroMessiCursor.truncate(l);
        assertEquals(ta, Integer.MIN_VALUE);
    }
}
