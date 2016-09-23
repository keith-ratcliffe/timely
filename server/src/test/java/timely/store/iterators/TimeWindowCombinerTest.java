package timely.store.iterators;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import timely.adapter.accumulo.MetricAdapter;

public class TimeWindowCombinerTest extends IteratorTestBase {

    public static class SummingLongTimeCombiner extends TimeWindowCombiner {

        @Override
        public Value reduce(Key key, Iterator<KeyValuePair> iter) {
            final ByteBuffer scratch = ByteBuffer.allocate(Long.BYTES);
            long result = 0L;
            while (iter.hasNext()) {
                KeyValuePair kvp = iter.next();
                Value v = kvp.getValue();
                scratch.clear();
                scratch.put(v.get());
                scratch.flip();
                result += scratch.getLong();
            }
            scratch.clear();
            scratch.putLong(result);
            scratch.flip();
            return new Value(scratch.array(), true);
        }
    }

    @Test
    public void testTimeWindowCalculation1Day() throws Exception {
        TreeMap<Key, Value> table = new TreeMap<>();
        SummingLongTimeCombiner c = new SummingLongTimeCombiner();
        IteratorSetting is = new IteratorSetting(1, SummingLongTimeCombiner.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "1d");
        c.validateOptions(is.getOptions());
        c.init(new SortedMapIterator(table), is.getOptions(), SCAN_IE);

        ZonedDateTime systemTime = Instant.now().atZone(TimeWindowCombiner.UTC_ZONE);
        long time = systemTime.toEpochSecond() * 1000;
        long remainder = time % 86400000;
        ZonedDateTime windowStart = systemTime.minus(remainder, ChronoUnit.MILLIS);
        windowStart = windowStart.truncatedTo(ChronoUnit.DAYS);
        long start = windowStart.toEpochSecond() * 1000;
        long end = start + 86400000;

        Assert.assertEquals(new Pair<Long, Long>(start, end), c.getCurrentWindow(time));
        Assert.assertTrue(time >= start && time < end);
    }

    @Test
    public void testTimeWindowCalculation1Hour() throws Exception {
        TreeMap<Key, Value> table = new TreeMap<>();
        SummingLongTimeCombiner c = new SummingLongTimeCombiner();
        IteratorSetting is = new IteratorSetting(1, SummingLongTimeCombiner.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "1h");
        c.validateOptions(is.getOptions());
        c.init(new SortedMapIterator(table), is.getOptions(), SCAN_IE);

        ZonedDateTime now = ZonedDateTime.now(TimeWindowCombiner.UTC_ZONE);
        ZonedDateTime today = now.truncatedTo(ChronoUnit.HOURS);
        long start = today.toEpochSecond() * 1000;
        long end = start + 3600000;

        Assert.assertEquals(new Pair<Long, Long>(start, end), c.getCurrentWindow(now.toEpochSecond() * 1000));
        Assert.assertTrue((now.toEpochSecond() * 1000) >= start && (now.toEpochSecond() * 1000) < end);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeWindow() throws Exception {
        TreeMap<Key, Value> table = new TreeMap<>();
        SummingLongTimeCombiner c = new SummingLongTimeCombiner();
        IteratorSetting is = new IteratorSetting(1, SummingLongTimeCombiner.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "1s");
        c.validateOptions(is.getOptions());
        c.init(new SortedMapIterator(table), is.getOptions(), SCAN_IE);

    }

    private final ByteBuffer scratch = ByteBuffer.allocate(Long.BYTES);

    @Test
    public void testTimeWindow() throws Exception {

        /*
         * Create a table and populate it with 36 rows, 1 hour apart
         */
        TreeMap<Key, Value> table = new TreeMap<>();
        long timestamp = System.currentTimeMillis() - (2 * 86400000); // start
                                                                      // two
                                                                      // days
                                                                      // ago
        final String metric = "sys.cpu.user";
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];

        for (long i = 0; i < 36; i++) {
            scratch.clear();
            scratch.putLong(i);
            scratch.flip();
            timestamp += 3600000;
            byte[] row = MetricAdapter.encodeRowKey(metric, timestamp);
            table.put(new Key(row, colf, colq, viz, timestamp), new Value(scratch.array(), true));
        }
        Assert.assertEquals(36, table.size());

        /*
         * Create the expected set of results by summing the value every 10 rows
         */
        Map<Key, Long> expected = new TreeMap<>();
        Key start = null;
        LocalDate windowStart = null;
        Long aggregate = 0L;
        SortedMapIterator s = new SortedMapIterator(table);
        s.seek(new Range(), EMPTY_COL_FAMS, false);
        while (s.hasTop()) {
            if (null == start) {
                start = s.getTopKey();
                windowStart = LocalDateTime.ofInstant(Instant.ofEpochMilli(start.getTimestamp()),
                        ZoneOffset.UTC.normalized()).toLocalDate();
            }
            LocalDate thisDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(s.getTopKey().getTimestamp()),
                    ZoneOffset.UTC.normalized()).toLocalDate();
            if (!thisDate.equals(windowStart)) {
                scratch.clear();
                scratch.putLong(aggregate);
                scratch.flip();
                expected.put(start, scratch.getLong());
                aggregate = 0L;
                start = s.getTopKey();
                windowStart = thisDate;
            }
            scratch.clear();
            scratch.put(s.getTopValue().get());
            scratch.flip();
            aggregate += scratch.getLong();
            s.next();
        }
        scratch.clear();
        scratch.putLong(aggregate);
        scratch.flip();
        expected.put(start, scratch.getLong());
        Assert.assertTrue("Expected size is not correct", 3 == expected.size() || 2 == expected.size()); // could
                                                                                                         // overlap
                                                                                                         // to
                                                                                                         // three
                                                                                                         // days

        /*
         * Set up a scan iterator that will sum the values in 10 seconds windows
         */
        SummingLongTimeCombiner c = new SummingLongTimeCombiner();
        IteratorSetting is = new IteratorSetting(1, SummingLongTimeCombiner.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "1d");

        c.validateOptions(is.getOptions());
        c.init(new SortedMapIterator(table), is.getOptions(), SCAN_IE);
        c.seek(new Range(), EMPTY_COL_FAMS, false);

        Map<Key, Long> results = new TreeMap<>();
        while (c.hasTop()) {
            Key k = c.getTopKey();
            Value v = c.getTopValue();
            scratch.clear();
            scratch.put(v.get());
            scratch.flip();
            results.put((Key) k.clone(), scratch.getLong());
            c.next();
        }
        Assert.assertEquals("Sizes are not equal", expected.size(), results.size());
        Assert.assertEquals(expected, results);
        for (Entry<Key, Long> exp : expected.entrySet()) {
            Long result = results.get(exp.getKey());
            Assert.assertNotNull("result missing for key: " + exp.getKey(), result);
        }
    }

}
