package timely.store.iterators;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import timely.adapter.accumulo.MetricAdapter;
import timely.store.iterators.DataPointsCompactionIterator.DirectByteBufferAllocator;

public class DataPointsCompactionIteratorTest extends IteratorTestBase {

    static {
        System.setProperty("java.library.path",
                "/local/dlmari2/Software/Blosc/c-blosc-1.11.1/include:/local/dlmari2/Software/Blosc/c-blosc-1.11.1/lib");
    }

    private static final Random RANDOM = new Random(351451234L);
    private DirectByteBufferAllocator allocator = new DirectByteBufferAllocator();

    @After
    public void tearDown() throws Exception {
        allocator.cleanup();
    }

    @Test
    public void testCompactSingleDay() throws Exception {

        ByteBuffer scratch = ByteBuffer.allocate(Double.BYTES);

        Instant now = Instant.now();
        Instant yesterday = now.minus(Duration.ofDays(1));
        yesterday = yesterday.truncatedTo(ChronoUnit.DAYS);
        /*
         * Create a table and populate it with 1440 rows, 30 seconds apart
         */
        TreeMap<Key, Value> table = new TreeMap<>();
        long timestamp = yesterday.toEpochMilli();
        final String metric = "sys.cpu.user";
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];

        for (double i = 0; i < 1440; i++) {
            // report a new random value every 60s
            scratch.clear();
            scratch.putDouble(RANDOM.nextDouble() * RANDOM.nextLong());
            timestamp += 30000;
            byte[] row = MetricAdapter.encodeRowKey(metric, timestamp);
            table.put(new Key(row, colf, colq, viz, timestamp), new Value(scratch.array(), true));
        }
        Assert.assertEquals(1440, table.size());

        // Iterate over the table and populate the expected values
        TreeMap<Long, Double> expected = new TreeMap<>();

        SortedMapIterator source = new SortedMapIterator(table);
        source.seek(new Range(), EMPTY_COL_FAMS, false);
        while (source.hasTop()) {
            scratch.clear();
            scratch.put(source.getTopValue().get());
            scratch.flip();
            expected.put(source.getTopKey().getTimestamp(), scratch.getDouble());
            source.next();
        }
        Assert.assertEquals(1440, expected.size());

        source = new SortedMapIterator(table);
        DataPointsCompactionIterator iter = new DataPointsCompactionIterator();
        IteratorSetting is = new IteratorSetting(1, DataPointsCompactionIterator.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "1d");
        iter.init(source, is.getOptions(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, false);

        Assert.assertTrue(iter.hasTop());
        Key topKey = iter.getTopKey();
        Assert.assertNotNull(topKey);
        Value topValue = iter.getTopValue();
        Assert.assertNotNull(topValue);

        TreeMap<Long, Double> results = DataPointsCompactionIterator.decompress(allocator, topValue);
        Assert.assertEquals(expected.size(), results.size());
        Assert.assertEquals(expected, results);

        iter.next();
        Assert.assertFalse(iter.hasTop());

    }

    @Test
    public void testManyBatches() throws Exception {
        /*
         * Create a table and populate it with 100 rows, 1 second apart where
         * the values are 10 datapoints compacted. Expect 1000 datapoints in
         * time sorted order.
         */
        TreeMap<Key, Value> table = new TreeMap<>();
        long timestamp = System.currentTimeMillis();
        long beginningTimestamp = timestamp;
        final String metric = "sys.cpu.user";
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];

        long startTimestamp = -1;
        TreeMap<Long, Double> values = new TreeMap<>();
        TreeMap<Long, Double> expected = new TreeMap<>();
        for (double i = 0; i < 1000; i++) {
            if (-1 == startTimestamp) {
                startTimestamp = timestamp;
            }
            if (i != 0 && ((i % 10) == 0)) {
                byte[] row = MetricAdapter.encodeRowKey(metric, startTimestamp);
                byte[] v = DataPointsCompactionIterator.compress(allocator, values);
                table.put(new Key(row, colf, colq, viz, startTimestamp), new Value(v, true));
                startTimestamp = -1;
                values.clear();
            }
            values.put(timestamp, i);
            expected.put(timestamp, i);
            timestamp += 1000;
        }
        byte[] row = MetricAdapter.encodeRowKey(metric, startTimestamp);
        byte[] v = DataPointsCompactionIterator.compress(allocator, values);
        table.put(new Key(row, colf, colq, viz, startTimestamp), new Value(v, true));
        Assert.assertEquals(100, table.size());

        SortedMapIterator source = new SortedMapIterator(table);
        DataPointsCompactionIterator iter = new DataPointsCompactionIterator();
        IteratorSetting is = new IteratorSetting(1, DataPointsCompactionIterator.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "1d");
        iter.init(source, is.getOptions(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, false);

        Assert.assertTrue(iter.hasTop());
        Key topKey = iter.getTopKey();
        Assert.assertNotNull(topKey);
        Assert.assertEquals(beginningTimestamp, topKey.getTimestamp());
        Value topValue = iter.getTopValue();
        Assert.assertNotNull(topValue);
        TreeMap<Long, Double> results = DataPointsCompactionIterator.decompress(allocator, topValue);
        Assert.assertEquals(expected.size(), results.size());
        Assert.assertEquals(expected, results);
        iter.next();
        Assert.assertFalse(iter.hasTop());

    }

    // TODO: Test compaction of mixed values
}
