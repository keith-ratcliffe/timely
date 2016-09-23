package timely.store.iterators;

import io.github.dlmarion.clowncar.Blosc;
import io.github.dlmarion.clowncar.BloscCompressorType;
import io.github.dlmarion.clowncar.BloscShuffleType;
import io.github.dlmarion.clowncar.jnr.BloscLibrary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

/**
 * Iterator designed to compact the metric timestamp and value data points into
 * an interleaved timestamp (long) and metric value (double) byte array.
 *
 */
@SuppressWarnings("restriction")
public class DataPointsCompactionIterator extends TimeWindowCombiner {

    public static class DirectByteBufferAllocator {

        /* Track native memory allocations */
        private List<ByteBuffer> allocations = new ArrayList<>();

        /**
         * Allocate a direct memory byte buffer
         * 
         * @param size
         * @return direct memory byte buffer
         */
        public ByteBuffer allocateBuffer(int size) {
            ByteBuffer buf = ByteBuffer.allocateDirect(size);
            allocations.add(buf);
            return buf;
        }

        /**
         * Cleanup direct memory allocations
         */
        public void cleanup() {
            allocations.forEach(b -> {
                ((DirectBuffer) b).cleaner().clean();
            });
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(DataPointsCompactionIterator.class);

    private DirectByteBufferAllocator allocator = new DirectByteBufferAllocator();

    @Override
    protected void finalize() throws Throwable {
        allocator.cleanup();
        super.finalize();
    }

    @Override
    public Value reduce(Key key, Iterator<KeyValuePair> iter) {
        try {
            // Use a map to re-sort all of the timestamp/values in this time
            // window.
            Map<Long, Double> values = new TreeMap<>();
            // Add all of the timestamp and values to the map, keeping track of
            // the
            // number of pairs
            iter.forEachRemaining(kvp -> {
                byte[] tmp = kvp.getValue().get();

                if (tmp.length == Double.BYTES) {
                    values.put(kvp.getKey().getTimestamp(), ByteBuffer.wrap(tmp).getDouble());
                } else if (tmp.length > Double.BYTES) {
                    values.putAll(decompress(allocator, kvp.getValue()));
                } else {
                    LOG.warn("Not writing {} bytes", Arrays.toString(tmp));
                }
            });
            LOG.debug("{} entries in data points map", values.size());

            byte[] buf = compress(allocator, values);
            return new Value(buf, 0, buf.length);
        } finally {
            allocator.cleanup();
        }
    }

    public static byte[] compress(DirectByteBufferAllocator allocator, Map<Long, Double> values) {
        // Write out a new combined value.
        ByteBuffer vals = allocator.allocateBuffer(values.size() * (Long.BYTES * 2));
        ByteBuffer times = vals.slice(); // write the timestamps starting at the
                                         // beginning
        vals.position(values.size() * Long.BYTES);
        ByteBuffer metrics = vals.slice(); // write the values after the
                                           // timestamps
        values.forEach((k, v) -> {
            times.putLong(k);
            metrics.putDouble(v);
        });
        vals.flip(); // get ready to read

        ByteBuffer compressedData = allocator.allocateBuffer(vals.capacity() + Blosc.OVERHEAD);
        int written = compressValues(vals, compressedData);
        compressedData.position(0);
        compressedData.limit(written);
        LOG.debug("Compressed {} timestamps/values ({} bytes) to {} bytes", values.size(),
                2 * Long.BYTES * values.size(), written);

        byte[] buf = new byte[12 + written];
        ByteBuffer wbuf = ByteBuffer.wrap(buf);
        wbuf.putInt(values.size());
        wbuf.putInt(written);
        wbuf.put(compressedData);
        return buf;
    }

    public static TreeMap<Long, Double> decompress(DirectByteBufferAllocator allocator, Value value) {
        TreeMap<Long, Double> results = new TreeMap<>();
        ByteBuffer scratch = allocator.allocateBuffer(value.get().length);
        scratch.put(value.get());
        scratch.flip();
        int num = scratch.getInt();
        LOG.trace("{} compressed timestamps/values", num);
        int len = scratch.getInt();
        LOG.trace("compressed bytes: {}", len);
        ByteBuffer src = scratch.slice();

        ByteBuffer dst = allocator.allocateBuffer(2 * num * Long.BYTES + Blosc.OVERHEAD);
        BloscLibrary.decompress(src, dst, dst.capacity(), 4);

        // Get a view of the times from the dst buffer
        dst.position(0);
        ByteBuffer times = dst.slice();
        times.limit(num * Long.BYTES);
        // Get a view of the metrics from the dst buffer
        dst.position(num * Long.BYTES);
        ByteBuffer metrics = dst.slice();

        for (int i = 0; i < num; i++) {
            Long timestamp = times.getLong();
            Double val = metrics.getDouble();
            results.put(timestamp, val);
        }
        return results;
    }

    public static int compressValues(ByteBuffer values, ByteBuffer dst) {
        return BloscLibrary
                .compress(6, BloscShuffleType.BIT_SHUFFLE.getShuffleType(), Double.BYTES, values, values.capacity(),
                        dst, dst.capacity(), BloscCompressorType.ZLIB.getCompressorName(), dst.capacity(), 4);
    }

}
