package timely.store.iterators;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Iterator designed to compact the metric timestamp and value data points into
 * an interleaved timestamp (long) and metric value (double) byte array.
 *
 */
public class DataPointsCompactionIterator extends TimeWindowCombiner {

    @Override
    public Value reduce(Key key, Iterator<KeyValuePair> iter) {
        // Use a map to re-sort all of the timestamp/values in this time window.
        Map<Long, Double> values = new TreeMap<>();
        // Add all of the timestamp and values to the map, keeping track of the
        // number of pairs
        iter.forEachRemaining(kvp -> {
            byte[] tmp = kvp.getValue().get();

            if (tmp.length == Double.BYTES) {
                values.put(kvp.getKey().getTimestamp(), ByteBuffer.wrap(tmp).getDouble());
            } else if (tmp.length % (DataPointsExpansionIterator.TIME_VALUE_LENGTH) == 0) {
                ByteBuffer scratch = ByteBuffer.wrap(tmp);
                int entries = tmp.length / DataPointsExpansionIterator.TIME_VALUE_LENGTH;
                for (int i = 0; i < entries; i++) {
                    Long timestamp = scratch.getLong();
                    Double value = scratch.getDouble();
                    values.put(timestamp, value);
                }
            } else {
                throw new RuntimeException("Incorrect number of bytes. " + tmp.length + " not a multiple of "
                        + DataPointsExpansionIterator.TIME_VALUE_LENGTH);
            }
        });

        // Write out a new combined value.
        ByteBuffer buf = ByteBuffer.allocate(values.size() * DataPointsExpansionIterator.TIME_VALUE_LENGTH);
        values.forEach((k, v) -> {
            buf.putLong(k);
            buf.putDouble(v);
        });
        return new Value(buf.array());
    }

}
