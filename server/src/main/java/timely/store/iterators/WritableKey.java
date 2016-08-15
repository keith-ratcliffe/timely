package timely.store.iterators;

import org.apache.accumulo.core.data.Key;

/**
 * Subclass of Accumulo Key class that enables us to reset the value of the row. 
 *
 */
public class WritableKey extends Key {

    public WritableKey(Key k) {
        super(k);
    }

    public void setRow(byte[] r) {
        this.row = r;
    }
}
