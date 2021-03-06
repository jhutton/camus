package com.linkedin.camus.coders;


import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Container for messages.  Enables the use of a custom message decoder with knowledge
 * of where these values are stored in the message schema
 *
 * @author kgoodhop
 *
 * @param <R> The type of decoded payload
 */
public class CamusWrapper<R> {
    private static final Text SERVER = new Text("server");
    private static final Text SERVICE = new Text("service");

    private R record;
    private long timestamp;
    private final MapWritable partitionMap = new MapWritable();

    public CamusWrapper() {
    }

    public CamusWrapper(R record) {
        this.record = record;
    }

    public void set(R record) {
        set(record, System.currentTimeMillis());
    }

    public void set(R record, long timestamp) {
        set(record, timestamp, "unknown_server", "unknown_service");
    }

    public void set(R record, long timestamp, String server, String service) {
        this.record = record;
        this.timestamp = timestamp;
        partitionMap.clear();
        partitionMap.put(SERVER, new Text(server));
        partitionMap.put(SERVICE, new Text(service));
    }

    /**
     * Returns the payload record for a single message
     * @return
     */
    public R getRecord() {
        return record;
    }

    /**
     * Returns current if not set by the decoder
     * @return
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Add a value for partitions
     */
    public void put(Writable key, Writable value) {
        partitionMap.put(key, value);
    }

    /**
     * Get a value for partitions
     * @return the value for the given key
     */
    public Writable get(Writable key) {
        return partitionMap.get(key);
    }

    /**
     * Get all the partition key/partitionMap
     */
    public MapWritable getPartitionMap() {
        return partitionMap;
    }

    public void clear() {
        record = null;
        timestamp = 0;
        partitionMap.clear();
    }

}
