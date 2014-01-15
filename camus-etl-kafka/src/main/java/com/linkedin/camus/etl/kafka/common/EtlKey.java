package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.linkedin.camus.etl.IEtlKey;

/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class EtlKey implements WritableComparable<EtlKey>, IEtlKey {
    public static final Text SERVER = new Text("server");
    public static final Text SERVICE = new Text("service");

    private String leaderId = "";
    private int partition = 0;
    private long beginOffset = 0;
    private long offset = 0;
    private long checksum = 0;
    private String topic = "";
    private long timestamp = 0;
    private final MapWritable partitionMap = new MapWritable();

    public EtlKey() {
    }

    public EtlKey(EtlKey other) {
        this.partition = other.partition;
        this.beginOffset = other.beginOffset;
        this.offset = other.offset;
        this.checksum = other.checksum;
        this.topic = other.topic;
        this.timestamp = other.timestamp;
        this.leaderId = other.leaderId;
        setPartition(other.getPartitionMap());
    }

    public EtlKey(String topic, String leaderId, int partition) {
        this(topic, leaderId, partition, 0, 0);
    }

    public EtlKey(String topic, String leaderId, int partition,
            long beginningOffset) {
        this(topic, leaderId, partition, beginningOffset, 0);
    }

    public EtlKey(String topic, String leaderId, int partition,
            long beginOffset, long offset) {
        this(topic, leaderId, partition, beginOffset, offset, 0, 0);
    }

    public EtlKey(String topic, String leaderId, int partition,
            long beginOffset, long offset, long timestamp, long checksum) {
        this.topic = topic;
        this.leaderId = leaderId;
        this.partition = partition;
        this.beginOffset = beginOffset;
        this.offset = offset;
        this.timestamp = timestamp;
        this.checksum = checksum;
    }

    public void clear() {
        leaderId = "";
        partition = 0;
        beginOffset = 0;
        offset = 0;
        checksum = 0;
        topic = "";
        timestamp = 0;
        partitionMap.clear();
    }


    @Override
    public String getServer() {
        return partitionMap.get(SERVER).toString();
    }

    public void setServer(String newServer) {
        partitionMap.put(SERVER, new Text(newServer));
    }

    @Override
    public String getService() {
        return partitionMap.get(SERVICE).toString();
    }

    public void setService(String newService) {
        partitionMap.put(SERVICE, new Text(newService));
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long time) {
        this.timestamp = time;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    @Override
    public int getPartition() {
        return this.partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @Override
    public long getBeginOffset() {
        return this.beginOffset;
    }

    public void setBeginOffset(long beginOffset) {
        this.beginOffset = beginOffset;
    }

    @Override
    public long getOffset() {
        return this.offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public long getChecksum() {
        return this.checksum;
    }

    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }

    @Override
    public void put(Writable key, Writable value) {
        this.partitionMap.put(key, value);
    }

    public void setPartition(MapWritable partitionMap) {
        this.partitionMap.clear();
        this.partitionMap.putAll(partitionMap);
    }

    @Override
    public MapWritable getPartitionMap() {
        return partitionMap;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.leaderId = Text.readString(in);
        this.partition = in.readInt();
        this.beginOffset = in.readLong();
        this.offset = in.readLong();
        this.checksum = in.readLong();
        this.topic = in.readUTF();
        this.timestamp = in.readLong();
        this.partitionMap.clear();
        try {
            this.partitionMap.readFields(in);
        } catch (IOException e) {
            setServer("unknown_server");
            setService("unknown_service");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.leaderId);
        out.writeInt(this.partition);
        out.writeLong(this.beginOffset);
        out.writeLong(this.offset);
        out.writeLong(this.checksum);
        out.writeUTF(this.topic);
        out.writeLong(this.timestamp);
        this.partitionMap.write(out);
    }

    @Override
    public int compareTo(EtlKey o) {
        if (partition != o.partition) {
            return partition - o.partition;
        } else if (beginOffset != o.beginOffset) {
            if (beginOffset > o.beginOffset) {
                return 1;
            } else {
                return -1;
            }
        } else if (checksum != o.checksum) {
            if (checksum > o.checksum) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        ToStringHelper helper = Objects.toStringHelper(this)
                .add("topic", topic)
                .add("partition", partition)
                .add("leaderId", leaderId)
                .add("beginOffset", beginOffset)
                .add("offset", offset)
                .add("checksum", checksum)
                .add("timestamp", timestamp);

        for (Map.Entry<Writable, Writable> e : partitionMap.entrySet()) {
            helper.add(e.getKey().toString(), e.getValue());
        }

        return helper.toString();
    }
}
