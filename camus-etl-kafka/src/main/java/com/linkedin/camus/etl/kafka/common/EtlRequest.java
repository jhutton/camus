package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;

/**
 * A class that represents the kafka pull request.
 *
 * The class is a container for topic, leaderId, partition, uri and offset. It
 * is used in reading and writing the sequence files used for the extraction
 * job.
 *
 * @author Richard Park
 */
public class EtlRequest implements Writable, Comparable<EtlRequest> {
    private static final long DEFAULT_OFFSET = -1;

    private String topic = "";
    private String leaderId = "";
    private int partition = 0;

    private URI uri;
    private long offset = DEFAULT_OFFSET;
    private long latestOffset = DEFAULT_OFFSET;
    private long earliestOffset = DEFAULT_OFFSET;

    public EtlRequest() {
    }

    public EtlRequest(EtlRequest other) {
        this.topic = other.topic;
        this.leaderId = other.leaderId;
        this.partition = other.partition;
        this.uri = other.uri;
        this.offset = other.offset;
        this.latestOffset = other.latestOffset;
        this.earliestOffset = other.earliestOffset;
    }

    /**
     * Constructor for the KafkaETLRequest with the offsets set to default
     * values.
     *
     * @param topic
     *            The topic name
     * @param leaderId
     *            The leader broker for this topic and partition
     * @param partition
     *            The partition to pull
     * @param brokerUri
     *            The uri for the broker.
     */
    public EtlRequest(String topic, String leaderId, int partition,
            URI brokerUri) {
        this.topic = topic;
        this.leaderId = leaderId;
        this.uri = brokerUri;
        this.partition = partition;
    }

    @Override
    public int compareTo(EtlRequest o) {
        if (!topic.equals(o.topic)) {
            return topic.compareTo(o.topic);
        } else {
            return partition - o.partition;
        }
    }

    /**
     * Sets the starting offset used by the kafka pull mapper.
     *
     * @param offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * Sets the broker uri for this request
     *
     * @param uri
     */
    public void setURI(URI uri) {
        this.uri = uri;
    }

    /**
     * Retrieve the broker node id.
     *
     * @return
     */
    public String getLeaderId() {
        return this.leaderId;
    }

    /**
     * Retrieve the topic
     *
     * @return
     */
    public String getTopic() {
        return this.topic;
    }

    /**
     * Retrieves the uri if set. The default is null.
     *
     * @return
     */
    public URI getURI() {
        return uri;
    }

    /**
     * Retrieves the partition number
     *
     * @return
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Retrieves the offset
     *
     * @return
     */
    public long getOffset() {
        return offset;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getEarliestOffset() {
        return earliestOffset;
    }

    public void setEarliestOffset(long earliestOffset) {
        this.earliestOffset = earliestOffset;
    }

    public long getLatestOffset() {
        return latestOffset;
    }

    public void setLatestOffset(long latestOffset) {
        this.latestOffset = latestOffset;
    }

    /**
     * Get the difference between the request offset and the latest offset.
     *
     * @return the difference in offsets or message count
     */
    public long estimateDataSize() {
        long latestOffset = getLatestOffset();
        return latestOffset - offset;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        topic = Text.readString(in);
        leaderId = Text.readString(in);
        String str = Text.readString(in);
        if (!str.isEmpty()) {
            try {
                uri = new URI(str);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        partition = in.readInt();
        offset = in.readLong();
        latestOffset = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, topic);
        Text.writeString(out, leaderId);
        if (uri != null) {
            Text.writeString(out, uri.toString());
        } else {
            Text.writeString(out, "");
        }
        out.writeInt(partition);
        out.writeLong(offset);
        out.writeLong(latestOffset);
    }

    /**
     * Returns the copy of KafkaETLRequest
     */
    @Override
    public EtlRequest clone() {
        EtlRequest request = new EtlRequest(topic, leaderId, partition, uri);
        request.setEarliestOffset(earliestOffset);
        request.setLatestOffset(latestOffset);
        request.setOffset(offset);
        return request;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("topic", topic)
                .add("leader", leaderId).add("partition", partition)
                .add("earliestOffset", earliestOffset)
                .add("latestOffset", latestOffset).add("offset", offset)
                .add("uri", uri).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topic, partition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EtlRequest other = (EtlRequest) obj;
        return Objects.equal(topic, other.topic)
                && Objects.equal(partition, other.partition);
    }

}