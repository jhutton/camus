package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

import com.linkedin.camus.etl.kafka.CamusJob;

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

    private JobContext context;
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
    public EtlRequest(JobContext context, String topic, String leaderId,
            int partition, URI brokerUri) {
        this.context = context;
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
        if (earliestOffset == -1) {
            SimpleConsumer consumer = createSimpleConsumer();

            try {
                Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = new HashMap<>();
                offsetInfo.put(
                        new TopicAndPartition(topic, partition),
                        new PartitionOffsetRequestInfo(kafka.api.OffsetRequest
                                .EarliestTime(), 1));

                OffsetResponse response = consumer
                        .getOffsetsBefore(new OffsetRequest(offsetInfo,
                                kafka.api.OffsetRequest.CurrentVersion(),
                                CamusJob.getKafkaClientName(context)));

                long[] offsets = response.offsets(topic, partition);

                if (offsets.length > 0) {
                    earliestOffset = offsets[0];
                } else {
                    earliestOffset = 0;
                }
            } finally {
                consumer.close();
            }
        }

        return earliestOffset;
    }

    public void setEarliestOffset(long earliestOffset) {
        this.earliestOffset = earliestOffset;
    }

    public long getLatestOffset() {
        if (latestOffset == -1) {
            this.latestOffset = getLatestOffset(kafka.api.OffsetRequest
                    .LatestTime());
        }

        return latestOffset;
    }

    public long getLatestOffset(long timestamp) {
        SimpleConsumer consumer = createSimpleConsumer();

        try {
            Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = new HashMap<>();
            offsetInfo.put(new TopicAndPartition(topic, partition),
                    new PartitionOffsetRequestInfo(timestamp, 1));

            OffsetResponse response = consumer
                    .getOffsetsBefore(new OffsetRequest(offsetInfo,
                            kafka.api.OffsetRequest.CurrentVersion(), CamusJob
                                    .getKafkaClientName(context)));

            long[] offsets = response.offsets(topic, partition);

            if (offsets.length > 0) {
                return offsets[0];
            } else {
                // if no information is found, set the offset at the beginning
                return 0;
            }
        } finally {
            consumer.close();
        }
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

    /**
     * Get the difference between the request offset and the latest offset based
     * on the specified end time.
     *
     * @param endTime
     *            the end time in milliseconds
     * @return the difference in offsets or message count
     */
    public long estimateDataSize(long endTime) {
        long latestOffset = getLatestOffset(endTime);
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
        EtlRequest request = new EtlRequest(context, topic, leaderId,
                partition, uri);
        request.setEarliestOffset(earliestOffset);
        request.setLatestOffset(latestOffset);
        request.setOffset(offset);
        return request;
    }

    @Override
    public String toString() {
        return topic + "\turi:" + (uri != null ? uri.toString() : "")
                + "\tleader:" + leaderId + "\tpartition:" + partition
                + "\tearliest_offset:" + earliestOffset + "\toffset:" + offset
                + "\tlatest_offset:" + latestOffset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + partition;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        return result;
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
        if (partition != other.partition)
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }

    private SimpleConsumer createSimpleConsumer() {
        if (uri == null) {
            throw new NullPointerException("uri not set");
        }

        SimpleConsumer consumer = new SimpleConsumer(uri.getHost(),
                uri.getPort(), CamusJob.getKafkaTimeoutValue(context),
                CamusJob.getKafkaBufferSize(context),
                CamusJob.getKafkaClientName(context));
        return consumer;
    }
}