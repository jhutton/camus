package com.linkedin.camus.etl.kafka.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static kafka.api.OffsetRequest.CurrentVersion;
import static kafka.api.OffsetRequest.EarliestTime;
import static kafka.api.OffsetRequest.LatestTime;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.linkedin.camus.etl.kafka.CamusJob;

/**
 * A client for interacting with kafka brokers. This class should be closed when
 * it's done being used to avoid leaking resources.
 */
public class KafkaBrokerClient implements AutoCloseable {
    private static final Logger log = Logger.getLogger(KafkaBrokerClient.class);

    public static final int DEFAULT_KAFKA_PORT = 9092;

    /**
     * Create a broker client using the specified uri and context to provide the
     * consumer configuration values.
     *
     * @param brokerUri
     *            the broker uri
     * @param context
     *            the context
     * @return a new client
     */
    public static KafkaBrokerClient create(URI brokerUri, JobContext context) {
        checkArgument(brokerUri.getPort() != -1,
                "Port must be specified in broker uri (%s)", brokerUri);
        checkNotNull(brokerUri.getHost(), "Host must be specified");

        String name = CamusJob.getKafkaClientName(context);
        Integer corrId = CamusJob.getKafkaFetchRequestCorrelationId(context);
        Integer reqMaxWait = CamusJob.getKafkaFetchRequestMaxWait(context);
        Integer minBytes = CamusJob.getKafkaFetchRequestMinBytes(context);
        Integer bufSize = CamusJob.getKafkaBufferSize(context);

        SimpleConsumer consumer = new SimpleConsumer(brokerUri.getHost(),
                brokerUri.getPort(), CamusJob.getKafkaTimeoutValue(context),
                CamusJob.getKafkaBufferSize(context), name);

        return new KafkaBrokerClient(name, consumer, corrId, reqMaxWait,
                minBytes, bufSize);
    }

    /**
     * Create a broker client using the specified uri and context to provide the
     * consumer configuration values.
     *
     * @param brokerUri
     *            the broker uri
     * @param context
     *            the context
     * @return a new client
     * @throws URISyntaxException
     *             if an invalid uri is passed
     */
    public static KafkaBrokerClient create(String brokerUri, JobContext context)
            throws URISyntaxException {
        String uriFmt = brokerUri;
        if (uriFmt.indexOf("://") == -1) {
            // add tcp scheme to make it a valid uri
            uriFmt = "tcp://" + uriFmt;
        }

        // if the port isn't specified, use the default port
        if (uriFmt.indexOf(':', 7) == -1) {
            uriFmt = uriFmt + ":" + DEFAULT_KAFKA_PORT;
        }

        final URI uri = new URI(uriFmt);
        return create(uri, context);
    }

    private final String name;
    private final SimpleConsumer consumer;
    /**
     * The "correlation id" to be used with fetch requests, which allows the
     * client and server to keep multiple requests and responses straight.
     * Usually, it's single request/ single response, so this isn't normally
     * needed to be messed with.
     */
    private Integer fetchRequestCorrelationId;
    private Integer fetchRequestMaxWait;
    private Integer fetchRequestMinBytes;
    private Integer fetchBufferSize;

    private long lastFetchTimeNanos;
    private long totalFetchTimeMillis;

    /**
     * @param name
     * @param consumer
     * @param fetchRequestCorrelationId
     * @param fetchRequestMaxWait
     * @param fetchRequestMinBytes
     * @param fetchBufferSize
     */
    private KafkaBrokerClient(String name, SimpleConsumer consumer,
            Integer fetchRequestCorrelationId, Integer fetchRequestMaxWait,
            Integer fetchRequestMinBytes, Integer fetchBufferSize) {
        checkNotNull(name);
        checkNotNull(consumer);
        checkNotNull(fetchRequestCorrelationId);
        checkNotNull(fetchRequestMaxWait);
        checkNotNull(fetchRequestMinBytes);
        checkNotNull(fetchBufferSize);
        this.name = name;
        this.consumer = consumer;
        this.fetchRequestCorrelationId = fetchRequestCorrelationId;
        this.fetchRequestMaxWait = fetchRequestMaxWait;
        this.fetchRequestMinBytes = fetchRequestMinBytes;
        this.fetchBufferSize = fetchBufferSize;
    }

    /**
     * Fetch data from the specified topic, partition and offset.
     *
     * @param topic
     *            the topic
     * @param partition
     *            the partition
     * @param offset
     *            the offset
     * @return the response returned by the underlying consumer
     */
    public FetchResponse fetch(String topic, Integer partition, Long offset) {
        TopicAndPartition tap = new TopicAndPartition(topic, partition);
        PartitionFetchInfo fetchInfo = new PartitionFetchInfo(offset,
                fetchBufferSize);

        Map<TopicAndPartition, PartitionFetchInfo> fetchSpec = ImmutableMap.of(
                tap, fetchInfo);

        FetchRequest request = new FetchRequest(fetchRequestCorrelationId,
                name, fetchRequestMaxWait, fetchRequestMinBytes, fetchSpec);

        if (log.isDebugEnabled()) {
            log.debug("Fetching data from offset: " + offset);
        }

        long tick = System.nanoTime();
        FetchResponse response = consumer.fetch(request);
        lastFetchTimeNanos = System.nanoTime() - tick;
        totalFetchTimeMillis += lastFetchTimeNanos / 1000000;
        return response;
    }

    /**
     * Get the "earliest" offset for the specified topic and partition, which is
     * to say the offset that is the oldest in the partition. The broker uri
     * used to configure this instances consumer should be the leader of the
     * partition in question.
     *
     * @param topic
     *            the topic
     * @param partition
     *            the partition
     * @return an optional offset
     */
    public Optional<Long> fetchEarliestOffset(String topic, Integer partition) {
        return fetchOffset(topic, partition, EarliestTime());
    }

    /**
     * Get the "earliest" offsets for the specified topics and partitions. The
     * broker this client is connected to should be the leader of these
     * partitions.
     *
     * @param topicsAndPartitions
     *            the topics and partitions
     * @return the offset response returned from the underlying consumer
     */
    public OffsetResponse fetchEarliestOffsets(
            List<TopicAndPartition> topicsAndPartitions) {
        return fetchOffsets(topicsAndPartitions, EarliestTime());
    }

    /**
     * Get the "latest" offset for the specified topic and partition, which is
     * to say the offset that is the most recent in the partition. The broker
     * uri used to configure this instances consumer should be the leader of the
     * partition in question.
     *
     * @param topic
     *            the topic
     * @param partition
     *            the partition
     * @return an optional offset
     */
    public Optional<Long> fetchLatestOffset(String topic, Integer partition) {
        return fetchOffset(topic, partition, LatestTime());
    }

    /**
     * Get the "latest" offsets for the specified topics and partitions. The
     * broker this client is connected to should be the leader of these
     * partitions.
     *
     * @param topicsAndPartitions
     *            the topics and partitions
     * @return the offset response returned from the underlying consumer
     */
    public OffsetResponse fetchLatestOffsets(
            List<TopicAndPartition> topicsAndPartitions) {
        return fetchOffsets(topicsAndPartitions, LatestTime());
    }

    /**
     * Get the offsets before the specified the timestamp for the given topics
     * and partitions.
     *
     * @param topicsAndPartitions
     *            the topics and partitions
     * @param timestamp
     *            the timestamp
     * @return the offset response returned from the underlying consumer
     */
    public OffsetResponse fetchOffsets(
            List<TopicAndPartition> topicsAndPartitions, long timestamp) {
        PartitionOffsetRequestInfo offsetRequestInfo = createOffsetRequestInfo(timestamp);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = Maps
                .newHashMap();
        for (TopicAndPartition topicAndPartition : topicsAndPartitions) {
            offsetInfo.put(topicAndPartition, offsetRequestInfo);
        }
        OffsetRequest request = createOffsetRequest(offsetInfo);
        return consumer.getOffsetsBefore(request);
    }

    /**
     * Get the offset before the specified timestamp for the given partition and
     * topic.
     *
     * @param topic
     *            the topic
     * @param partition
     *            the partition
     * @param timestamp
     *            the timestamp
     * @return the offset in an optional or absent if no offsets are returned
     */
    public Optional<Long> fetchOffset(String topic, Integer partition,
            long timestamp) {
        OffsetRequest request = createOffsetRequest(topic, partition, timestamp);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        long[] offsets = response.offsets(topic, partition);

        if (offsets.length > 0) {
            return Optional.of(offsets[0]);
        } else {
            return Optional.absent();
        }
    }

    /**
     * Fetch the topic metadata for all topics known to the broker we're
     * configured with.
     *
     * @return a list of all topic metadata
     */
    public List<TopicMetadata> fetchTopicMetadata() {
        return fetchTopicMetadata(Collections.<String> emptyList());
    }

    /**
     * Fetch the topic metadata for the specified topic.
     *
     * @param topic
     *            the topic to get metadata for
     * @return the optional topic metadata or absent
     */
    public Optional<TopicMetadata> fetchTopicMetadata(String topic) {
        List<TopicMetadata> metadata = fetchTopicMetadata(ImmutableList
                .of(topic));
        if (metadata.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(metadata.get(0));
        }
    }

    /**
     * Fetch the topic metadata for the specified topics.
     *
     * @param topics
     *            the topics for which to fetch metadata
     * @return a list of topic metadata
     */
    public List<TopicMetadata> fetchTopicMetadata(List<String> topics) {
        log.info(String
                .format("Fetching metadata from broker %s with client id %s for topic(s): %s",
                        consumer.host() + ":" + consumer.port(),
                        consumer.clientId(), topics));

        TopicMetadataResponse response = consumer
                .send(new TopicMetadataRequest(topics));
        return response.topicsMetadata();
    }

    public String getName() {
        return name;
    }

    public Integer getFetchRequestCorrelationId() {
        return fetchRequestCorrelationId;
    }

    public void setFetchRequestCorrelationId(Integer fetchRequestCorrelationId) {
        this.fetchRequestCorrelationId = fetchRequestCorrelationId;
    }

    public Integer getFetchRequestMaxWait() {
        return fetchRequestMaxWait;
    }

    public void setFetchRequestMaxWait(Integer fetchRequestMaxWait) {
        this.fetchRequestMaxWait = fetchRequestMaxWait;
    }

    public Integer getFetchRequestMinBytes() {
        return fetchRequestMinBytes;
    }

    public void setFetchRequestMinBytes(Integer fetchRequestMinBytes) {
        this.fetchRequestMinBytes = fetchRequestMinBytes;
    }

    public Integer getFetchBufferSize() {
        return fetchBufferSize;
    }

    public void setFetchBufferSize(Integer fetchBufferSize) {
        this.fetchBufferSize = fetchBufferSize;
    }

    public String getHost() {
        return consumer.host();
    }

    public int getPort() {
        return consumer.port();
    }

    public long getLastFetchTimeNanos() {
        return lastFetchTimeNanos;
    }

    public long getLastFetchTimeMillis() {
        return lastFetchTimeNanos / 1000000;
    }

    public long getTotalFetchTimeMillis() {
        return totalFetchTimeMillis;
    }

    /**
     * Close the underlying consumer for this client.
     *
     * @throws Exception
     *             if the close operation fails
     */
    @Override
    public void close() throws Exception {
        consumer.close();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("name", name)
                .add("consumer", consumer)
                .add("correlationId", fetchRequestCorrelationId)
                .add("fetchRequestMaxWait", fetchRequestMaxWait)
                .add("fetchRequestMinBytes", fetchRequestMinBytes)
                .add("fetchBufferSize", fetchBufferSize).toString();
    }

    /**
     * @param topic
     * @param partition
     * @param timestamp
     * @return
     */
    private OffsetRequest createOffsetRequest(String topic, Integer partition,
            long timestamp) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = ImmutableMap
                .of(new TopicAndPartition(topic, partition),
                        createOffsetRequestInfo(timestamp));
        return createOffsetRequest(offsetInfo);
    }

    /**
     * @param offsetInfo
     * @return
     */
    private OffsetRequest createOffsetRequest(
            Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo) {
        return new OffsetRequest(offsetInfo, CurrentVersion(), name);
    }

    /**
     * @param timestamp
     * @return
     */
    private PartitionOffsetRequestInfo createOffsetRequestInfo(long timestamp) {
        return createOffsetRequestInfo(timestamp, 1);

    }

    /**
     * @param timestamp
     * @param maxNumberOfOffsets
     * @return
     */
    private PartitionOffsetRequestInfo createOffsetRequestInfo(long timestamp,
            int maxNumberOfOffsets) {
        return new PartitionOffsetRequestInfo(timestamp, maxNumberOfOffsets);
    }

}
