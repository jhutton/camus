package com.linkedin.camus.etl.kafka.mapred;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.primitives.Longs;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.KafkaBrokerClient;
import com.linkedin.camus.etl.kafka.common.LeaderInfo;

/**
 * Input format for a Kafka pull job.
 */
public class EtlInputFormat extends InputFormat<EtlKey, CamusWrapper<?>> {

    public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
    public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

    public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";

    public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
    public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

    public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
    public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
    public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

    public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
    public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
    public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

    private final Logger log = Logger.getLogger(getClass());

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public RecordReader<EtlKey, CamusWrapper<?>> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new EtlRecordReader(split, context);
    }

    /**
     * Gets the topic metadata list from Kafka.
     *
     * @param context
     *            the current job context
     * @return a list of metadata
     */
    public List<TopicMetadata> getKafkaMetadata(JobContext context) {
        CamusJob.startTiming("kafkaSetupTime");
        String brokerString = CamusJob.getKafkaBrokers(context);
        List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));

        checkState(!brokers.isEmpty(), "No brokers to fetch from!");

        Collections.shuffle(brokers);
        List<TopicMetadata> topicMetadataList = null;

        for (String broker : brokers) {
            try (KafkaBrokerClient client = KafkaBrokerClient.create(broker,
                    context)) {
                topicMetadataList = client.fetchTopicMetadata();
                break;
            } catch (Exception e) {
                if (topicMetadataList == null) {
                    // fetch failed, log the exception and try the next one in
                    // the list if there are any more
                    log.error(
                            "Failed fetching metadata from broker: " + broker,
                            e);
                } else {
                    // fetch succeeded, but closing the client caused an
                    // exception
                    log.warn("Failed closing client broker: " + broker);
                    break;
                }
            }
        }

        if (topicMetadataList == null) {
            throw new RuntimeException(
                    "Failed to fetch kafka topic metadata! (See previous exceptions");
        }

        CamusJob.stopTiming("kafkaSetupTime");
        return topicMetadataList;
    }

    /**
     * Gets the latest offsets and create the requests as needed
     *
     * @param context
     * @param topicAndPartionByLeaderMap
     * @return
     */
    public List<EtlRequest> fetchLatestOffsetAndCreateEtlRequests(
            JobContext context,
            Map<LeaderInfo, List<TopicAndPartition>> topicAndPartionByLeaderMap) {
        final List<EtlRequest> finalRequests = new ArrayList<EtlRequest>();
        for (LeaderInfo leader : topicAndPartionByLeaderMap.keySet()) {

            try (KafkaBrokerClient brokerClient = createBrokerClient(context,
                    leader)) {
                List<TopicAndPartition> topicAndPartitions = topicAndPartionByLeaderMap
                        .get(leader);
                OffsetResponse earliestOffsetResponse = brokerClient
                        .fetchEarliestOffsets(topicAndPartitions);
                OffsetResponse latestOffsetResponse = brokerClient
                        .fetchLatestOffsets(topicAndPartitions);

                for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                    EtlRequest etlRequest = createEtlRequest(leader,
                            latestOffsetResponse, earliestOffsetResponse,
                            topicAndPartition);
                    finalRequests.add(etlRequest);
                }
            } catch (Exception e) {
                log.error("Failed closing broker client", e);
            }
        }
        return finalRequests;
    }

    public String createTopicRegEx(Set<String> topicsSet) {
        String regex = "";
        StringBuilder stringbuilder = new StringBuilder();
        for (String whiteList : topicsSet) {
            stringbuilder.append(whiteList);
            stringbuilder.append("|");
        }
        regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1)
                + ")";
        Pattern.compile(regex);
        return regex;
    }

    public List<TopicMetadata> filterWhitelistTopics(
            List<TopicMetadata> topicMetadataList, Set<String> whiteListTopics) {
        List<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
        String regex = createTopicRegEx(whiteListTopics);
        for (TopicMetadata topicMetadata : topicMetadataList) {
            if (Pattern.matches(regex, topicMetadata.topic())) {
                filteredTopics.add(topicMetadata);
            } else {
                log.info("Discrading topic : " + topicMetadata.topic());
            }
        }
        return filteredTopics;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {
        CamusJob.startTiming("getSplits");
        Map<LeaderInfo, List<TopicAndPartition>> topicAndPartitionByLeaderMap = getTopicPartitionInfoByLeader(context);

        if (topicAndPartitionByLeaderMap == null) {
            throw new RuntimeException("Failed to get broker metadata...");
        }

        Map<EtlRequest, EtlKey> largestOffsetKeys = getLargestPreviousOffsets(context);

        Set<String> moveToLatestTopics = getMoveToLatestTopicsSet(context);

        // Get the latest offsets and generate the EtlRequests. The requests
        // will have the earliest and latest offsets reported by the leader of
        // each partition
        List<EtlRequest> finalRequests = fetchLatestOffsetAndCreateEtlRequests(
                context, topicAndPartitionByLeaderMap);
        Collections.sort(finalRequests);

        for (EtlRequest request : finalRequests) {
            if (moveToLatestTopics.contains("all")
                    || moveToLatestTopics.contains(request.getTopic())) {
                largestOffsetKeys.put(request, new EtlKey(request.getTopic(),
                        request.getLeaderId(), request.getPartition(), 0,
                        request.getLatestOffset()));
            }

            EtlKey key = largestOffsetKeys.get(request);
            if (key != null) {
                request.setOffset(key.getOffset());
            }

            if (request.getOffset() < request.getEarliestOffset()) {
                log.warn("The request offset was found to be less than the earliest offset available in request:\n"
                        + request);
                log.warn("Moving to the earliest offset");

                request.setOffset(request.getEarliestOffset());
                largestOffsetKeys.put(request, new EtlKey(request.getTopic(),
                        request.getLeaderId(), request.getPartition(), 0,
                        request.getLatestOffset()));
            } else if (request.getOffset() > request.getLatestOffset()) {
                log.warn("The request offset was found to be greater than the latest offset available in request:\n"
                        + request);
                log.warn("Moving to the latest offset");

                request.setOffset(request.getLatestOffset());
                largestOffsetKeys.put(
                        request,
                        new EtlKey(request.getTopic(), request.getLeaderId(),
                                request.getPartition(), request
                                        .getLatestOffset(), request
                                        .getLatestOffset()));
            }

            log.info(request);
        }

        writeRequests(finalRequests, context);
        writePreviousEtlKeys(largestOffsetKeys.values(), context);
        CamusJob.stopTiming("getSplits");
        return allocateWork(finalRequests, context);
    }

    private List<InputSplit> allocateWork(List<EtlRequest> requests,
            JobContext context) {
        CamusJob.startTiming("hadoop");
        CamusJob.setTime("hadoop_start");

        int numTasks = context.getConfiguration()
                .getInt("mapred.map.tasks", 30);

        // Reverse sort by size
        Collections.sort(requests, new Comparator<EtlRequest>() {
            @Override
            public int compare(EtlRequest o1, EtlRequest o2) {
                return Longs.compare(o2.estimateDataSize(),
                        o1.estimateDataSize());
            }
        });

        final List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();
        final int reqCount = requests.size();
        int reqInx = 0;
        for (int i = 0; i < numTasks; i++) {
            EtlSplit split = new EtlSplit();

            if (reqInx < reqCount) {
                EtlRequest request = requests.get(reqInx++);
                split.addRequest(request);
                kafkaETLSplits.add(split);
            }
        }

        while (reqInx < reqCount) {
            sortSplitsByLength(kafkaETLSplits);

            for (InputSplit split : kafkaETLSplits) {
                if (reqInx < reqCount) {
                    EtlRequest request = requests.get(reqInx++);
                    ((EtlSplit) split).addRequest(request);
                } else {
                    break;
                }
            }
        }

        return kafkaETLSplits;
    }

    private void sortSplitsByLength(final List<InputSplit> kafkaETLSplits) {
        Collections.sort(kafkaETLSplits, new Comparator<InputSplit>() {
            @Override
            public int compare(InputSplit o1, InputSplit o2) {
                try {
                    if (o1.getLength() != o2.getLength()) {
                        return Longs.compare(o1.getLength(), o2.getLength());
                    } else {
                        return ((EtlSplit) o1).getNumRequests()
                                - ((EtlSplit) o2).getNumRequests();
                    }
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private EtlRequest createEtlRequest(LeaderInfo leader,
            OffsetResponse latestOffsetResponse,
            OffsetResponse earliestOffsetResponse,
            TopicAndPartition topicAndPartition) {
        long latestOffset = latestOffsetResponse.offsets(
                topicAndPartition.topic(), topicAndPartition.partition())[0];
        long earliestOffset = earliestOffsetResponse.offsets(
                topicAndPartition.topic(), topicAndPartition.partition())[0];

        // create a new etl request. The 'offset' field will be set to the
        // default, -1
        EtlRequest etlRequest = new EtlRequest(topicAndPartition.topic(),
                Integer.toString(leader.getLeaderId()),
                topicAndPartition.partition(), leader.getUri());

        etlRequest.setLatestOffset(latestOffset);
        etlRequest.setEarliestOffset(earliestOffset);
        return etlRequest;
    }

    private KafkaBrokerClient createBrokerClient(JobContext context,
            LeaderInfo leader) {
        return KafkaBrokerClient.create(leader.getUri(), context);
    }

    private Set<String> getMoveToLatestTopicsSet(JobContext context) {
        Set<String> topics = new HashSet<String>();
        String[] arr = getMoveToLatestTopics(context);

        if (arr != null) {
            for (String topic : arr) {
                topics.add(topic);
            }
        }

        return topics;
    }

    private boolean createMessageDecoder(JobContext context, String topic) {
        try {
            MessageDecoderFactory.createMessageDecoder(context, topic);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Map<LeaderInfo, List<TopicAndPartition>> getTopicPartitionInfoByLeader(
            JobContext context) {
        try {
            Map<LeaderInfo, List<TopicAndPartition>> offsetRequestInfo = new HashMap<>();

            // Get Metadata for all topics
            List<TopicMetadata> topicMetadataList = getKafkaMetadata(context);

            // Filter any white list topics
            Set<String> whiteListTopics = getKafkaWhitelistTopic(context);
            if (!whiteListTopics.isEmpty()) {
                topicMetadataList = filterWhitelistTopics(topicMetadataList,
                        whiteListTopics);
            }

            // Filter all blacklist topics
            Set<String> blackListTopics = getKafkaBlacklistTopic(context);

            String regex = "";
            if (!blackListTopics.isEmpty()) {
                regex = createTopicRegEx(blackListTopics);
            }

            for (TopicMetadata topicMetadata : topicMetadataList) {
                if (Pattern.matches(regex, topicMetadata.topic())) {
                    log.info("Discarding topic (blacklisted): "
                            + topicMetadata.topic());
                } else if (!createMessageDecoder(context, topicMetadata.topic())) {
                    log.info("Discarding topic (Decoder generation failed) : "
                            + topicMetadata.topic());
                } else {
                    for (PartitionMetadata partitionMetadata : topicMetadata
                            .partitionsMetadata()) {
                        if (partitionMetadata.errorCode() != ErrorMapping
                                .NoError()) {
                            log.info("Skipping the creation of ETL request for Topic : "
                                    + topicMetadata.topic()
                                    + " and Partition : "
                                    + partitionMetadata.partitionId()
                                    + " Exception : "
                                    + ErrorMapping
                                            .exceptionFor(partitionMetadata
                                                    .errorCode()));
                            continue;
                        } else {
                            LeaderInfo leader = new LeaderInfo(new URI("tcp://"
                                    + partitionMetadata.leader()
                                            .getConnectionString()),
                                    partitionMetadata.leader().id());

                            List<TopicAndPartition> topicAndPartitions = offsetRequestInfo
                                    .get(leader);
                            if (topicAndPartitions == null) {
                                topicAndPartitions = new ArrayList<TopicAndPartition>();
                                offsetRequestInfo.put(leader,
                                        topicAndPartitions);
                            }
                            topicAndPartitions.add(new TopicAndPartition(
                                    topicMetadata.topic(), partitionMetadata
                                            .partitionId()));
                        }
                    }
                }
            }
            return offsetRequestInfo;
        } catch (Exception e) {
            log.error(
                    "Unable to pull requests from Kafka brokers. Exiting the program",
                    e);
            return null;
        }
    }

    private void writePreviousEtlKeys(Collection<EtlKey> etlKeys,
            JobContext context) throws IOException {
        if (etlKeys.isEmpty()) {
            return;
        }

        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputPath = FileOutputFormat.getOutputPath(context);

        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }

        Path output = new Path(outputPath, EtlMultiOutputFormat.OFFSET_PREFIX
                + "-previous");

        try (SequenceFile.Writer writer = SequenceFile.createWriter(
                context.getConfiguration(), SequenceFile.Writer.file(output),
                SequenceFile.Writer.keyClass(EtlKey.class),
                SequenceFile.Writer.valueClass(NullWritable.class))) {
            for (EtlKey key : etlKeys) {
                writer.append(key, NullWritable.get());
            }
        }
    }

    private void writeRequests(List<EtlRequest> requests, JobContext context)
            throws IOException {
        if (requests.isEmpty()) {
            return;
        }

        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputPath = FileOutputFormat.getOutputPath(context);

        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }

        Path output = new Path(outputPath, EtlMultiOutputFormat.REQUESTS_FILE);

        try (SequenceFile.Writer writer = SequenceFile.createWriter(
                context.getConfiguration(), SequenceFile.Writer.file(output),
                SequenceFile.Writer.keyClass(EtlRequest.class),
                SequenceFile.Writer.valueClass(NullWritable.class))) {
            for (EtlRequest r : requests) {
                writer.append(r, NullWritable.get());
            }
        }
    }

    private Map<EtlRequest, EtlKey> getLargestPreviousOffsets(JobContext context)
            throws IOException {
        Path[] inputs = FileInputFormat.getInputPaths(context);
        Map<EtlRequest, EtlKey> offsetKeysMap = new HashMap<>();

        for (Path input : inputs) {
            log.debug("checking input path: " + input);
            FileSystem fs = input.getFileSystem(context.getConfiguration());

            for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
                log.info("previous offset file:" + f.getPath().toString());

                try (SequenceFile.Reader reader = new SequenceFile.Reader(
                        context.getConfiguration(), SequenceFile.Reader.file(f
                                .getPath()))) {
                    final EtlKey key = new EtlKey();
                    while (reader.next(key, NullWritable.get())) {
                        EtlRequest request = new EtlRequest(key.getTopic(),
                                key.getLeaderId(), key.getPartition(), null);

                        EtlKey oldKey = offsetKeysMap.get(request);
                        if (oldKey != null) {
                            if (key.getOffset() > oldKey.getOffset()) {
                                offsetKeysMap.put(request, key);
                            }
                        } else {
                            offsetKeysMap.put(request, key);
                        }
                    }
                }
            }
        }

        return offsetKeysMap;
    }

    public static void setMoveToLatestTopics(JobContext job, String val) {
        job.getConfiguration().set(KAFKA_MOVE_TO_LAST_OFFSET_LIST, val);
    }

    public static String[] getMoveToLatestTopics(JobContext job) {
        return job.getConfiguration()
                .getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
    }

    public static void setKafkaClientBufferSize(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_CLIENT_BUFFER_SIZE, val);
    }

    public static int getKafkaClientBufferSize(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_CLIENT_BUFFER_SIZE,
                2 * 1024 * 1024);
    }

    public static void setKafkaClientTimeout(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_CLIENT_SO_TIMEOUT, val);
    }

    public static int getKafkaClientTimeout(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_CLIENT_SO_TIMEOUT, 60000);
    }

    public static void setKafkaMaxPullHrs(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_MAX_PULL_HRS, val);
    }

    public static int getKafkaMaxPullHrs(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MAX_PULL_HRS, -1);
    }

    public static void setKafkaMaxPullMinutesPerTask(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, val);
    }

    public static int getKafkaMaxPullMinutesPerTask(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MAX_PULL_MINUTES_PER_TASK,
                -1);
    }

    public static void setKafkaMaxHistoricalDays(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_MAX_HISTORICAL_DAYS, val);
    }

    public static int getKafkaMaxHistoricalDays(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MAX_HISTORICAL_DAYS, -1);
    }

    public static void setKafkaBlacklistTopic(JobContext job, String val) {
        job.getConfiguration().set(KAFKA_BLACKLIST_TOPIC, val);
    }

    public static Set<String> getKafkaBlacklistTopic(JobContext job) {
        if (job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC) != null
                && !job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC).isEmpty()) {
            String[] list = job.getConfiguration().getStrings(
                    KAFKA_BLACKLIST_TOPIC);
            return new HashSet<>(Arrays.asList(list));
        } else {
            return Collections.emptySet();
        }
    }

    public static void setKafkaWhitelistTopic(JobContext job, String val) {
        job.getConfiguration().set(KAFKA_WHITELIST_TOPIC, val);
    }

    public static Set<String> getKafkaWhitelistTopic(JobContext job) {
        if (job.getConfiguration().get(KAFKA_WHITELIST_TOPIC) != null
                && !job.getConfiguration().get(KAFKA_WHITELIST_TOPIC).isEmpty()) {
            String[] list = job.getConfiguration().getStrings(
                    KAFKA_WHITELIST_TOPIC);
            return new HashSet<>(Arrays.asList(list));
        } else {
            return Collections.emptySet();
        }
    }

    public static void setEtlIgnoreSchemaErrors(JobContext job, boolean val) {
        job.getConfiguration().setBoolean(ETL_IGNORE_SCHEMA_ERRORS, val);
    }

    public static boolean getEtlIgnoreSchemaErrors(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_IGNORE_SCHEMA_ERRORS,
                false);
    }

    public static void setEtlAuditIgnoreServiceTopicList(JobContext job,
            String topics) {
        job.getConfiguration().set(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, topics);
    }

    public static String[] getEtlAuditIgnoreServiceTopicList(JobContext job) {
        return job.getConfiguration().getStrings(
                ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, "");
    }

    public static void setMessageDecoderClass(JobContext job,
            Class<? extends MessageDecoder<?>> cls) {
        job.getConfiguration().setClass(CAMUS_MESSAGE_DECODER_CLASS, cls,
                MessageDecoder.class);
    }

    public static Class<? extends MessageDecoder<?>> getMessageDecoderClass(
            JobContext job) {
        @SuppressWarnings("unchecked")
        Class<? extends MessageDecoder<?>> cls = (Class<? extends MessageDecoder<?>>) job
                .getConfiguration().getClass(CAMUS_MESSAGE_DECODER_CLASS,
                        KafkaAvroMessageDecoder.class);
        if (!(MessageDecoder.class.isAssignableFrom(cls))) {
            throw new IllegalArgumentException(
                    "Should be a message decoder class: " + cls);
        }
        return cls;
    }

    private class OffsetFileFilter implements PathFilter {

        @Override
        public boolean accept(Path arg0) {
            return arg0.getName()
                    .startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
        }
    }
}
