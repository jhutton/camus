package com.linkedin.camus.etl.kafka.mapred;

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

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

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

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
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

    @Override
    public RecordReader<EtlKey, CamusWrapper<?>> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new EtlRecordReader(split, context);
    }

    /**
     * Gets the topic metadata list from Kafka.
     *
     * @param context the current job context
     * @return a list of metadata
     */
    public List<TopicMetadata> getKafkaMetadata(JobContext context) {
        List<String> metaRequestTopics = new ArrayList<String>();
        CamusJob.startTiming("kafkaSetupTime");
        String brokerString = CamusJob.getKafkaBrokers(context);
        List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
        Collections.shuffle(brokers);
        boolean fetchMetaDataSucceeded = false;
        int i = 0;
        List<TopicMetadata> topicMetadataList = null;
        Exception savedException = null;
        while (i < brokers.size() && !fetchMetaDataSucceeded) {
            SimpleConsumer consumer = createConsumer(context, brokers.get(i));
            log.info(String
                    .format("Fetching metadata from broker %s with client id %s for %d topic(s) %s",
                            brokers.get(i), consumer.clientId(),
                            metaRequestTopics.size(), metaRequestTopics));
            try {
                topicMetadataList = consumer.send(
                        new TopicMetadataRequest(metaRequestTopics))
                        .topicsMetadata();
                fetchMetaDataSucceeded = true;
            } catch (Exception e) {
                savedException = e;
                log.warn(
                        String.format(
                                "Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed",
                                consumer.clientId(), metaRequestTopics,
                                brokers.get(i)), e);
            } finally {
                consumer.close();
                i++;
            }
        }
        if (!fetchMetaDataSucceeded) {
            throw new RuntimeException("Failed to obtain metadata!",
                    savedException);
        }
        CamusJob.stopTiming("kafkaSetupTime");
        return topicMetadataList;
    }

    private SimpleConsumer createConsumer(JobContext context, String broker) {
        String[] hostPort = broker.split(":");
        SimpleConsumer consumer = new SimpleConsumer(hostPort[0],
                Integer.valueOf(hostPort[1]),
                CamusJob.getKafkaTimeoutValue(context),
                CamusJob.getKafkaBufferSize(context),
                CamusJob.getKafkaClientName(context));
        return consumer;
    }

    /**
     * Gets the latest offsets and create the requests as needed
     *
     * @param context
     * @param offsetRequestInfo
     * @return
     */
    public List<EtlRequest> fetchLatestOffsetAndCreateEtlRequests(
            JobContext context,
            Map<LeaderInfo, List<TopicAndPartition>> offsetRequestInfo) {
        final List<EtlRequest> finalRequests = new ArrayList<EtlRequest>();
        for (LeaderInfo leader : offsetRequestInfo.keySet()) {
            SimpleConsumer consumer = createSimpleConsumer(context, leader);
            List<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);


            // Latest Offset request info
            PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.LatestTime(), 1);

            OffsetResponse latestOffsetResponse = getOffsetResponse(context,
                    consumer, topicAndPartitions, partitionLatestOffsetRequestInfo);

            // Earliest Offset request info
            PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.EarliestTime(), 1);
            OffsetResponse earliestOffsetResponse = getOffsetResponse(context,
                    consumer, topicAndPartitions, partitionEarliestOffsetRequestInfo);

            consumer.close();

            for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                EtlRequest etlRequest = createEtlRequest(context, leader,
                        latestOffsetResponse, earliestOffsetResponse,
                        topicAndPartition);
                finalRequests.add(etlRequest);
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
            List<TopicMetadata> topicMetadataList,
            Set<String> whiteListTopics) {
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
        Map<LeaderInfo, List<TopicAndPartition>> offsetRequestInfo = getOffsetRequestInfo(context);
        if (offsetRequestInfo == null) {
            // failed getting broker metadata
            //
            return null;
        }

        // Get the latest offsets and generate the EtlRequests
        List<EtlRequest> finalRequests = fetchLatestOffsetAndCreateEtlRequests(context,
                offsetRequestInfo);

        Collections.sort(finalRequests, new Comparator<EtlRequest>() {
            @Override
            public int compare(EtlRequest r1, EtlRequest r2) {
                return r1.getTopic().compareTo(r2.getTopic());
            }
        });

        writeRequests(finalRequests, context);
        Map<EtlRequest, EtlKey> offsetKeys = getPreviousOffsets(
                FileInputFormat.getInputPaths(context), context);

        Set<String> moveLatest = getMoveToLatestTopicsSet(context);

        for (EtlRequest request : finalRequests) {
            if (moveLatest.contains("all") ||
                    moveLatest.contains(request.getTopic())) {
                offsetKeys.put(
                        request,
                        new EtlKey(request.getTopic(), request.getLeaderId(),
                                request.getPartition(), 0, request.getLastOffset()));
            }

            EtlKey key = offsetKeys.get(request);
            if (key != null) {
                request.setOffset(key.getOffset());
            }

            boolean moveToEarliest = false;
            if (request.getEarliestOffset() > request.getOffset()) {
                log.error("The earliest offset was found to be more than the current offset");
                log.error("Moving to the earliest offset available");
                moveToEarliest = true;
            } else {
                log.error("The current offset was found to be more than the latest offset");
                log.error("Moving to the earliest offset available");
                moveToEarliest = true;
            }

            if (moveToEarliest) {
                request.setOffset(request.getEarliestOffset());
                offsetKeys.put(
                        request,
                        new EtlKey(request.getTopic(), request.getLeaderId(),
                                request.getPartition(), 0, request
                                        .getLastOffset()));
            }

            log.info(request);
        }

        writePrevious(offsetKeys.values(), context);

        CamusJob.stopTiming("getSplits");
        CamusJob.startTiming("hadoop");
        CamusJob.setTime("hadoop_start");
        return allocateWork(finalRequests, context);
    }

    private List<InputSplit> allocateWork(List<EtlRequest> requests,
            JobContext context) throws IOException {
        int numTasks = context.getConfiguration()
                .getInt("mapred.map.tasks", 30);

        // Reverse sort by size
        Collections.sort(requests, new Comparator<EtlRequest>() {
            @Override
            public int compare(EtlRequest o1, EtlRequest o2) {
                if (o2.estimateDataSize() == o1.estimateDataSize()) {
                    return 0;
                } else if (o2.estimateDataSize() < o1.estimateDataSize()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

        for (int i = 0; i < numTasks; i++) {
            EtlSplit split = new EtlSplit();

            if (requests.size() > 0) {
                EtlRequest request = requests.remove(0);
                split.addRequest(request);
                kafkaETLSplits.add(split);
            }
        }

        for (EtlRequest r : requests) {
            getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
        }

        return kafkaETLSplits;
    }

    private EtlRequest createEtlRequest(JobContext context, LeaderInfo leader,
            OffsetResponse latestOffsetResponse,
            OffsetResponse earliestOffsetResponse,
            TopicAndPartition topicAndPartition) {
        long latestOffset = latestOffsetResponse.offsets(
                topicAndPartition.topic(),
                topicAndPartition.partition())[0];
        long earliestOffset = earliestOffsetResponse.offsets(
                topicAndPartition.topic(),
                topicAndPartition.partition())[0];
        EtlRequest etlRequest = new EtlRequest(context,
                topicAndPartition.topic(), Integer.toString(leader
                        .getLeaderId()), topicAndPartition.partition(),
                leader.getUri());
        etlRequest.setLatestOffset(latestOffset);
        etlRequest.setEarliestOffset(earliestOffset);
        return etlRequest;
    }

    private SimpleConsumer createSimpleConsumer(JobContext context,
            LeaderInfo leader) {
        SimpleConsumer consumer = new SimpleConsumer(leader.getUri()
                .getHost(), leader.getUri().getPort(),
                CamusJob.getKafkaTimeoutValue(context),
                CamusJob.getKafkaBufferSize(context),
                CamusJob.getKafkaClientName(context));
        return consumer;
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

    private Map<LeaderInfo, List<TopicAndPartition>> getOffsetRequestInfo(
            JobContext context) {
        try {
            Map<LeaderInfo, List<TopicAndPartition>> offsetRequestInfo = new HashMap<LeaderInfo, List<TopicAndPartition>>();

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
                                    + partitionMetadata.leader().getConnectionString()),
                                    partitionMetadata.leader().id());

                            List<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
                            if (topicAndPartitions == null) {
                                topicAndPartitions = new ArrayList<TopicAndPartition>();
                                offsetRequestInfo.put(leader, topicAndPartitions);
                            }
                            topicAndPartitions.add(new TopicAndPartition(
                                    topicMetadata.topic(),
                                    partitionMetadata.partitionId()));
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

    private OffsetResponse getOffsetResponse(
            JobContext context,
            SimpleConsumer consumer,
            List<TopicAndPartition> topicAndPartitions,
            PartitionOffsetRequestInfo partitionRequestInfo) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> topicOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        for (TopicAndPartition topicAndPartition : topicAndPartitions) {
            topicOffsetInfo.put(topicAndPartition,
                    partitionRequestInfo);
        }

        OffsetResponse latestOffsetResponse = consumer
                .getOffsetsBefore(new OffsetRequest(topicOffsetInfo,
                        kafka.api.OffsetRequest.CurrentVersion(), CamusJob
                                .getKafkaClientName(context)));

        return latestOffsetResponse;
    }

    private EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits)
            throws IOException {
        EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

        for (int i = 1; i < kafkaETLSplits.size(); i++) {
            EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
            if (smallest.getLength() > challenger.getLength()
                    || (smallest.getLength() == challenger.getLength() && smallest
                        .getNumRequests() > challenger.getNumRequests())) {
                smallest = challenger;
            }
        }

        return smallest;
    }

    private void writePrevious(Collection<EtlKey> missedKeys, JobContext context)
            throws IOException {
        if (missedKeys.isEmpty()) {
            return;
        }

        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputPath = FileOutputFormat.getOutputPath(context);

        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }

        Path output = new Path(outputPath, EtlMultiOutputFormat.OFFSET_PREFIX
                + "-previous");

        SequenceFile.Writer writer = SequenceFile.createWriter(context.getConfiguration(),
                SequenceFile.Writer.file(output),
                SequenceFile.Writer.keyClass(EtlKey.class),
                SequenceFile.Writer.valueClass(NullWritable.class));

        try {
            for (EtlKey key : missedKeys) {
                writer.append(key, NullWritable.get());
            }
        } finally {
            writer.close();
        }
    }

    private void writeRequests(List<EtlRequest> requests, JobContext context)
            throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputPath = FileOutputFormat.getOutputPath(context);

        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }

        Path output = new Path(outputPath, EtlMultiOutputFormat.REQUESTS_FILE);
        SequenceFile.Writer writer = SequenceFile.createWriter(context.getConfiguration(),
                SequenceFile.Writer.file(output),
                SequenceFile.Writer.keyClass(EtlRequest.class),
                SequenceFile.Writer.valueClass(NullWritable.class));
        try {
            for (EtlRequest r : requests) {
                writer.append(r, NullWritable.get());
            }
        } finally {
            writer.close();
        }
    }

    private Map<EtlRequest, EtlKey> getPreviousOffsets(Path[] inputs,
            JobContext context) throws IOException {
        Map<EtlRequest, EtlKey> offsetKeysMap = new HashMap<EtlRequest, EtlKey>();

        for (Path input : inputs) {
            log.debug("checking input path: " + input);
            FileSystem fs = input.getFileSystem(context.getConfiguration());

            for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
                log.info("previous offset file:" + f.getPath().toString());
                SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(),
                        SequenceFile.Reader.file(f.getPath()));

                try {
                    final EtlKey key = new EtlKey();
                    while (reader.next(key, NullWritable.get())) {
                        EtlRequest request = new EtlRequest(context,
                                key.getTopic(), key.getLeaderId(),
                                key.getPartition());

                        EtlKey oldKey = offsetKeysMap.get(request);
                        if (oldKey != null) {
                            if (key.getOffset() > oldKey.getOffset()) {
                                offsetKeysMap.put(request, key);
                            }
                        } else {
                            offsetKeysMap.put(request, key);
                        }
                    }
                } finally {
                    reader.close();
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
            String[] list = job.getConfiguration().getStrings(KAFKA_BLACKLIST_TOPIC);
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
            String[] list = job.getConfiguration().getStrings(KAFKA_WHITELIST_TOPIC);
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

    public static Class<? extends MessageDecoder<?>> getMessageDecoderClass(JobContext job) {
        @SuppressWarnings("unchecked")
        Class<? extends MessageDecoder<?>> cls = (Class<? extends MessageDecoder<?>>) job.getConfiguration().getClass(
                CAMUS_MESSAGE_DECODER_CLASS, KafkaAvroMessageDecoder.class);
        if (!(MessageDecoder.class.isAssignableFrom(cls))) {
            throw new IllegalArgumentException("Should be a message decoder class: " + cls);
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
