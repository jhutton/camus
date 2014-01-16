package com.linkedin.camus.etl.kafka.mapred;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.Maps;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.coders.DefaultPartitioner;
import com.linkedin.camus.etl.kafka.common.AvroRecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;

/**
 * MultipleAvroOutputFormat.
 *
 * File names are determined by output keys.
 */

public class EtlMultiOutputFormat extends FileOutputFormat<EtlKey, Object> {
    public static final String ETL_DESTINATION_PATH = "etl.destination.path";
    public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";
    public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";
    public static final String ETL_RUN_TRACKING_POST = "etl.run.tracking.post";

    public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
    public static final String ETL_DEFLATE_LEVEL = "etl.deflate.level";
    public static final String ETL_AVRO_WRITER_SYNC_INTERVAL = "etl.avro.writer.sync.interval";
    public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";

    public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS = "kafka.monitor.time.granularity";
    public static final String ETL_DEFAULT_PARTITIONER_CLASS = "etl.partitioner.class";
    public static final String ETL_OUTPUT_CODEC = "etl.output.codec";
    public static final String ETL_DEFAULT_OUTPUT_CODEC = "deflate";
    public static final String ETL_RECORD_WRITER_PROVIDER_CLASS = "etl.record.writer.provider.class";

    public static final DateTimeFormatter FILE_DATE_FORMATTER = DateUtils
            .getDateTimeFormatter("YYYYMMddHH");
    public static final String OFFSET_PREFIX = "offsets";
    public static final String ERRORS_PREFIX = "errors";
    public static final String COUNTS_PREFIX = "counts";

    public static final String REQUESTS_FILE = "requests.previous";

    private static Logger log = Logger.getLogger(EtlMultiOutputFormat.class);

    private final ConcurrentMap<String, PartitionCounts> counts = Maps
            .newConcurrentMap();

    private final ConcurrentMap<String, Partitioner> topicPartitionerCache = Maps
            .newConcurrentMap();

    private EtlMultiOutputCommitter committer = null;

    @Override
    public RecordWriter<EtlKey, Object> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        // in the normal code path, the getOutputCommitter() is called
        // immediately
        // after creating the output format, so this shouldn't occur.
        if (committer == null) {
            throw new NullPointerException("Null output committer");
        }
        return new MultiEtlRecordWriter(context);
    }

    private RecordWriter<IEtlKey, CamusWrapper<?>> getDataRecordWriter(
            TaskAttemptContext context, String fileName, CamusWrapper<?> value)
            throws IOException, InterruptedException {
        RecordWriterProvider recordWriterProvider;
        try {
            recordWriterProvider = getRecordWriterProviderClass(context)
                    .newInstance();
        } catch (InstantiationException e) {
            throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return recordWriterProvider.getDataRecordWriter(context, fileName,
                value, committer);
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(
            TaskAttemptContext context) throws IOException {
        if (committer == null) {
            committer = new EtlMultiOutputCommitter(getOutputPath(context),
                    context);
        }

        return committer;
    }

    public static void setRecordWriterProviderClass(JobContext job,
            Class<RecordWriterProvider> recordWriterProviderClass) {
        job.getConfiguration().setClass(ETL_RECORD_WRITER_PROVIDER_CLASS,
                recordWriterProviderClass, RecordWriterProvider.class);
    }

    @SuppressWarnings("unchecked")
    public static Class<RecordWriterProvider> getRecordWriterProviderClass(
            JobContext job) {
        return (Class<RecordWriterProvider>) job.getConfiguration().getClass(
                ETL_RECORD_WRITER_PROVIDER_CLASS,
                AvroRecordWriterProvider.class);
    }

    public static void setDefaultTimeZone(JobContext job, String tz) {
        job.getConfiguration().set(ETL_DEFAULT_TIMEZONE, tz);
    }

    public static String getDefaultTimeZone(JobContext job) {
        return job.getConfiguration().get(ETL_DEFAULT_TIMEZONE,
                "America/Los_Angeles");
    }

    public static void setDestinationPath(JobContext job, Path dest) {
        job.getConfiguration().set(ETL_DESTINATION_PATH, dest.toString());
    }

    public static Path getOutputDestinationPath(JobContext job) {
        return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH));
    }

    public static void setDestPathTopicSubDir(JobContext job, String subPath) {
        job.getConfiguration().set(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY,
                subPath);
    }

    public static Path getDestPathTopicSubDir(JobContext job) {
        return new Path(job.getConfiguration().get(
                ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, "hourly"));
    }

    public static void setMonitorTimeGranularityMins(JobContext job, int mins) {
        job.getConfiguration().setInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, mins);
    }

    public static int getMonitorTimeGranularityMins(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MONITOR_TIME_GRANULARITY_MS,
                10);
    }

    public static void setEtlAvroWriterSyncInterval(JobContext job, int val) {
        job.getConfiguration().setInt(ETL_AVRO_WRITER_SYNC_INTERVAL, val);
    }

    public static int getEtlAvroWriterSyncInterval(JobContext job) {
        return job.getConfiguration().getInt(ETL_AVRO_WRITER_SYNC_INTERVAL,
                16000);
    }

    public static void setEtlDeflateLevel(JobContext job, int val) {
        job.getConfiguration().setInt(ETL_DEFLATE_LEVEL, val);
    }

    public static void setEtlOutputCodec(JobContext job, String codec) {
        job.getConfiguration().set(ETL_OUTPUT_CODEC, codec);
    }

    public static String getEtlOutputCodec(JobContext job) {
        return job.getConfiguration().get(ETL_OUTPUT_CODEC,
                ETL_DEFAULT_OUTPUT_CODEC);

    }

    public static int getEtlDeflateLevel(JobContext job) {
        return job.getConfiguration().getInt(ETL_DEFLATE_LEVEL, 6);
    }

    public static int getEtlOutputFileTimePartitionMins(JobContext job) {
        return job.getConfiguration().getInt(
                ETL_OUTPUT_FILE_TIME_PARTITION_MINS, 60);
    }

    public static void setEtlOutputFileTimePartitionMins(JobContext job, int val) {
        job.getConfiguration().setInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, val);
    }

    public static boolean isRunMoveData(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_RUN_MOVE_DATA, true);
    }

    public static void setRunMoveData(JobContext job, boolean value) {
        job.getConfiguration().setBoolean(ETL_RUN_MOVE_DATA, value);
    }

    public static boolean isRunTrackingPost(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_RUN_TRACKING_POST, false);
    }

    public static void setRunTrackingPost(JobContext job, boolean value) {
        job.getConfiguration().setBoolean(ETL_RUN_TRACKING_POST, value);
    }

    public String getWorkingFileName(JobContext context, EtlKey key)
            throws IOException {
        Partitioner partitioner = getPartitionerForTopic(context,
                key.getTopic());
        return "data." + key.getTopic().replaceAll("\\.", "_") + "."
                + key.getLeaderId() + "." + key.getPartition() + "."
                + partitioner.encodePartition(context, key);
    }

    public Partitioner getPartitionerForTopic(JobContext job, String topicName)
            throws IOException {
        Partitioner topicPartitioner = topicPartitionerCache.get(topicName);

        if (topicPartitioner != null) {
            return topicPartitioner;
        }

        // check if a custom partitioner was configured for this topic
        String className = job.getConfiguration().get(
                ETL_DEFAULT_PARTITIONER_CLASS + "." + topicName);

        if (className == null) {
            // no partitioner configured for this topic, so just use
            // the default partitioner
            className = job.getConfiguration().get(
                    ETL_DEFAULT_PARTITIONER_CLASS);
            checkNotNull(className, "No default partitioner configured!");

            topicPartitioner = createPartitioner(className, job);
            if (log.isInfoEnabled()) {
                log.info("No partitioner configured for topic (" + topicName
                        + "), using the default partitioner: " + className);
            }
        } else {
            if (log.isInfoEnabled()) {
                log.info("Custom partioner configured for topic (" + topicName
                        + "): " + className);
            }
        }

        topicPartitioner = createPartitioner(className, job);
        topicPartitionerCache.put(topicName, topicPartitioner);

        return topicPartitioner;
    }

    private Partitioner createPartitioner(String className, JobContext job) {
        Class<?> clazz;
        try {
            if (className == null) {
                clazz = DefaultPartitioner.class;
            } else {
                clazz = job.getConfiguration().getClassByName(className);
            }

            try {
                return (Partitioner) clazz.getConstructor(JobContext.class)
                        .newInstance(job);
            } catch (InstantiationException | IllegalAccessException
                    | IllegalArgumentException | InvocationTargetException
                    | NoSuchMethodException | SecurityException e) {
                throw new RuntimeException(
                        "Partitioner class may not implement single arg ctor taking a JobContext",
                        e);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    class MultiEtlRecordWriter extends RecordWriter<EtlKey, Object> {
        private final TaskAttemptContext context;
        private Writer errorWriter = null;
        private String currentTopic = "";

        private final Map<String, RecordWriter<IEtlKey, CamusWrapper<?>>> dataWriters = new HashMap<>();

        public MultiEtlRecordWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.context = context;
            errorWriter = SequenceFile.createWriter(context.getConfiguration(),
                    SequenceFile.Writer.file(new Path(committer.getWorkPath(),
                            getUniqueFile(context, ERRORS_PREFIX, ""))),
                    SequenceFile.Writer.keyClass(EtlKey.class),
                    SequenceFile.Writer.valueClass(ExceptionWritable.class));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            for (String w : dataWriters.keySet()) {
                dataWriters.get(w).close(context);
            }
            errorWriter.close();
        }

        @Override
        public void write(EtlKey key, Object val) throws IOException,
                InterruptedException {
            if (val instanceof CamusWrapper<?>) {
                final CamusWrapper<?> value = (CamusWrapper<?>) val;

                if (!key.getTopic().equals(currentTopic)) {
                    for (RecordWriter<IEtlKey, CamusWrapper<?>> writer : dataWriters
                            .values()) {
                        writer.close(context);
                    }
                    dataWriters.clear();
                    currentTopic = key.getTopic();
                }

                committer.incrementCount(key);

                String workingFileName = getWorkingFileName(context, key);
                RecordWriter<IEtlKey, CamusWrapper<?>> recordWriter = dataWriters
                        .get(workingFileName);

                if (recordWriter == null) {
                    recordWriter = getDataRecordWriter(context,
                            workingFileName, value);
                    dataWriters.put(workingFileName, recordWriter);
                }
                recordWriter.write(key, value);

            } else if (val instanceof ExceptionWritable) {
                System.err.println(key.toString());
                System.err.println(val.toString());
                errorWriter.append(key, (ExceptionWritable) val);
            }
        }
    }

    class PartitionCounts {
        final AtomicInteger count = new AtomicInteger();
        volatile EtlKey lastKey;

        void increment(EtlKey key) {
            count.incrementAndGet();
            lastKey = key;
        }
    }

    public class EtlMultiOutputCommitter extends FileOutputCommitter {
        private final TaskAttemptContext context;
        private final RecordWriterProvider recordWriterProvider;
        private final Pattern workingFileMetadataPattern;

        public EtlMultiOutputCommitter(Path outputPath,
                TaskAttemptContext context) throws IOException {
            super(outputPath, context);
            this.context = context;
            try {
                recordWriterProvider = getRecordWriterProviderClass(context)
                        .newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            workingFileMetadataPattern = Pattern
                    .compile("data\\.([^\\.]+)\\.(\\d+)\\.(\\d+)\\.([^\\.]+)-m-\\d+"
                            + recordWriterProvider.getFilenameExtension());
        }

        public void incrementCount(EtlKey key) throws IOException {
            String workingFileName = getWorkingFileName(context, key);
            PartitionCounts value = counts.get(workingFileName);

            if (value == null) {
                value = new PartitionCounts();
                PartitionCounts result = counts.putIfAbsent(workingFileName,
                        value);

                // somebody else put a value in before us for this key, so
                // we just use that
                if (result != null) {
                    value = result;
                }
            }

            value.increment(key);
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            List<Map<String, Object>> allCountObject = new ArrayList<Map<String, Object>>();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (isRunMoveData(context)) {
                Path workPath = super.getWorkPath();
                Path baseOutDir = getOutputDestinationPath(context);
                for (FileStatus f : fs.listStatus(workPath)) {
                    String file = f.getPath().getName();
                    if (file.startsWith("data")) {
                        String workingFileName = file.substring(0,
                                file.lastIndexOf("-m"));

                        PartitionCounts count = counts.get(workingFileName);
                        String outputFile = getFinalOutputFilePath(context,
                                file, count.count.get(),
                                count.lastKey.getOffset());

                        Path dest = new Path(baseOutDir, outputFile);

                        if (!fs.exists(dest.getParent())) {
                            fs.mkdirs(dest.getParent());
                        }

                        fs.rename(f.getPath(), dest);
                    }
                }

                Path tempPath = new Path(workPath, "counts."
                        + context.getConfiguration().get("mapred.task.id"));

                OutputStream outputStream = new BufferedOutputStream(
                        fs.create(tempPath));
                ObjectMapper mapper = new ObjectMapper();
                log.info("Writing counts to : " + tempPath.toString());
                long time = System.currentTimeMillis();
                mapper.writeValue(outputStream, allCountObject);
                log.debug("Time taken : " + (System.currentTimeMillis() - time)
                        / 1000);
            }

            SequenceFile.Writer offsetWriter = SequenceFile.createWriter(
                    context.getConfiguration(), SequenceFile.Writer
                            .file(new Path(super.getWorkPath(), getUniqueFile(
                                    context, OFFSET_PREFIX, ""))),
                    SequenceFile.Writer.keyClass(EtlKey.class),
                    SequenceFile.Writer.valueClass(NullWritable.class));

            for (Entry<String, PartitionCounts> entry : counts.entrySet()) {
                // write the last key passed to the committer
                offsetWriter.append(entry.getValue().lastKey,
                        NullWritable.get());
            }

            offsetWriter.close();
            super.commitTask(context);
        }

        public String getFinalOutputFilePath(JobContext context, String file,
                int count, long offset) throws IOException {
            Matcher m = workingFileMetadataPattern.matcher(file);
            if (!m.find()) {
                throw new IOException(
                        "Could not extract metadata from working filename '"
                                + file + "'");
            }
            String topic = m.group(1);
            String leaderId = m.group(2);
            String partition = m.group(3);
            String encodedPartition = m.group(4);

            String partitionedPath = getPartitionerForTopic(context, topic)
                    .generatePartitionedPath(context, topic,
                            Integer.parseInt(leaderId),
                            Integer.parseInt(partition), encodedPartition);

            return partitionedPath + "/" + topic + "." + leaderId + "."
                    + partition + "." + count + "." + offset
                    + recordWriterProvider.getFilenameExtension();
        }
    }
}
