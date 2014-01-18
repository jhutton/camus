package com.linkedin.camus.etl.kafka.mapred;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.DefaultPartitioner;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.mapred.io.AvroRecordWriterProvider;
import com.linkedin.camus.etl.kafka.persistence.JobRecordKeeper;

/**
 * MultipleAvroOutputFormat.
 *
 * File names are determined by output keys.
 */

public class EtlMultiOutputFormat extends FileOutputFormat<EtlKey, Object> {
    public static final String ETL_DESTINATION_PATH = "etl.destination.path";
    public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";
    public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";

    public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
    public static final String ETL_DEFLATE_LEVEL = "etl.deflate.level";
    public static final String ETL_AVRO_WRITER_SYNC_INTERVAL = "etl.avro.writer.sync.interval";
    public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";

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

    private EtlMultiOutputCommitter committer;
    private JobRecordKeeper jobRecordKeeper;

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

    private RecordWriter<IEtlKey, CamusWrapper<?>> createDataRecordWriter(
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

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getOutputCommitter
     * (org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public synchronized OutputCommitter getOutputCommitter(
            TaskAttemptContext context) throws IOException {
        // this is the *first* method called by the framework after
        // instantiating this class
        if (committer == null) {
            committer = new EtlMultiOutputCommitter(getOutputPath(context),
                    context);
        }

        // we use this method as a kind of 'init()' method to create the
        // job record keeper
        jobRecordKeeper = CamusJob.getRecordKeeper(context);
        return committer;
    }

    public static void setRecordWriterProviderClass(JobContext job,
            Class<? extends RecordWriterProvider> recordWriterProviderClass) {
        job.getConfiguration().setClass(ETL_RECORD_WRITER_PROVIDER_CLASS,
                recordWriterProviderClass, RecordWriterProvider.class);
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends RecordWriterProvider> getRecordWriterProviderClass(
            JobContext job) {
        return (Class<? extends RecordWriterProvider>) job.getConfiguration()
                .getClass(ETL_RECORD_WRITER_PROVIDER_CLASS,
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

    public String getWorkingFileName(JobContext context, EtlKey key) {
        Partitioner partitioner = getPartitionerForTopic(context,
                key.getTopic());
        return "data." + key.getTopic().replaceAll("\\.", "_") + "."
                + key.getLeaderId() + "." + key.getPartition() + "."
                + partitioner.encodePartition(context, key);
    }

    public Partitioner getPartitionerForTopic(JobContext job, String topicName) {
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
        private final Writer errorWriter;
        private String currentTopic = "";

        private final ConcurrentMap<String, RecordWriter<IEtlKey, CamusWrapper<?>>> dataWriters = Maps
                .newConcurrentMap();

        public MultiEtlRecordWriter(TaskAttemptContext context)
                throws IOException {
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
                    log.warn("clearing data writers since current topic changed to "
                            + key.getTopic());
                }

                committer.incrementCount(key);
                RecordWriter<IEtlKey, CamusWrapper<?>> recordWriter = getRecordWriter(
                        key, value);
                recordWriter.write(key, value);
            } else if (val instanceof ExceptionWritable) {
                System.err.println(key.toString());
                System.err.println(val.toString());
                errorWriter.append(key, (ExceptionWritable) val);
            }
        }

        private RecordWriter<IEtlKey, CamusWrapper<?>> getRecordWriter(
                EtlKey key, final CamusWrapper<?> value) throws IOException,
                InterruptedException {
            final String workingFileName = getWorkingFileName(context, key);
            RecordWriter<IEtlKey, CamusWrapper<?>> recordWriter = dataWriters
                    .get(workingFileName);

            if (recordWriter == null) {
                dataWriters.putIfAbsent(workingFileName,
                        createDataRecordWriter(context, workingFileName, value));
                recordWriter = dataWriters.get(workingFileName);
            }

            return recordWriter;
        }
    }

    class PartitionCounts {
        final AtomicInteger count = new AtomicInteger();
        volatile EtlKey lastKey;

        PartitionCounts(EtlKey initialKey) {
            lastKey = new EtlKey(initialKey);
        }

        void increment(EtlKey key) {
            // update variable values on last key instance to the latest
            // key instance
            lastKey.setOffset(key.getOffset());
            lastKey.setTimestamp(key.getTimestamp());
            lastKey.setChecksum(key.getChecksum());
            count.incrementAndGet();
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

        public void incrementCount(EtlKey key) {
            String workingFileName = getWorkingFileName(context, key);
            PartitionCounts value = counts.get(workingFileName);

            if (value == null) {
                counts.putIfAbsent(workingFileName, new PartitionCounts(key));
                value = counts.get(workingFileName);
            }

            value.increment(key);
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path workPath = getWorkPath();
            if (isRunMoveData(context)) {
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
            }

            Iterable<EtlKey> finalKeys = Iterables.transform(counts.entrySet(),
                    new Function<Entry<String, PartitionCounts>, EtlKey>() {
                        @Override
                        public EtlKey apply(Entry<String, PartitionCounts> input) {
                            return input.getValue().lastKey;
                        }
                    });

            Path outputPath = new Path(workPath, getUniqueFile(context,
                    OFFSET_PREFIX, ""));

            jobRecordKeeper.recordFinalOffsets(finalKeys, outputPath);
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
