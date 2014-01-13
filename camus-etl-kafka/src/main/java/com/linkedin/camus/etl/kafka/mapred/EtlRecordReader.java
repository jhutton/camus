package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import kafka.message.Message;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.KafkaReader;

public class EtlRecordReader extends RecordReader<EtlKey, CamusWrapper<?>> {
    private static Logger log = Logger.getLogger(EtlRecordReader.class);
    private static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";
    private static final String DEFAULT_SERVER = "server";
    private static final String DEFAULT_SERVICE = "service";

    private Mapper<EtlKey, Writable, EtlKey, Writable>.Context context;

    private boolean skipSchemaErrors = false;
    private final BytesWritable msgValue = new BytesWritable();
    private final BytesWritable msgKey = new BytesWritable();
    private final EtlKey key = new EtlKey();
    private CamusWrapper<?> value;

    private int maxPullHours = 0;
    private int exceptionCount = 0;
    private long maxPullTime = 0;
    private long beginTimeStamp = 0;
    private long endTimeStamp = 0;
    private final Set<String> ignoreServerServiceList = new HashSet<String>();
    private final StringBuilder statusMsgBuilder = new StringBuilder();

    private final EtlSplit split;
    private final long totalBytes;

    private KafkaReader reader;
    private MessageDecoder<?> decoder;
    private long readBytes = 0;

    /**
     * Create an instance of the record reader for the specified split and
     * context.
     *
     * @param split
     *            the split
     * @param context
     *            the task context
     * @throws IOException
     * @throws InterruptedException
     */
    public EtlRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.split = (EtlSplit) split;

        this.skipSchemaErrors = EtlInputFormat
                .getEtlIgnoreSchemaErrors(context);

        int maxPullHours = EtlInputFormat.getKafkaMaxPullHrs(context);
        if (maxPullHours > 0) {
            this.maxPullHours = maxPullHours;
        } else {
            this.endTimeStamp = Long.MAX_VALUE;
        }

        int maxPullMinutes = EtlInputFormat
                .getKafkaMaxPullMinutesPerTask(context);
        if (maxPullMinutes > 0) {
            DateTime now = new DateTime();
            this.maxPullTime = now.plusMinutes(maxPullMinutes).getMillis();
        } else {
            this.maxPullTime = Long.MAX_VALUE;
        }

        int maxHistoricalDays = EtlInputFormat
                .getKafkaMaxHistoricalDays(context);
        if (maxHistoricalDays != -1) {
            beginTimeStamp = (new DateTime()).minusDays(maxHistoricalDays)
                    .getMillis();
        } else {
            beginTimeStamp = 0;
        }

        for (String ignoreServerServiceTopic : EtlInputFormat
                .getEtlAuditIgnoreServiceTopicList(context)) {
            ignoreServerServiceList.add(ignoreServerServiceTopic);
        }

        this.totalBytes = split.getLength();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // The context instance here should be a {@link Mapper.Context}
        // implementation,
        // while the one passed to the constructor will not be, which is why we
        // can't make
        // the context a final variable and assign it in the constructor
        this.context = (Mapper<EtlKey, Writable, EtlKey, Writable>.Context) context;
    }

    @Override
    public synchronized void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (getPos() == 0) {
            return 0f;
        }

        if (getPos() >= totalBytes) {
            return 1f;
        }
        return (float) ((double) getPos() / totalBytes);
    }

    private long getPos() throws IOException {
        if (reader != null) {
            return readBytes + reader.getReadBytes();
        } else {
            return readBytes;
        }
    }

    @Override
    public EtlKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public CamusWrapper<?> getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (true) {
            try {
                if (reader == null || !reader.hasNext()) {
                    if (!initReaderWithSplit()) {
                        return false;
                    }
                }

                while (reader.getNext(key, msgValue, msgKey)) {
                    updateCountersAndProgress();

                    final byte[] valueBytes = msgValue.copyBytes();
                    final byte[] keyBytes = msgKey.copyBytes();

                    // check the checksum of message. If message has partition
                    // key, need to construct it with Key for checkSum to match
                    //
                    final Message message;
                    if (keyBytes.length == 0) {
                        message = new Message(valueBytes);
                    } else {
                        message = new Message(valueBytes, keyBytes);
                    }

                    validateKey(message);

                    value = decodeRecord(valueBytes);
                    if (value == null) {
                        continue;
                    }

                    final long timeStamp = value.getTimestamp();
                    try {
                        key.setTime(timeStamp);
                        key.setPartition(value.getPartitionMap());
                        if (ignoreServerServiceList.contains(key.getTopic())
                                || ignoreServerServiceList.contains("all")) {
                            key.setServer(DEFAULT_SERVER);
                            key.setService(DEFAULT_SERVICE);
                        }
                    } catch (Exception e) {
                        context.write(key, new ExceptionWritable(e));
                        continue;
                    }

                    if (timeStamp < beginTimeStamp) {
                        // skip records until we reach our time to begin
                        // processing
                        //
                        context.getCounter("total", "skip-old").increment(1);
                        continue;
                    } else if (endTimeStamp == 0) {
                        setStartStatus(timeStamp);
                    } else if (timeStamp > endTimeStamp) {
                        setEndStatus(timeStamp,
                                "Kafka Max history hours reached for " + key);
                        closeReader();
                        // break out and try any other partitions that are
                        // queued
                        break;
                    } else if (timeStamp > maxPullTime) {
                        setEndStatus(timeStamp,
                                "Kafka task total pull time limit reached");
                        closeReader();
                        return false;
                    }

                    return true;
                }

                closeReader();
            } catch (Exception e) {
                context.write(key, new ExceptionWritable(e));
                closeReader();
            }
        }
    }

    private CamusWrapper<?> decodeRecord(byte[] bytes) throws IOException,
            InterruptedException {
        final long decodeStart = System.currentTimeMillis();
        final CamusWrapper<?> wrapper;
        try {
            wrapper = getWrappedRecord(key.getTopic(), bytes);
        } catch (Exception e) {
            maybePrintException(e);
            return null;
        }

        if (wrapper == null) {
            context.write(key, new ExceptionWritable(new RuntimeException(
                    "null record")));
        } else {
            long decodeTime = System.currentTimeMillis() - decodeStart;
            context.getCounter("total", "decode-time(ms)")
                    .increment(decodeTime);
        }

        return wrapper;
    }

    private CamusWrapper<?> getWrappedRecord(String topicName, byte[] payload)
            throws IOException {
        CamusWrapper<?> r = null;
        try {
            r = decoder.decode(payload);
        } catch (Exception e) {
            if (!skipSchemaErrors) {
                throw new IOException(e);
            }
        }
        return r;
    }

    private void maybePrintException(Exception e) throws IOException,
            InterruptedException {
        if (exceptionCount < getMaximumDecoderExceptionsToPrint(context)) {
            context.write(key, new ExceptionWritable(e));
            exceptionCount++;
        } else if (exceptionCount == getMaximumDecoderExceptionsToPrint(context)) {
            exceptionCount = Integer.MAX_VALUE;
            log.info("The same exception has occured for more than "
                    + getMaximumDecoderExceptionsToPrint(context)
                    + " records. All further exceptions will not be printed");
        }
    }

    private void setEndStatus(long timeStamp, String message)
            throws IOException {
        log.info(message);
        DateTime time = new DateTime(timeStamp);
        statusMsgBuilder.append(" max read at ").append(time.toString());
        context.setStatus(statusMsgBuilder.toString());
        log.info(key.getTopic() + " max read at " + time.toString());
    }

    private void setStartStatus(final long timeStamp) {
        DateTime time = new DateTime(timeStamp);
        statusMsgBuilder.append(" begin read at ").append(time.toString());
        context.setStatus(statusMsgBuilder.toString());
        log.info(key.getTopic() + " begin read at " + time.toString());
        endTimeStamp = (time.plusHours(this.maxPullHours)).getMillis();
    }

    private void updateCountersAndProgress() {
        context.progress();
        context.getCounter("total", "data-read")
                .increment(msgValue.getLength());
        context.getCounter("total", "event-count").increment(1);
        context.getCounter("total", "request-time(ms)").increment(
                reader.getFetchTime());
    }

    private void validateKey(Message message) throws ChecksumException {
        long checksum = key.getChecksum();
        if (checksum != message.checksum()) {
            throw new ChecksumException("Invalid message checksum "
                    + message.checksum() + ". Expected " + key.getChecksum(),
                    key.getOffset());
        }
    }

    private boolean initReaderWithSplit() throws IOException, Exception {
        EtlRequest request = split.popRequest();
        if (request == null) {
            return false;
        }

        // will cause endTimeStamp to be reset
        //
        if (maxPullHours > 0) {
            endTimeStamp = 0;
        }

        key.set(request.getTopic(), request.getLeaderId(),
                request.getPartition(), request.getOffset(),
                request.getOffset(), 0);

        value = null;

        log.info("\n\ntopic:" + request.getTopic() + " partition:"
                + request.getPartition() + " beginOffset:"
                + request.getOffset() + " estimatedLastOffset:"
                + request.getLastOffset());

        if (statusMsgBuilder.length() > 0) {
            statusMsgBuilder.append("; ");
        }
        statusMsgBuilder.append(request.getTopic()).append(":")
                .append(request.getLeaderId()).append(":")
                .append(request.getPartition());
        context.setStatus(statusMsgBuilder.toString());

        closeReader();

        reader = new KafkaReader(context, request,
                CamusJob.getKafkaTimeoutValue(context),
                CamusJob.getKafkaBufferSize(context));

        decoder = MessageDecoderFactory.createMessageDecoder(context,
                request.getTopic());

        return true;
    }

    private void closeReader() throws IOException {
        if (reader != null) {
            try {
                readBytes += reader.getReadBytes();
                reader.close();
            } catch (Exception e) {
                // not much to do here but skip the task
            } finally {
                reader = null;
            }
        }
    }

    public static int getMaximumDecoderExceptionsToPrint(JobContext job) {
        return job.getConfiguration().getInt(PRINT_MAX_DECODER_EXCEPTIONS, 10);
    }
}
