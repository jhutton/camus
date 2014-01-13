package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import kafka.message.Message;

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

public class EtlRecordReader<T> extends RecordReader<EtlKey, CamusWrapper<T>> {
    private static Logger log = Logger.getLogger(EtlRecordReader.class);
    private static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";
    private static final String DEFAULT_SERVER = "server";
    private static final String DEFAULT_SERVICE = "service";

    private Mapper<EtlKey, Writable, EtlKey, Writable>.Context context;

    private final EtlKey key = new EtlKey();
    private final CamusWrapper<T> value = new CamusWrapper<>();

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
    private MessageDecoder<T> decoder;
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
    public CamusWrapper<T> getCurrentValue() throws IOException,
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

                Message message;

                while ((message = reader.getNext()) != null) {
                    updateCountersAndProgress(message.size());

                    if (!decodeMessage(message)) {
                        // try the next one
                        continue;
                    }

                    final long timeStamp = value.getTimestamp();
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

    private boolean decodeMessage(Message message) throws IOException,
            InterruptedException {
        final long decodeStart = System.currentTimeMillis();
        final byte[] valueBytes = getValueBytes(message);

        boolean decoded;
        try {
            decoder.decode(valueBytes, value);

            key.setOffset(reader.getCurrentOffset());
            key.setChecksum(message.checksum());
            key.setTimestamp(value.getTimestamp());
            key.setPartition(value.getPartitionMap());
            if (ignoreServerServiceList.contains(key.getTopic())
                    || ignoreServerServiceList.contains("all")) {
                key.setServer(DEFAULT_SERVER);
                key.setService(DEFAULT_SERVICE);
            }
            decoded = true;
        } catch (Exception e) {
            maybePrintException(e);
            context.write(key, new ExceptionWritable(e));
            decoded = false;
        }

        long decodeTime = System.currentTimeMillis() - decodeStart;
        context.getCounter("total", "decode-time(ms)").increment(decodeTime);
        return decoded;
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

    /**
     * Get the value bytes from the specified message.
     *
     * @param message
     *            the message
     * @return the bytes making up the value
     */
    private byte[] getValueBytes(Message message) {
        ByteBuffer messagePayload = message.payload();
        int payloadSize = messagePayload.remaining();
        byte[] payloadData = new byte[payloadSize];
        messagePayload.get(payloadData, messagePayload.position(), payloadSize);
        return payloadData;
    }

    private void setStartStatus(final long timeStamp) {
        DateTime time = new DateTime(timeStamp);
        statusMsgBuilder.append(" begin read at ").append(time.toString());
        context.setStatus(statusMsgBuilder.toString());
        log.info(key.getTopic() + " begin read at " + time.toString());
        endTimeStamp = (time.plusHours(this.maxPullHours)).getMillis();
    }

    private void updateCountersAndProgress(int dataRead) {
        context.progress();
        context.getCounter("total", "data-read").increment(dataRead);
        context.getCounter("total", "event-count").increment(1);
        context.getCounter("total", "request-time(ms)").increment(
                reader.getFetchTime());
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

        key.clear();
        value.clear();

        key.setTopic(request.getTopic());
        key.setLeaderId(request.getLeaderId());
        key.setPartition(request.getPartition());
        key.setBeginOffset(request.getOffset());

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

        @SuppressWarnings("unchecked")
        MessageDecoder<T> d = (MessageDecoder<T>) MessageDecoderFactory
                .createMessageDecoder(context, request.getTopic());
        decoder = d;

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
