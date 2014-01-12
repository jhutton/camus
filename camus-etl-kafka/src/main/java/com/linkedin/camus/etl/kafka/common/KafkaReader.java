package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.api.PartitionFetchInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.CamusJob;

/**
 * Poorly named class that handles kafka pull events within each
 * KafkaRecordReader.
 *
 * @author Richard Park
 */
public class KafkaReader {
    // index of context
    private static Logger log = Logger.getLogger(KafkaReader.class);
    private final EtlRequest kafkaRequest;
    private final SimpleConsumer simpleConsumer;

    private final long beginOffset;
    private long currentOffset;

    private final long lastOffset;

    private final TaskAttemptContext context;

    private Iterator<MessageAndOffset> messageIter;

    private long currentCount = 0;
    private long totalFetchTime = 0;
    private long lastFetchTime = 0;

    private final int fetchBufferSize;

    /**
     * Construct using the json representation of the kafka request
     */
    public KafkaReader(TaskAttemptContext context, EtlRequest request,
            int clientTimeout, int fetchBufferSize) throws Exception {
        this.fetchBufferSize = fetchBufferSize;
        this.context = context;

        log.info("bufferSize=" + fetchBufferSize);
        log.info("timeout=" + clientTimeout);

        // Create the kafka request from the json

        kafkaRequest = request;

        beginOffset = request.getOffset();
        currentOffset = request.getOffset();
        lastOffset = request.getLastOffset();

        // read data from queue

        URI uri = kafkaRequest.getURI();
        simpleConsumer = new SimpleConsumer(uri.getHost(), uri.getPort(),
                CamusJob.getKafkaTimeoutValue(context),
                CamusJob.getKafkaBufferSize(context),
                CamusJob.getKafkaClientName(context));

        log.info("Connected to leader " + uri + " beginning reading at offset "
                + beginOffset + " latest offset=" + lastOffset);
        fetch();
    }

    public boolean hasNext() throws IOException {
        if (messageIter != null && messageIter.hasNext()) {
            return true;
        } else {
            return fetch();
        }
    }

    /**
     * Fetches the next Kafka message and stuffs the results into the specified message key
     * and value.
     *
     * @param key the etl key
     * @param msgValue the message value
     * @param msgKey the message key
     * @return true if a value was retrieved and set else false
     * @throws IOException
     */
    public boolean getNext(EtlKey key, BytesWritable msgValue, BytesWritable msgKey)
            throws IOException {
        if (hasNext()) {
            MessageAndOffset msgAndOffset = messageIter.next();
            Message message = msgAndOffset.message();

            setMsgKey(msgKey, message);
            setMsgValue(msgValue, message);

            key.clear();
            key.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(),
                    kafkaRequest.getPartition(), currentOffset,
                    msgAndOffset.offset() + 1, message.checksum());

            currentOffset = msgAndOffset.offset() + 1; // increase offset
            currentCount++; // increase count
            return true;
        } else {
            return false;
        }
    }

    /**
     * Extract the key from the specified message and populate the specified {@code msgKey} reference
     * with the data.
     *
     * @param msgKey the message key to populate
     * @param message the message
     */
    private void setMsgKey(BytesWritable msgKey, Message message) {
        ByteBuffer messageKey = message.key();
        if (messageKey != null) {
            int keySize = messageKey.remaining();
            byte[] keyData = new byte[keySize];
            messageKey.get(keyData, messageKey.position(), keySize);
            msgKey.set(keyData, 0, keySize);
        }
    }

    /**
     * Extract the payload from the specified message and populate the specified {@code msgValue} reference
     * with the data.
     *
     * @param msgValue the message value to populate.
     * @param message the message
     */
    private void setMsgValue(BytesWritable msgValue, Message message) {
        ByteBuffer messagePayload = message.payload();
        int payloadSize = messagePayload.remaining();
        byte[] payloadData = new byte[payloadSize];
        messagePayload.get(payloadData, messagePayload.position(), payloadSize);
        msgValue.set(payloadData, 0, payloadSize);
    }

    /**
     * Creates a fetch request.
     *
     * @return false if there's no more fetches
     * @throws IOException
     */

    public boolean fetch() throws IOException {
        // fetch up until the last offset is reached
        //
        if (currentOffset >= lastOffset) {
            return false;
        }

        long tempTime = System.currentTimeMillis();
        TopicAndPartition topicAndPartition = new TopicAndPartition(
                kafkaRequest.getTopic(), kafkaRequest.getPartition());

        log.debug("\nAsking for offset : " + (currentOffset));
        PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(
                currentOffset, fetchBufferSize);

        Map<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
        fetchInfo.put(topicAndPartition, partitionFetchInfo);

        FetchRequest fetchRequest = new FetchRequest(
                CamusJob.getKafkaFetchRequestCorrelationId(context),
                CamusJob.getKafkaClientName(context),
                CamusJob.getKafkaFetchRequestMaxWait(context),
                CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);

        try {
            final FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
            if (fetchResponse.hasError()) {
                log.info("Error encountered during a fetch request from Kafka");
                log.info("Error Code generated : "
                        + fetchResponse.errorCode(kafkaRequest.getTopic(),
                                kafkaRequest.getPartition()));
                return false;
            } else {
                ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(
                        kafkaRequest.getTopic(), kafkaRequest.getPartition());
                lastFetchTime = (System.currentTimeMillis() - tempTime);
                log.debug("Time taken to fetch : " + (lastFetchTime / 1000)
                        + " seconds");
                log.debug("The size of the ByteBufferMessageSet returned is : "
                        + messageBuffer.sizeInBytes());

                int skipped = 0;
                totalFetchTime += lastFetchTime;

                for (MessageAndOffset message : messageBuffer) {
                    if (message.offset() < currentOffset) {
                        // flag = true;
                        skipped++;
                    } else {
                        log.debug("Skipped offsets till : " + message.offset());
                        break;
                    }
                }

                log.debug("Number of offsets to be skipped: " + skipped);
                messageIter = messageBuffer.iterator();

                while (skipped != 0) {
                    MessageAndOffset skippedMessage = messageIter.next();
                    log.debug("Skipping offset : " + skippedMessage.offset());
                    skipped--;
                }

                if (!messageIter.hasNext()) {
                    System.out
                            .println("No more data left to process. Returning false");
                    messageIter = null;
                    return false;
                }

                return true;
            }
        } catch (Exception e) {
            log.info("Exception generated during fetch");
            e.printStackTrace();
            return false;
        }

    }

    /**
     * Closes this context
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (simpleConsumer != null) {
            simpleConsumer.close();
        }
    }

    /**
     * Returns the total bytes that will be fetched. This is calculated by
     * taking the diffs of the offsets
     *
     * @return
     */
    public long getTotalBytes() {
        return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
    }

    /**
     * Returns the total bytes that have been fetched so far
     *
     * @return
     */
    public long getReadBytes() {
        return currentOffset - beginOffset;
    }

    /**
     * Returns the number of events that have been read r
     *
     * @return
     */
    public long getCount() {
        return currentCount;
    }

    /**
     * Returns the fetch time of the last fetch in ms
     *
     * @return
     */
    public long getFetchTime() {
        return lastFetchTime;
    }

    /**
     * Returns the totalFetchTime in ms
     *
     * @return
     */
    public long getTotalFetchTime() {
        return totalFetchTime;
    }
}
