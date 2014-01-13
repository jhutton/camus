package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
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

    public long getCurrentOffset() {
        return currentOffset;
    }

    /**
     * Fetches the next Kafka message and returns it to the caller.
     *
     * @return a message or null
     * @throws IOException
     */
    public Message getNext() throws IOException {
        if (!hasNext()) {
            return null;
        }

        MessageAndOffset msgAndOffset = messageIter.next();
        Message message = msgAndOffset.message();

        currentOffset = msgAndOffset.offset() + 1; // increase offset
        currentCount++; // increase count
        return message;
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
                        if (log.isDebugEnabled()) {
                            log.debug("Skipped offsets till : " + message.offset());
                        }
                        break;
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("Number of offsets to be skipped: " + skipped);
                }

                messageIter = messageBuffer.iterator();

                while (skipped != 0) {
                    MessageAndOffset skippedMessage = messageIter.next();
                    if (log.isDebugEnabled()) {
                        log.debug("Skipping offset : " + skippedMessage.offset());
                    }
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
     * @return the topic this reader is associated with
     */
    public String getTopic() {
        return kafkaRequest.getTopic();
    }

    /**
     * @return the leader id this reader is associated with
     */
    public String getLeaderId() {
        return kafkaRequest.getLeaderId();
    }

    /**
     * @return the partition id this reader is reading from
     */
    public int getPartition() {
        return kafkaRequest.getPartition();
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
