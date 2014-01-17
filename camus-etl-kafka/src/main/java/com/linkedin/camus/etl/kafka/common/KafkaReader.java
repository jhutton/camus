package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.util.Iterator;

import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

import com.google.common.collect.AbstractIterator;

/**
 * Poorly named class that handles kafka pull events within each
 * KafkaRecordReader.
 *
 * @author Richard Park
 */
public class KafkaReader extends AbstractIterator<Message> {
    private static final Logger log = Logger.getLogger(KafkaReader.class);

    private final KafkaBrokerClient brokerClient;
    private final EtlRequest request;

    // initial offset this reader starts at
    private final long initialOffset;
    // the current offset from where the next fetch will start
    private long currentOffset;
    // the limit for this reader. When we reach this offset in a returned
    // message set, we close the reader
    private final long limitOffset;

    private long currentCount = 0;
    private final long totalFetchTime = 0;
    private final long lastFetchTime = 0;

    private Iterator<MessageAndOffset> delegate;

    public KafkaReader(KafkaBrokerClient brokerClient, EtlRequest request)
            throws Exception {
        this.brokerClient = brokerClient;
        this.request = request;

        // initialize offsets
        initialOffset = currentOffset = request.getOffset();
        limitOffset = request.getLatestOffset();

        log.info("Created KafkaReader with broker client: " + brokerClient
                + "\n initialOffset: " + initialOffset + "\n limitOffset: "
                + limitOffset);
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    protected Message computeNext() {
        if (delegate == null) {
            delegate = fetch();

            if (delegate == null) {
                return endOfData();
            }
        }

        while (delegate.hasNext()) {
            MessageAndOffset message = delegate.next();

            if (message.offset() < currentOffset) {
                log.error(String
                        .format("Skipping message (%s) which has an offset less than current offset (%s)",
                                message, currentOffset));
            } else {
                currentOffset = message.offset() + 1; // increase offset
                currentCount++; // increase count
                return message.message();
            }
        }

        delegate = null;
        return computeNext();
    }

    /**
     * Creates a fetch request.
     *
     * @return false if there's no more fetches
     * @throws IOException
     */

    private Iterator<MessageAndOffset> fetch() {
        // fetch up until the last offset is reached
        //
        if (currentOffset >= limitOffset) {
            return null;
        }

        try {
            final FetchResponse fetchResponse = brokerClient.fetch(
                    request.getTopic(), request.getPartition(), currentOffset);

            if (fetchResponse.hasError()) {
                short errorCode = fetchResponse.errorCode(request.getTopic(),
                        request.getPartition());
                log.error(String.format(
                        "Failed fetching data from topic (%s),"
                                + " partition (%d), at offset (%d) using broker client (%s):"
                                + "\n Error code: %d", request.getTopic(),
                        request.getPartition(), currentOffset, brokerClient,
                        errorCode));
                return null;
            } else {
                ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(
                        request.getTopic(), request.getPartition());

                if (log.isDebugEnabled()) {
                    log.debug("Fetch took completed after "
                            + brokerClient.getLastFetchTimeMillis()
                            + "ms. ByteBufferMessageSet returned is "
                            + messageBuffer.sizeInBytes() + " bytes");
                }

                Iterator<MessageAndOffset> iterator = messageBuffer.iterator();
                if (iterator.hasNext()) {
                    return iterator;
                } else {
                    return null;
                }
            }
        } catch (Exception e) {
            log.error("Exception generated during fetch", e);
            return null;
        }
    }

    public void close() {
        try {
            brokerClient.close();
        } catch (Exception e) {
            log.error("Failed closing broker client", e);
        }
    }

    /**
     * Returns the total bytes that will be fetched. This is calculated by
     * taking the diffs of the offsets
     *
     * @return
     */
    public long getTotalBytes() {
        return (limitOffset > initialOffset) ? limitOffset - initialOffset : 0;
    }

    /**
     * Returns the total bytes that have been fetched so far
     *
     * @return
     */
    public long getReadBytes() {
        return currentOffset - initialOffset;
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
        return request.getTopic();
    }

    /**
     * @return the leader id this reader is associated with
     */
    public String getLeaderId() {
        return request.getLeaderId();
    }

    /**
     * @return the partition id this reader is reading from
     */
    public int getPartition() {
        return request.getPartition();
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
