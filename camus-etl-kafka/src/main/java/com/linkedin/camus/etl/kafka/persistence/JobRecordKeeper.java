package com.linkedin.camus.etl.kafka.persistence;

import java.util.Map;

import org.apache.hadoop.fs.Path;

import kafka.common.TopicAndPartition;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;

/**
 * Represents a class that can read and write job meta-data records.
 */
public interface JobRecordKeeper {

    Map<TopicAndPartition, EtlKey> getLatestEtlOffsets();

    void recordFinalOffsets(Iterable<EtlKey> keys, Path outputPath);

    void recordPreviousKeys(Iterable<EtlKey> keys);

    void recordRequests(Iterable<EtlRequest> request);

}
