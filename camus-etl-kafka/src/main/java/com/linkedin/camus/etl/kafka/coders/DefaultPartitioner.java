package com.linkedin.camus.etl.kafka.coders;

import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class DefaultPartitioner implements Partitioner {
    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";
    protected final DateTimeFormatter outputDateFormatter;

    public DefaultPartitioner(JobContext context) {
        outputDateFormatter = DateUtils.getDateTimeFormatter(
                OUTPUT_DATE_FORMAT, DateTimeZone.forID(EtlMultiOutputFormat
                        .getDefaultTimeZone(context)));
    }

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        long outfilePartitionMs = EtlMultiOutputFormat
                .getEtlOutputFileTimePartitionMins(context) * 60000L;
        return "" + DateUtils.getPartition(outfilePartitionMs, key.getTimestamp());
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic,
            int brokerId, int partitionId, String encodedPartition) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append(
                "/");
        DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
        sb.append(bucket.toString(OUTPUT_DATE_FORMAT));
        return sb.toString();
    }
}
