package com.linkedin.camus.etl.kafka.persistence;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * Responsible for creating {@link JobRecordKeeper} instances.
 */
public class JobRecordKeeperFactory {

    /**
     * Create a record keeper implementation of the specified type using the
     * given configuration.
     *
     * @param configuration
     *            the configuration
     * @param type
     *            the instance type to create
     * @return a new instance
     */
    public <T extends JobRecordKeeper> T create(JobContext context,
            Class<T> type) {
        if (type == SequenceFileRecordKeeper.class) {
            return createSequenceFileRecordKeeper(context);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported job record keeper type: " + type);
        }
    }

    private <T extends JobRecordKeeper> T createSequenceFileRecordKeeper(
            JobContext context) {
        SequenceFileReaderWriterFactory factory = new SequenceFileReaderWriterFactory();
        @SuppressWarnings("unchecked")
        T t = (T) new SequenceFileRecordKeeper(context, factory);
        return t;
    }

}
