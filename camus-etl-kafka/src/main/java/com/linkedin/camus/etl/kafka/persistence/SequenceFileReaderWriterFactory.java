package com.linkedin.camus.etl.kafka.persistence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

/**
 * Factory for creating sequence file readers and writers.
 */
public class SequenceFileReaderWriterFactory {

    /**
     * Create a new {@link SequenceFile.Reader} instance using the specified
     * configuration and path.
     *
     * @param configuration
     *            the configuration
     * @param path
     *            the path
     * @return a new reader
     * @throws IOException
     *             if a network exception occurs
     */
    public SequenceFile.Reader createReader(Configuration configuration,
            Path path) throws IOException {
        return new SequenceFile.Reader(configuration,
                SequenceFile.Reader.file(path));
    }

    /**
     * Create a new {@link SequenceFile.Writer} instance using the specified
     * configuration and path.
     *
     * @param configuration
     *            the configuration
     * @param path
     *            the path
     * @return a new reader
     * @throws IOException
     *             if a network exception occurs
     */
    public SequenceFile.Writer createWriter(Configuration configuration,
            Path path, Class<?> keyClass, Class<?> valueClass)
            throws IOException {
        return SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(keyClass),
                SequenceFile.Writer.valueClass(valueClass));
    }
}
