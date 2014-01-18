package com.linkedin.camus.etl.kafka.persistence;

import java.io.IOException;
import java.util.Map;

import kafka.common.TopicAndPartition;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

/**
 * A {@link JobRecordKeeper} that reads data from HDFS sequence files.
 */
public class SequenceFileRecordKeeper implements JobRecordKeeper {
    private static final Logger log = Logger
            .getLogger(SequenceFileRecordKeeper.class);

    private static class OffsetFileFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            return path.getName()
                    .startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
        }
    }

    private final JobContext context;
    private final SequenceFileReaderWriterFactory factory;

    public SequenceFileRecordKeeper(JobContext context,
            SequenceFileReaderWriterFactory factory) {
        this.context = context;
        this.factory = factory;
    }

    @Override
    public Map<TopicAndPartition, EtlKey> getLatestEtlOffsets() {
        final Map<TopicAndPartition, EtlKey> results = Maps.newHashMap();
        Path[] inputPaths = FileInputFormat.getInputPaths(context);

        for (Path inputPath : inputPaths) {
            log.debug("checking input path (" + inputPath
                    + ") for EtlKey sequence files containing offsets");

            try {
                final FileSystem fs = inputPath.getFileSystem(context
                        .getConfiguration());

                for (FileStatus f : fs.listStatus(inputPath,
                        new OffsetFileFilter())) {
                    log.info("previous offset file:" + f.getPath().toString());

                    try (SequenceFile.Reader reader = factory.createReader(
                            context.getConfiguration(), f.getPath())) {
                        final EtlKey key = new EtlKey();

                        // read all keys in the file, saving the ones with the
                        // highest
                        // offsets per topic/partition
                        while (reader.next(key, NullWritable.get())) {
                            TopicAndPartition top = new TopicAndPartition(
                                    key.getTopic(), key.getPartition());

                            EtlKey knownKey = results.get(top);
                            if (knownKey == null) {
                                results.put(top, new EtlKey(key));
                            } else {
                                // set the key with the greatest offset in the
                                // map
                                if (key.getOffset() > knownKey.getOffset()) {
                                    results.put(top, new EtlKey(key));
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Failed reading previous job etl keys", e);
                throw new RuntimeException(e);
            }
        }

        return results;
    }

    @Override
    public void recordFinalOffsets(Iterable<EtlKey> keys, Path outputPath) {
        try (SequenceFile.Writer offsetWriter = factory.createWriter(
                context.getConfiguration(), outputPath, EtlKey.class,
                NullWritable.class)) {
            for (EtlKey key : keys) {
                offsetWriter.append(key, NullWritable.get());
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to write final offsets (%s) to output path (%s)",
                    keys, outputPath), e);
        }
    }

    @Override
    public void recordPreviousKeys(Iterable<EtlKey> keys) {
        try {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path outputPath = FileOutputFormat.getOutputPath(context);

            if (!fs.exists(outputPath)) {
                fs.mkdirs(outputPath);
            }

            Path output = new Path(outputPath,
                    EtlMultiOutputFormat.OFFSET_PREFIX + "-previous");

            try (SequenceFile.Writer writer = factory.createWriter(
                    context.getConfiguration(), output, EtlKey.class,
                    NullWritable.class)) {
                for (EtlKey key : keys) {
                    writer.append(key, NullWritable.get());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed writing etl keys");
        }

    }

    @Override
    public void recordRequests(Iterable<EtlRequest> requests) {
        try {
            Path outputPath = FileOutputFormat.getOutputPath(context);
            FileSystem fs = FileSystem.get(context.getConfiguration());

            if (!fs.exists(outputPath)) {
                fs.mkdirs(outputPath);
            }

            Path requestsPath = new Path(outputPath,
                    EtlMultiOutputFormat.REQUESTS_FILE);

            try (SequenceFile.Writer writer = factory.createWriter(
                    context.getConfiguration(), requestsPath, EtlRequest.class,
                    NullWritable.class)) {
                for (EtlRequest r : requests) {
                    writer.append(r, NullWritable.get());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed writing etl requests.", e);
        }
    }

}
