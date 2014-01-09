package com.linkedin.camus.etl.kafka.common;

import static junit.framework.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class EtlKeyTest {

    @Test
    public void testShouldReadOldVersionOfEtlKey() throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        OldEtlKey oldKey = new OldEtlKey();
        EtlKey newKey = new EtlKey();
        oldKey.write(out);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());

        newKey.readFields(in);
        assertEquals("leaderId", newKey.getLeaderId());
        assertEquals(1, newKey.getPartition());
        assertEquals(2, newKey.getBeginOffset());
        assertEquals(3, newKey.getOffset());
        assertEquals(4, newKey.getChecksum());
        assertEquals("topic", newKey.getTopic());
        assertEquals(5, newKey.getTime());
        assertEquals("server", newKey.getServer());
        assertEquals("service", newKey.getService());
    }

    public static class OldEtlKey implements WritableComparable<OldEtlKey> {
        private final String leaderId = "leaderId";
        private final int partition = 1;
        private final long beginOffset = 2;
        private final long offset = 3;
        private final long checksum = 4;
        private final String topic = "topic";
        private final long time = 5;
        private final String server = "server";
        private final String service = "service";

        @Override
        public int compareTo(OldEtlKey o) {
            return 0;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, this.leaderId);
            out.writeInt(this.partition);
            out.writeLong(this.beginOffset);
            out.writeLong(this.offset);
            out.writeLong(this.checksum);
            out.writeUTF(this.topic);
            out.writeLong(this.time);
            out.writeUTF(this.server);
            out.writeUTF(this.service);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
        }
    }
}
