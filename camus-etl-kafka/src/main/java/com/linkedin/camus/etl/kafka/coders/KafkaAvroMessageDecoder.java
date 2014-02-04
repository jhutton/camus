package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import com.google.common.collect.Maps;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.RegistryKey;
import com.linkedin.camus.schemaregistry.SchemaRegistry;

public class KafkaAvroMessageDecoder extends MessageDecoder<Record> {
    public static final String KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";

    protected DecoderFactory decoderFactory;
    protected SchemaRegistry registry;
    private Schema latestSchema;
    private final ConcurrentMap<Integer, RegistryKey> keyMap = Maps
            .newConcurrentMap();

    @Override
    public void init(Properties props, String topicName) {
        super.init(props, topicName);
        try {
            SchemaRegistry registry = (SchemaRegistry) Class
                    .forName(
                            props.getProperty(KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS))
                    .newInstance();
            registry.init(props);

            this.registry = new CachedSchemaRegistry(registry);
            this.latestSchema = registry.getLatestSchema(topicName).getSchema();
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        decoderFactory = DecoderFactory.get();
    }

    @Override
    public boolean decode(byte[] payload, CamusWrapper<Record> wrapper) {
        BinaryDecoder decoder = decoderFactory.binaryDecoder(payload, null);

        try {
            Integer id = decoder.readInt();
            RegistryKey key = keyMap.get(id);
            if (key == null) {
                key = RegistryKey.get(topicName, id);
                keyMap.put(id, key);
            }

            Schema schema = registry.getSchema(key);
            DatumReader<Record> reader = new GenericDatumReader<Record>(
                    latestSchema, schema);
            wrapper.set(reader.read(null, decoder));
            return true;
        } catch (IOException e) {
            throw new MessageDecoderException(e);
        }
    }
}
