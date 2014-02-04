package com.linkedin.camus.schemaregistry;

import java.util.Properties;

import org.apache.avro.Schema;

/**
 * The schema registry is used to read and write schemas for Kafka topics. This
 * is useful because it means you no longer have to store your schema with your
 * message payload. Instead, you can store a schema id with the message, and use
 * a schema registry to look up the message's schema when you wish to decode it.
 * In essence, a schema registry is just a client-side interface for a versioned
 * key-value store that's meant to store schemas.
 */
public interface SchemaRegistry {

    /**
     * Initializer for SchemaRegistry;
     *
     * @param props
     *            Java properties
     */
    public void init(Properties props);

    /**
     * Get a schema for a given registry key regardless of whether the schema
     * was the last one written for this topic.
     *
     * @param the
     *            registry key
     * @return schema
     * @throws SchemaNotFoundException
     *             if no schema is found
     */
    public Schema getSchema(RegistryKey key);

    /**
     * Get the last schema that was written for a specific topic.
     *
     * @param topic
     *            A topic name.
     * @return the schema details
     * @throws SchemaNotFoundException
     *             if no schema is found
     */
    public SchemaDetails getLatestSchema(String topic);
}