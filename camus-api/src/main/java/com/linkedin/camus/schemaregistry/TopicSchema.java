package com.linkedin.camus.schemaregistry;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.avro.Schema;

import com.google.common.base.Objects;

/**
 * Represents a topic and schema association.
 */
public class TopicSchema {

    /**
     * Get a topic schema for the specified topic and schema object.
     *
     * @param topic
     *            the topic
     * @param schema
     *            the schema
     * @return a topic schema
     */
    public static TopicSchema get(String topic, Schema schema) {
        return new TopicSchema(topic, schema);
    }

    private final String topic;
    private final Schema schema;

    private TopicSchema(String topic, Schema schema) {
        checkNotNull(topic, "Null topic");
        checkNotNull(topic, "Null schema");
        this.topic = topic;
        this.schema = schema;
    }

    public String getTopic() {
        return topic;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        // null checked in constructor
        result = prime * result + schema.hashCode();
        result = prime * result + topic.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicSchema other = (TopicSchema) obj;
        // null checked in constructor
        return topic.equals(other.topic)
                && schema.equals(other.schema);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("topic", topic)
                .add("schema", schema).toString();
    }
}
