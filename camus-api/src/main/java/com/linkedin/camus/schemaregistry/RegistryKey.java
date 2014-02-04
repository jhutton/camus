package com.linkedin.camus.schemaregistry;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;

/**
 * A key to be used to look up schemas in a registry.
 */
public class RegistryKey {

    /**
     * Get a key for the specified topic and id.
     *
     * @param topic the topic
     * @param id the id
     * @return a registry key
     */
    public static RegistryKey get(String topic, Integer id) {
        return new RegistryKey(topic, id);
    }

    private final Integer id;
    private final String topic;

    private RegistryKey(String topic, Integer id) {
        checkNotNull(topic, "Null topic");
        checkNotNull(id, "Null id");
        this.topic = topic;
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        // null checked in constructor
        result = prime * result + id.hashCode();
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
        RegistryKey other = (RegistryKey) obj;
        // null checked in constructor
        return id.equals(other.id)
                && topic.equals(other.topic);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("topic", topic)
                .add("id", id)
                .toString();
    }

}
