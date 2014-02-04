package com.linkedin.camus.schemaregistry;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.avro.Schema;

public class SchemaDetails {
	private final String topic;
	private final Integer id;
	private final Schema schema;

	public SchemaDetails(String topic, Integer id, Schema schema) {
	    checkNotNull(topic, "Null topic");
	    checkNotNull(id, "Null id");
	    checkNotNull(schema, "Null schema");
		this.topic = topic;
		this.id = id;
		this.schema = schema;
	}

	/**
	 * Get the schema
	 */
	public Schema getSchema() {
		return schema;
	}

	/**
	 * Get the topic
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * @return the id
	 */
	public Integer getId() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		// these are null checked in the constructor
		result = prime * result + id.hashCode();
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
		SchemaDetails other = (SchemaDetails) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SchemaDetails [topic=" + topic + ", id=" + id + ", schema="
				+ schema + "]";
	}
}