package com.linkedin.camus.schemaregistry;

import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Schema;

import com.google.common.collect.Maps;

public class CachedSchemaRegistry implements SchemaRegistry {
	private final SchemaRegistry registry;
	private final ConcurrentMap<RegistryKey, Schema> cachedById = Maps.newConcurrentMap();
	private final ConcurrentMap<String, Schema> cachedLatest = Maps.newConcurrentMap();
	private final ConcurrentMap<TopicSchema, Integer> cachedBySchema = Maps.newConcurrentMap();

	@Override
    public void init(Properties props) {}

	public CachedSchemaRegistry(SchemaRegistry registry) {
		this.registry = registry;
	}

	@Override
    public Schema getSchema(RegistryKey key) {
        Schema schema = cachedById.get(key);
		if (schema == null) {
			schema = registry.getSchema(key);
			cachedById.putIfAbsent(key, schema);
		}
		return schema;
	}

    @Override
    public SchemaDetails getLatestSchema(String topicName) {
        Schema schema = cachedLatest.get(topicName);
		if (schema == null) {
			schema = registry.getLatestSchema(topicName).getSchema();
			cachedLatest.putIfAbsent(topicName, schema);
		}
		return registry.getLatestSchema(topicName);
	}

}
