package com.linkedin.camus.schemaregistry;

public interface Serializer<O> {
	public byte[] toBytes(O obj);

	public O fromBytes(byte[] bytes);
}
