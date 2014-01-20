package com.linkedin.camus.schemaregistry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

@RunWith(value = Parameterized.class)
public class TestSchemaRegistries {
	protected final SchemaRegistry registry;
	static Schema schema1 = Mockito.mock(Schema.class);
	static Schema schema2 = Mockito.mock(Schema.class);

	static {
	    when(schema1.toString()).thenReturn("schema1");
        when(schema2.toString()).thenReturn("schema2");
	}

	public TestSchemaRegistries(SchemaRegistry registry) {
		this.registry = registry;
	}

	public Schema getSchema1() {
		return schema1;
	}

	public Schema getSchema2() {
		return schema2;
	}

	@Test
	public void testSchemaRegistry() {
		try {
			registry.getLatestSchemaByTopic("test");
			fail("Should have failed with a SchemaNotFoundException.");
		} catch (SchemaNotFoundException e) {
			// expected
		}

		try {
			registry.getSchemaByID("test", "abc");
			fail("Should have failed with a SchemaNotFoundException.");
		} catch (SchemaNotFoundException e) {
			// expected
		}

		String id = registry.register("test", getSchema1());

		try {
			registry.getSchemaByID("test", "abc");
			fail("Should have failed with a SchemaNotFoundException.");
		} catch (SchemaNotFoundException e) {
			// expected
		}

		assertEquals(getSchema1(), registry.getSchemaByID("test", id));
		assertEquals(new SchemaDetails<Schema>("test", id, getSchema1()),
				registry.getLatestSchemaByTopic("test"));

		String secondId = registry.register("test", getSchema2());

		assertEquals(getSchema1(), registry.getSchemaByID("test", id));
		assertEquals(getSchema2(), registry.getSchemaByID("test", secondId));
		assertEquals(new SchemaDetails<Schema>("test", secondId, getSchema2()),
				registry.getLatestSchemaByTopic("test"));

		try {
			registry.getLatestSchemaByTopic("test-2");
			fail("Should have failed with a SchemaNotFoundException.");
		} catch (SchemaNotFoundException e) {
			// expected
		}

		try {
			registry.getSchemaByID("test-2", "");
			fail("Should have failed with a SchemaNotFoundException.");
		} catch (SchemaNotFoundException e) {
			// expected
		}

		secondId = registry.register("test", getSchema2());

		assertEquals(getSchema1(), registry.getSchemaByID("test", id));
		assertEquals(getSchema2(), registry.getSchemaByID("test", secondId));
		assertEquals(new SchemaDetails<Schema>("test", secondId, getSchema2()),
				registry.getLatestSchemaByTopic("test"));
	}

	@Parameters
	public static Collection<?> data() {
		File tmpDir = new File("/tmp/test-" + System.currentTimeMillis());
		SchemaRegistry fileRegistry = new FileSchemaRegistry(
				tmpDir, new MockSchemaSerializer());
		SchemaRegistry memoryRegistry = new MemorySchemaRegistry();
		Object[][] data = new Object[][] { { fileRegistry }, { memoryRegistry } };
		return Arrays.asList(data);
	}

	static class MockSchemaSerializer implements Serializer<Schema> {
        @Override
        public byte[] toBytes(Schema schema) {
            try {
                return schema.toString().getBytes("utf-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Schema fromBytes(byte[] bytes) {
            String value = new String(bytes, Charset.forName("utf-8"));
            if ("schema1".equals(value)) {
                return schema1;
            } else if ("schema2".equals(value)) {
                return schema2;
            } else {
                throw new RuntimeException("unhandled schema value: " + value);
            }
        }

	}
}
