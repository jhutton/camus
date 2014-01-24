package com.linkedin.camus.schemaregistry;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Properties;

import org.apache.avro.Schema;

/**
 * Not thread safe.
 */
public class FileSchemaRegistry implements SchemaRegistry {
    private final File root;
    private final Serializer<Schema> serializer;

    @Override
    public void init(Properties props) {
    }

    public FileSchemaRegistry(File root, Serializer<Schema> serializer) {
        this.root = root;
        this.serializer = serializer;
    }

    @Override
    public String register(String topic, Schema schema) {

        File topicDir = getTopicPath(topic);

        if (!topicDir.exists()) {
            topicDir.mkdirs();
        }

        byte[] bytes = serializer.toBytes(schema);
        final String id;
        try {
            id = SHAsum(bytes);
        } catch (NoSuchAlgorithmException e1) {
            throw new RuntimeException(e1);
        }
        File file = getSchemaPath(topic, id, true);

        try (FileOutputStream out = new FileOutputStream(file)) {
            out.write(bytes);

            // move any old "latest" files to be regular schema files
            for (File fileToRename : topicDir.listFiles()) {
                if (!fileToRename.equals(file)
                        && fileToRename.getName().endsWith(".latest")) {
                    String oldName = fileToRename.getName();
                    // 7 = len(.latest)
                    File renameTo = new File(fileToRename.getParentFile(),
                            oldName.substring(0, oldName.length() - 7));
                    fileToRename.renameTo(renameTo);
                }
            }
            return id;
        } catch (Exception e) {
            throw new SchemaRegistryException(e);
        }
    }

    @Override
    public Schema getSchemaByID(String topic, String id) {
        File file = getSchemaPath(topic, id, false);

        if (!file.exists()) {
            file = getSchemaPath(topic, id, true);
        }

        if (!file.exists()) {
            throw new SchemaNotFoundException(
                    "No matching schema found for topic " + topic + " and id "
                            + id + ".");
        }

        return serializer.fromBytes(readBytes(file));
    }

    @Override
    public SchemaDetails getLatestSchemaByTopic(
            String topicName) {
        File topicDir = getTopicPath(topicName);
        if (topicDir.exists()) {
            for (File file : topicDir.listFiles()) {
                if (file.getName().endsWith(".latest")) {
                    String id = file.getName().replace(".schema.latest", "");
                    return new SchemaDetails(topicName, id,
                            serializer.fromBytes(readBytes(file)));
                }
            }
        }
        throw new SchemaNotFoundException(
                "Unable to find a latest schema for topic " + topicName + ".");
    }

    private byte[] readBytes(File file) {
        byte[] bytes = new byte[(int) file.length()];

        try (DataInputStream dis = new DataInputStream(
                new FileInputStream(file))) {
            dis.readFully(bytes);
        } catch (Exception e) {
            throw new SchemaRegistryException(e);
        }
        return bytes;
    }

    private File getTopicPath(String topic) {
        return new File(root, topic);
    }

    private File getSchemaPath(String topic, String id, boolean latest) {
        return new File(getTopicPath(topic), id + ".schema"
                + ((latest) ? ".latest" : ""));
    }

    public static String SHAsum(byte[] convertme)
            throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        return byteArray2Hex(md.digest(convertme));
    }

    private static String byteArray2Hex(final byte[] hash) {
        try (Formatter formatter = new Formatter()) {
            for (byte b : hash) {
                formatter.format("%02x", b);
            }
            String hex = formatter.toString();
            formatter.close();
            return hex;
        }
    }
}
