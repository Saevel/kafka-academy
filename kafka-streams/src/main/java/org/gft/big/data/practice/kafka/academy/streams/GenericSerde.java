package org.gft.big.data.practice.kafka.academy.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Map;

public class GenericSerde<T> implements Serde<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        ;
    }

    @Override
    public void close() {
        ;
    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                ;
            }

            @Override
            public byte[] serialize(String topic, T data) {
                try {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
                    oos.writeObject(data);
                    oos.flush();
                    return outputStream.toByteArray();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException("Cannot serialize to Java bytes: " + data);
                }
            }

            @Override
            public void close() {
                ;
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                ;
            }

            @Override
            public T deserialize(String topic, byte[] data) {
                try {
                    ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data));
                    return (T) is.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException("Cannot deserialize data from byte array");
                }
            }

            @Override
            public void close() {
                ;
            }
        };
    }
}
