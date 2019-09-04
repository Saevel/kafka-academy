package org.gft.big.data.practice.kafka.academy.streams;

import java.io.Serializable;

public class Pair<K, V> implements Serializable {

    private K key;

    private V value;

    public Pair(){
        ;
    }

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public String toString(){
        return "(" + key.toString() + ", " + value.toString() + ")";
    }
}
