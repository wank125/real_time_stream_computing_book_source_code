package com.alain898.book.realtimestreaming.common.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class WriterPartitioner implements Partitioner {

    /**
     * constructor, avoid java.lang.NoSuchMethodException...(kafka.utils.VerifiableProperties)
     */
//    public WriterPartitioner(VerifiableProperties verifiableProperties) {
//    }
//
//
    public int partition(Object key, int numPartitions) {
        if (key == null) {
            throw new NullPointerException("key is not null, key value: " + key);
        }
        return Math.abs(hash(key.toString())) % numPartitions;
    }


    private int hash(String k) {
        int h = 0;
        h ^= k.hashCode();
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
