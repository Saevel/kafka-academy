package org.gft.big.data.practice.kafka.academy.streams.aggregations;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.gft.big.data.practice.kafka.academy.model.User;

public class NameHistogramCalculator {

    public KTable<String, Long> calculateNameHistograms(KStream<?, User> users){
        return null;
    }
}
