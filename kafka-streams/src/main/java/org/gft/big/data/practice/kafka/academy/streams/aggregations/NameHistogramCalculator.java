package org.gft.big.data.practice.kafka.academy.streams.aggregations;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.gft.big.data.practice.kafka.academy.model.User;

/**
 * Implement the calculateNameHistograms so that it groups all the Users by their names and for each name,
 * it calculates the total count of all the people having each of those names and returned a KTable
 * formed in the process.
 * Once this is done, run the NameHistogramCalculatorTest to verify the correctness of your implementation.
 */
public class NameHistogramCalculator {

    public KTable<String, Long> calculateNameHistograms(KStream<?, User> users){
        return users
                .groupBy((key, value) -> value.getName())
                .count();
    }
}
