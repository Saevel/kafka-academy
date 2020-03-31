package org.gft.big.data.practice.kafka.academy.streams.joins;

import org.apache.kafka.streams.kstream.KStream;
import org.gft.big.data.practice.kafka.academy.model.User;

/**
 * Implement the matchUsers method so that from the two streams,
 * it calculates all the pairs of users having identical surnames and returns them as a Pair<User, User>.
 * Once this is done, run the UserMatcherTest to verify the correctness of your implementation.
 */
public class UserMatcher {

    public KStream<User, User> matchUsers(KStream<?, User> left, KStream<?, User> right, long windowDuration){
        return null;
    }
}
