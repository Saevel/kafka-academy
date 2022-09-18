package org.gft.big.data.practice.kafka.academy.streams.joins;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.gft.big.data.practice.kafka.academy.model.User;

/**
 * Implement the matchUsers method so that from the two streams,
 * it calculates all the pairs of users having identical surnames and returns them as a Pair<User, User>.
 * Once this is done, run the UserMatcherTest to verify the correctness of your implementation.
 */
public class UserMatcher {

    public KStream<User, User> matchUsers(KStream<?, User> left, KStream<?, User> right, long windowDuration){
        KStream<String, User> regroupedLeft = left.selectKey((key, value) -> value.getSurname());
        KStream<String, User> regroupedRight = right.selectKey((key, value) -> value.getSurname());

        return regroupedLeft.join(
                        regroupedRight,
                        (leftItem, rightItem) -> new KeyValue<>(leftItem, rightItem),
                        JoinWindows.of(windowDuration))
                .map((anyKey, pair) -> pair);
    }
}
