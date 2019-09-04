package org.gft.big.data.practice.kafka.academy.streams.joins;

import org.apache.kafka.streams.kstream.KStream;
import org.gft.big.data.practice.kafka.academy.model.User;

public class UserMatcher {

    public KStream<User, User> matchUsers(KStream<?, User> left, KStream<?, User> right, long windowDuration){
        return null;
    }
}
