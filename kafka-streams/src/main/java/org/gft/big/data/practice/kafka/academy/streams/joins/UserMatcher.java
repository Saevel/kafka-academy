package org.gft.big.data.practice.kafka.academy.streams.joins;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.gft.big.data.practice.kafka.academy.streams.Pair;

public class UserMatcher {

    public KStream<?, Pair<User, User>> matchUsers(KStream<?, User> left, KStream<?, User> right, long windowDuration){
        return null;
    }
}
