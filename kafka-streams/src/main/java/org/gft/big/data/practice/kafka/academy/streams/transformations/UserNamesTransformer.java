package org.gft.big.data.practice.kafka.academy.streams.transformations;

import org.apache.kafka.streams.kstream.KStream;
import org.gft.big.data.practice.kafka.academy.model.User;

public class UserNamesTransformer {

    public KStream<?, String> transform(KStream<?, User> usersStream){
        return usersStream
                .filter((any, user) -> user.getName().startsWith("A") && user.getAge() >= 18)
                .mapValues(user -> user.getName() + " " + user.getSurname());
    }
}
