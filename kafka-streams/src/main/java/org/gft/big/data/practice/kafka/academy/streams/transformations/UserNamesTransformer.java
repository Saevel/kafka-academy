package org.gft.big.data.practice.kafka.academy.streams.transformations;

import org.apache.kafka.streams.kstream.KStream;
import org.gft.big.data.practice.kafka.academy.model.User;

/**
 * filters out all users whose name does not begin with an „A”
 * filters out all users below the age of 18
 * transforms each User to his name and surname, separated by a space
 * Once this is done, run the UserNamesTransformerTest to verify the correctness of your implementation.
 */
public class UserNamesTransformer {

    public KStream<?, String> transform(KStream<?, User> usersStream){
        return null;
    }
}
