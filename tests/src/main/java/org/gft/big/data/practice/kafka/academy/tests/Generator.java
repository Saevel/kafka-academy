package org.gft.big.data.practice.kafka.academy.tests;

import java.util.function.Function;

public interface Generator<X> {

    public X sample();

    default<Y> Generator<Y> map(Function<X, Y> f) {
        return () -> f.apply(this.sample());
    }

    default<Y> Generator<Y> flatMap(Function<X, Generator<Y>> f){
        return () -> f.apply(this.sample()).sample();
    }
}
