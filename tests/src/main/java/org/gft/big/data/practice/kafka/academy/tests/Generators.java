package org.gft.big.data.practice.kafka.academy.tests;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

public class Generators {

    private Generators(){
        ;
    }

    public static final Generator<Integer> intGenerator(){
        Random random = new Random();
        return () -> random.nextInt();
    }

    public static final Generator<Long> longGenerator(){
        Random random = new Random();
        return () -> random.nextLong();
    }

    public static final Generator<Double> doubleGenerator(){
        Random random = new Random();
        return () -> random.nextDouble();
    }

    public static final Generator<Boolean> booleanGenerator(){
        Random random = new Random();
        return () -> random.nextBoolean();
    }

    // TODO: Short generator

    // TODO: Byte generator

    // TODO: Char generator

    // TODO: String generators

    public static final<T> Generator<T> pure(T t){
        return () -> t;
    }

    public static final Generator<Integer> range(int min, int max){
        return intGenerator().map(i -> i + min).map(i -> i % max);
    }

    public static final Generator<Long> range(long min, long max){
        return longGenerator().map(i -> i + min).map(i -> i % max);
    }

    public static final<T> Generator<Stream<T>> streamOf(Generator<T> basicGenerator){
        return () -> Stream.generate(basicGenerator::sample);
    }

    public static final<T> Generator<Optional<T>> optional(Generator<T> basicGenerator){
        if(booleanGenerator().sample()){
           return basicGenerator.map(Optional::ofNullable);
        } else {
            return pure(Optional.empty());
        }
    }

    public static final<T> Generator<T> oneOf(List<T> options){
        if(options.isEmpty()){
            throw new IllegalArgumentException("Options cannot be empty");
        } else {
            return intGenerator().map(Math::abs).map(i -> i % options.size()).map(options::get);
        }

    }

    public static final<T> Generator<T> oneOf(T... options){
        if(options.length == 0){
            throw new IllegalArgumentException("Options cannot be empty");
        } else {
            return intGenerator().map(Math::abs).map(i -> i % options.length).map(i -> options[i]);
        }

    }
}
