package org.rhyssaldanha.projectreactor;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Utility class for {@link Tuple2} instances that contain
 * {@link Optional} values
 */
public abstract class OptionalTuple2 {
    private OptionalTuple2() {
    }

    public static <T1, T2> Tuple2<Optional<T1>, Optional<T2>> of(final T1 left, final T2 right) {
        return Tuples.of(Optional.of(left), Optional.of(right));
    }

    public static <T1> Tuple2<Optional<T1>, Optional<T1>> same(final T1 same) {
        return Tuples.of(Optional.of(same), Optional.of(same));
    }

    public static <T1, T2> Tuple2<Optional<T1>, Optional<T2>> right(final T2 right) {
        return Tuples.of(Optional.empty(), Optional.of(right));
    }

    public static <T1, T2> Tuple2<Optional<T1>, Optional<T2>> left(final T1 left) {
        return Tuples.of(Optional.of(left), Optional.empty());
    }

    public static <T1, T2> Predicate<Tuple2<Optional<T1>, Optional<T2>>> leftIsPresent() {
        return tuple -> tuple.getT1().isPresent();
    }

    public static <T1, T2> Predicate<Tuple2<Optional<T1>, Optional<T2>>> onlyLeftIsPresent() {
        return tuple -> tuple.getT1().isPresent() && !tuple.getT2().isPresent();
    }

    public static <T1, T2> T1 getLeft(final Tuple2<Optional<T1>, Optional<T2>> tuple2) {
        return tuple2.getT1().get();
    }

    public static <T1, T2> Predicate<Tuple2<Optional<T1>, Optional<T2>>> rightIsPresent() {
        return tuple -> tuple.getT2().isPresent();
    }

    public static <T1, T2> Predicate<Tuple2<Optional<T1>, Optional<T2>>> onlyRightIsPresent() {
        return tuple -> tuple.getT2().isPresent() && !tuple.getT1().isPresent();
    }

    public static <T1, T2> T2 getRight(final Tuple2<Optional<T1>, Optional<T2>> tuple2) {
        return tuple2.getT2().get();
    }
}
