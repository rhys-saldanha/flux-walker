package org.rhyssaldanha.projectreactor;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Optional;

/**
 * Utility class for {@link Tuple2} instances that contain {@link Optional} values
 */
abstract class OptionalTuple2 {
    private OptionalTuple2() {
    }

    static <T1, T2> Tuple2<Optional<T1>, Optional<T2>> of(final T1 left, final T2 right) {
        return Tuples.of(Optional.of(left), Optional.of(right));
    }

    static <T1> Tuple2<Optional<T1>, Optional<T1>> same(final T1 same) {
        return Tuples.of(Optional.of(same), Optional.of(same));
    }

    static <T1, T2> Tuple2<Optional<T1>, Optional<T2>> right(final T2 right) {
        return Tuples.of(Optional.empty(), Optional.of(right));
    }

    static <T1, T2> Tuple2<Optional<T1>, Optional<T2>> left(final T1 left) {
        return Tuples.of(Optional.of(left), Optional.empty());
    }

    static <T1, T2> boolean bothArePresent(final Tuple2<Optional<T1>, Optional<T2>> tuple2) {
        return tuple2.getT1().isPresent() && tuple2.getT2().isPresent();
    }

    static <T1, T2> boolean onlyLeftIsPresent(final Tuple2<Optional<T1>, Optional<T2>> tuple2) {
        return tuple2.getT1().isPresent() && !tuple2.getT2().isPresent();
    }

    static <T1, T2> boolean onlyRightIsPresent(final Tuple2<Optional<T1>, Optional<T2>> tuple2) {
        return tuple2.getT2().isPresent() && !tuple2.getT1().isPresent();
    }

    static <T1, T2> T1 getLeft(final Tuple2<Optional<T1>, Optional<T2>> tuple2) {
        return tuple2.getT1().get();
    }

    static <T1, T2> T2 getRight(final Tuple2<Optional<T1>, Optional<T2>> tuple2) {
        return tuple2.getT2().get();
    }
}
