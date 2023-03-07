package org.rhyssaldanha.projectreactor;

import reactor.util.function.Tuple2;

import java.util.Comparator;
import java.util.Optional;

abstract class FluxWalkerStrategy<T> {
    protected final Comparator<T> comparator;
    protected final T leftCurrent;
    protected final T rightCurrent;

    FluxWalkerStrategy(final Comparator<T> comparator,
                       final T leftCurrent,
                       final T rightCurrent) {
        this.comparator = comparator;
        this.leftCurrent = leftCurrent;
        this.rightCurrent = rightCurrent;
    }

    abstract T getLeft();

    abstract T getRight();

    abstract Tuple2<Optional<T>, Optional<T>> getOptionalTuple2();

    FluxWalkerStrategy<T> nextStrategy(final T leftNext, final T rightNext) {
        if (leftNext == null) {
            return new WalkRight<>(comparator, null, rightNext);
        }
        if (rightNext == null) {
            return new WalkLeft<>(comparator, leftNext, null);
        }

        final int comparison = comparator.compare(leftNext, rightNext);

        if (comparison == 0) {
            return new WalkBoth<>(comparator, leftNext, rightNext);
        } else if (comparison < 0) {
            return new WalkLeft<>(comparator, leftNext, rightNext);
        } else {
            return new WalkRight<>(comparator, leftNext, rightNext);
        }
    }

    static class WalkBoth<T> extends FluxWalkerStrategy<T> {

        WalkBoth(final Comparator<T> comparator,
                 final T leftCurrent,
                 final T rightCurrent) {
            super(comparator, leftCurrent, rightCurrent);
        }

        @Override
        public T getLeft() {
            return null;
        }

        @Override
        public T getRight() {
            return null;
        }

        @Override
        public Tuple2<Optional<T>, Optional<T>> getOptionalTuple2() {
            return OptionalTuple2.of(leftCurrent, rightCurrent);
        }
    }

    static class WalkLeft<T> extends FluxWalkerStrategy<T> {

        WalkLeft(final Comparator<T> comparator,
                 final T leftCurrent,
                 final T rightCurrent) {
            super(comparator, leftCurrent, rightCurrent);
        }

        @Override
        public T getLeft() {
            return null;
        }

        @Override
        public T getRight() {
            return rightCurrent;
        }

        @Override
        public Tuple2<Optional<T>, Optional<T>> getOptionalTuple2() {
            return OptionalTuple2.left(leftCurrent);
        }
    }

    static class WalkRight<T> extends FluxWalkerStrategy<T> {

        WalkRight(final Comparator<T> comparator,
                  final T leftCurrent,
                  final T rightCurrent) {
            super(comparator, leftCurrent, rightCurrent);
        }

        @Override
        public T getLeft() {
            return leftCurrent;
        }

        @Override
        public T getRight() {
            return null;
        }

        @Override
        public Tuple2<Optional<T>, Optional<T>> getOptionalTuple2() {
            return OptionalTuple2.right(rightCurrent);
        }
    }
}
