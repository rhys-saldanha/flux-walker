package org.rhyssaldanha.projectreactor;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static reactor.core.Fuseable.ASYNC;
import static reactor.core.Fuseable.SYNC;

/**
 * @apiNote Heavily inspired by {@link reactor.core.publisher.FluxZip}
 */
public final class FluxWalker<T extends Comparable<T>> extends Flux<Tuple2<Optional<T>, Optional<T>>> implements Scannable {

    final Publisher<T> leftPublisher, rightPublisher;

    final Comparator<T> comparator;

    final Supplier<? extends Queue<T>> queueSupplier;

    final int prefetch;

    FluxWalker(final Publisher<T> leftPublisher,
               final Publisher<T> rightPublisher,
               final Comparator<T> comparator,
               final Supplier<? extends Queue<T>> queueSupplier,
               final int prefetch) {
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        this.leftPublisher = Objects.requireNonNull(leftPublisher, "leftPublisher");
        this.rightPublisher = Objects.requireNonNull(rightPublisher, "rightPublisher");
        this.comparator = comparator;
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.prefetch = prefetch;
    }

    public static <T extends Comparable<T>> Flux<T> bothContain(final Publisher<T> left,
                                                                final Publisher<T> right) {
        //noinspection OptionalGetWithoutIsPresent
        return new FluxWalker<>(left, right,
                Comparator.naturalOrder(),
                Queues.xs(),
                Queues.XS_BUFFER_SIZE)
                .filter(tuple -> tuple.getT1().isPresent() && tuple.getT2().isPresent())
                .map(tuple -> tuple.getT1().get());
    }

    public static <T extends Comparable<T>> Flux<Tuple2<Optional<T>, Optional<T>>> zipOptional(final Publisher<T> left,
                                                                                               final Publisher<T> right) {
        return new FluxWalker<>(left, right,
                Comparator.naturalOrder(),
                Queues.xs(),
                Queues.XS_BUFFER_SIZE);
    }

    @Override
    public int getPrefetch() {
        return prefetch;
    }

    @Override
    public void subscribe(final CoreSubscriber<? super Tuple2<Optional<T>, Optional<T>>> actual) {
        try {
            final WalkCoordinator<T> coordinator = new WalkCoordinator<>(actual, comparator, queueSupplier, prefetch);

            actual.onSubscribe(coordinator);

            coordinator.subscribe(leftPublisher, rightPublisher);
        } catch (final Throwable e) {
            Operators.reportThrowInSubscribe(actual, e);
        }
    }

    @Override
    public Object scanUnsafe(final Attr key) {
        if (key == Attr.PREFETCH) return prefetch;
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
        return null;
    }

    @Override
    public String stepName() {
        return "source(" + getClass().getSimpleName() + ")";
    }

    static final class WalkCoordinator<T> implements Scannable, Subscription {

        final CoreSubscriber<? super Tuple2<Optional<T>, Optional<T>>> actual;

        final WalkInner<T> leftSubscriber, rightSubscriber;

        final Comparator<T> comparator;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<WalkCoordinator> WIP =
                AtomicIntegerFieldUpdater.newUpdater(WalkCoordinator.class, "wip");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<WalkCoordinator> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(WalkCoordinator.class, "requested");

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<WalkCoordinator, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(WalkCoordinator.class,
                        Throwable.class,
                        "error");

        volatile boolean cancelled;

        T leftCurrent, rightCurrent;

        FluxWalkerStrategy<T> strategyCurrent;

        public WalkCoordinator(final CoreSubscriber<? super Tuple2<Optional<T>, Optional<T>>> actual,
                               final Comparator<T> comparator,
                               final Supplier<? extends Queue<T>> queueSupplier,
                               final int prefetch) {
            this.actual = actual;
            this.comparator = comparator;
            this.leftSubscriber = new WalkInner<>(this, prefetch, queueSupplier);
            this.rightSubscriber = new WalkInner<>(this, prefetch, queueSupplier);
            this.strategyCurrent = new FluxWalkerStrategy.WalkBoth<>(comparator, null, null);
        }

        void subscribe(final Publisher<T> left, final Publisher<T> right) {
            if (cancelled || error != null) {
                return;
            }
            try {
                left.subscribe(leftSubscriber);
            } catch (final Throwable e) {
                Operators.reportThrowInSubscribe(leftSubscriber, e);
            }
            try {
                right.subscribe(rightSubscriber);
            } catch (final Throwable e) {
                Operators.reportThrowInSubscribe(leftSubscriber, e);
            }
        }

        @Override
        public void request(final long n) {
            if (Operators.validate(n)) {
                Operators.addCap(REQUESTED, this, n);
                drain(null, null);
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;

                cancelAll();

                if (WIP.getAndIncrement(this) == 0) {
                    discardAll(1);
                }
            }
        }

        public CoreSubscriber<? super Tuple2<Optional<T>, Optional<T>>> actual() {
            return actual;
        }

        @Override
        public Stream<? extends Scannable> inners() {
            return Stream.of(leftSubscriber, rightSubscriber);
        }

        @Override
        @Nullable
        public Object scanUnsafe(final Attr key) {
            if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
            if (key == Attr.ERROR) return error;
            if (key == Attr.CANCELLED) return cancelled;
            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

            if (key == Attr.ACTUAL) {
                return actual();
            }
            return null;
        }

        void error(final Throwable e) {
            if (Exceptions.addThrowable(ERROR, this, e)) {
                drain(null, null);
            } else {
                Operators.onErrorDropped(e, actual.currentContext());
            }
        }

        void cancelAll() {
            leftSubscriber.cancel();
            rightSubscriber.cancel();
        }

        void discardAll(int m) {
            final Context context = actual.currentContext();
            final T leftCurrent = this.leftCurrent;
            final T rightCurrent = this.rightCurrent;
            Operators.onDiscardMultiple(Arrays.asList(leftCurrent, rightCurrent), context);

            for (; ; ) {
                for (final WalkInner<T> s : Arrays.asList(leftSubscriber, rightSubscriber)) {
                    final Queue<T> queue = s.queue;
                    final int sourceMode = s.sourceMode;

                    if (queue != null) {
                        if (sourceMode == ASYNC) {
                            // delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
                            queue.clear();
                        } else {
                            Operators.onDiscardQueueWithClear(queue, context, null);
                        }
                    }
                }

                final int missed = wip;
                if (m == missed) {
                    if (WIP.compareAndSet(this, m, Integer.MIN_VALUE)) {
                        return;
                    } else {
                        m = wip;
                    }
                } else {
                    m = missed;
                }
            }
        }

        void drain(@Nullable final WalkInner<T> callerInner, @Nullable final Object dataSignal) {
            final int previousWork = addWork(this);
            if (previousWork != 0) {
                if (callerInner != null) {
                    if (callerInner.sourceMode == ASYNC && previousWork == Integer.MIN_VALUE) {
                        callerInner.queue.clear();
                    } else if (dataSignal != null && cancelled) {
                        // discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
                        Operators.onDiscard(dataSignal, actual.currentContext());
                    }
                }
                return;
            }

            final CoreSubscriber<? super Tuple2<Optional<T>, Optional<T>>> a = actual;
            final WalkInner<T> leftSubscriber = this.leftSubscriber;
            final WalkInner<T> rightSubscriber = this.rightSubscriber;

            int missed = 1;

            do {
                final long requested = this.requested;
                long count = 0L;

                while (requested != count) {

                    if (cancelled) {
                        discardAll(missed);
                        return;
                    }

                    if (error != null) {
                        cancelAll();
                        discardAll(missed);

                        final Throwable ex = Exceptions.terminate(ERROR, this);

                        a.onError(ex);

                        return;
                    }

                    if (isComplete(leftSubscriber) && isComplete(rightSubscriber)) {
                        cancelAll();
                        discardAll(missed);

                        a.onComplete();
                        return;
                    }

                    if (leftCurrent == null) {
                        try {
                            final Queue<T> queue = leftSubscriber.queue;

                            leftCurrent = queue != null ? queue.poll() : null;
                        } catch (final Throwable operatorError) {
                            Throwable mappedError = Operators.onOperatorError(operatorError, actual.currentContext());

                            cancelAll();
                            discardAll(missed);

                            Exceptions.addThrowable(ERROR, this, mappedError);
                            //noinspection ConstantConditions
                            mappedError = Exceptions.terminate(ERROR, this);

                            a.onError(mappedError);

                            return;
                        }
                    }

                    if (rightCurrent == null) {
                        try {
                            final Queue<T> queue = rightSubscriber.queue;

                            rightCurrent = queue != null ? queue.poll() : null;
                        } catch (final Throwable operatorError) {
                            Throwable mappedError = Operators.onOperatorError(operatorError, actual.currentContext());

                            cancelAll();
                            discardAll(missed);

                            Exceptions.addThrowable(ERROR, this, mappedError);
                            //noinspection ConstantConditions
                            mappedError = Exceptions.terminate(ERROR, this);

                            a.onError(mappedError);

                            return;
                        }
                    }

                    if (leftCurrent == null && !isComplete(leftSubscriber)) {
                        break;
                    }

                    if (rightCurrent == null && !isComplete(rightSubscriber)) {
                        break;
                    }

                    try {
                        strategyCurrent = strategyCurrent.nextStrategy(leftCurrent, rightCurrent);
                        leftCurrent = strategyCurrent.getLeft();
                        rightCurrent = strategyCurrent.getRight();

                        a.onNext(strategyCurrent.getOptionalTuple2());

                        count++;

                    } catch (final Throwable operatorError) {
                        Throwable mappedError = Operators.onOperatorError(null, operatorError,
                                new Object[]{leftCurrent, rightCurrent}, actual.currentContext());
                        cancelAll();
                        discardAll(missed);

                        Exceptions.addThrowable(ERROR, this, mappedError);
                        //noinspection ConstantConditions
                        mappedError = Exceptions.terminate(ERROR, this);

                        a.onError(mappedError);

                        return;
                    }
                }

                if (requested == count) {
                    if (cancelled) {
                        return;
                    }

                    if (error != null) {
                        cancelAll();
                        discardAll(missed);

                        final Throwable ex = Exceptions.terminate(ERROR, this);

                        a.onError(ex);

                        return;
                    }

                    if (leftCurrent == null) {
                        try {
                            final boolean done = leftSubscriber.done;
                            final Queue<T> queue = leftSubscriber.queue;

                            final T value = queue != null ? queue.poll() : null;

                            final boolean sourceEmpty = value == null;
                            if (done && sourceEmpty) {
                                cancelAll();
                                discardAll(missed);

                                a.onComplete();
                                return;
                            }
                            if (!sourceEmpty) {
                                leftCurrent = value;
                            }
                        } catch (final Throwable operatorError) {
                            Throwable mappedError = Operators.onOperatorError(operatorError, actual.currentContext());

                            cancelAll();
                            discardAll(missed);

                            Exceptions.addThrowable(ERROR, this, mappedError);
                            //noinspection ConstantConditions
                            mappedError = Exceptions.terminate(ERROR, this);

                            a.onError(mappedError);

                            return;
                        }
                    }

                    if (rightCurrent == null) {
                        try {
                            final boolean done = rightSubscriber.done;
                            final Queue<T> queue = rightSubscriber.queue;

                            final T value = queue != null ? queue.poll() : null;

                            final boolean sourceEmpty = value == null;
                            if (done && sourceEmpty) {
                                cancelAll();
                                discardAll(missed);

                                a.onComplete();
                                return;
                            }
                            if (!sourceEmpty) {
                                rightCurrent = value;
                            }
                        } catch (final Throwable operatorError) {
                            Throwable mappedError = Operators.onOperatorError(operatorError, actual.currentContext());

                            cancelAll();
                            discardAll(missed);

                            Exceptions.addThrowable(ERROR, this, mappedError);
                            //noinspection ConstantConditions
                            mappedError = Exceptions.terminate(ERROR, this);

                            a.onError(mappedError);

                            return;
                        }
                    }
                }

                if (count != 0L) {
                    leftSubscriber.request(count);
                    rightSubscriber.request(count);

                    if (requested != Long.MAX_VALUE) {
                        REQUESTED.addAndGet(this, -count);
                    }
                }

                missed = WIP.addAndGet(this, -missed);
            } while (missed != 0);
        }

        private static <T> boolean isComplete(final WalkInner<T> subscriber) {
            return subscriber.done && isQueueEmpty(subscriber);
        }

        private static <T> boolean isQueueEmpty(final WalkInner<T> subscriber) {
            return subscriber.queue != null && subscriber.queue.isEmpty();
        }

        static int addWork(final WalkCoordinator<?> instance) {
            for (; ; ) {
                final int state = instance.wip;

                if (state == Integer.MIN_VALUE) {
                    return Integer.MIN_VALUE;
                }

                if (WIP.compareAndSet(instance, state, state + 1)) {
                    return state;
                }
            }
        }
    }

    /**
     * @apiNote Heavily influenced by {@link reactor.core.publisher.FluxZip.ZipInner}
     */
    static final class WalkInner<T> implements CoreSubscriber<T>, Scannable {

        final WalkCoordinator<T> parent;

        final int prefetch;

        final int limit;

        final Supplier<? extends Queue<T>> queueSupplier;

        volatile Queue<T> queue;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<WalkInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(WalkInner.class,
                        Subscription.class,
                        "s");

        long produced;

        volatile boolean done;

        int sourceMode;

        WalkInner(final WalkCoordinator<T> parent,
                  final int prefetch,
                  final Supplier<? extends Queue<T>> queueSupplier) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.queueSupplier = queueSupplier;
            this.limit = unboundedOrLimit(prefetch);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onSubscribe(final Subscription s) {
            if (Operators.setOnce(S, this, s)) {
                if (s instanceof Fuseable.QueueSubscription) {
                    final Fuseable.QueueSubscription<T> f = (Fuseable.QueueSubscription<T>) s;

                    final int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

                    if (m == SYNC) {
                        sourceMode = SYNC;
                        queue = f;
                        done = true;
                        parent.drain(this, null);
                        return;
                    } else if (m == ASYNC) {
                        sourceMode = ASYNC;
                        queue = f;
                    } else {
                        queue = queueSupplier.get();
                    }
                } else {
                    queue = queueSupplier.get();
                }
                s.request(unboundedOrPrefetch(prefetch));
                parent.drain(this, null);
            }
        }

        @Override
        public void onNext(final T t) {
            if (sourceMode != ASYNC) {
                if (!queue.offer(t)) {
                    Operators.onDiscard(t, currentContext());
                    onError(Operators.onOperatorError(s, Exceptions.failWithOverflow
                            (Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), currentContext()));
                    return;
                }
            }
            parent.drain(this, t);
        }

        @Override
        public Context currentContext() {
            return parent.actual.currentContext();
        }

        @Override
        public void onError(final Throwable t) {
            if (done) {
                Operators.onErrorDropped(t, currentContext());
                return;
            }
            done = true;
            parent.error(t);
        }

        @Override
        public void onComplete() {
            done = true;
            parent.drain(this, null);
        }

        @Override
        @Nullable
        public Object scanUnsafe(final Attr key) {
            if (key == Attr.PARENT) return s;
            if (key == Attr.ACTUAL) return parent;
            if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
            if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
            if (key == Attr.TERMINATED) return done && s != Operators.cancelledSubscription();
            if (key == Attr.PREFETCH) return prefetch;
            if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

            return null;
        }

        void cancel() {
            Operators.terminate(S, this);
        }

        void request(final long n) {
            if (sourceMode != SYNC) {
                final long p = produced + n;
                if (p >= limit) {
                    produced = 0L;
                    s.request(p);
                } else {
                    produced = p;
                }
            }
        }
    }

    /**
     * @apiNote Taken from {@link Operators#unboundedOrLimit(int)}
     */
    static int unboundedOrLimit(final int prefetch) {
        return prefetch == Integer.MAX_VALUE ? Integer.MAX_VALUE : (prefetch - (prefetch >> 2));
    }

    /**
     * @apiNote Taken from {@link Operators#unboundedOrPrefetch(int)}
     */
    static long unboundedOrPrefetch(final int prefetch) {
        return prefetch == Integer.MAX_VALUE ? Long.MAX_VALUE : prefetch;
    }
}
