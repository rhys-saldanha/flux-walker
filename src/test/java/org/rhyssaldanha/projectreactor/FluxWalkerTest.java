package org.rhyssaldanha.projectreactor;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class FluxWalkerTest {

    @Nested
    class BothContain {

        @Test
        void empty() {
            StepVerifier.create(FluxWalker.<Integer>bothContain(Flux.empty(), Flux.empty()))
                    .verifyComplete();
        }

        @Test
        void allElementsMatch() {
            final Flux<Integer> numbers = Flux.just(1, 2, 3);

            StepVerifier.create(FluxWalker.bothContain(numbers, numbers))
                    .expectNext(1, 2, 3)
                    .verifyComplete();
        }

        @Test
        void oneMatchingElement() {
            StepVerifier.create(FluxWalker.bothContain(Flux.just(1, 2, 3), Flux.just(3, 4, 5)))
                    .expectNext(3)
                    .verifyComplete();
        }
    }

    @Nested
    class ZipOptional {

        @Test
        void empty() {
            StepVerifier.create(FluxWalker.<Integer>zipOptional(Flux.empty(), Flux.empty()))
                    .verifyComplete();
        }

        @Test
        void allElementsMatch() {
            final Flux<Integer> numbers = Flux.just(1, 2, 3);

            StepVerifier.create(FluxWalker.zipOptional(numbers, numbers))
                    .expectNext(OptionalTuple2.same(1))
                    .expectNext(OptionalTuple2.same(2))
                    .expectNext(OptionalTuple2.same(3))
                    .verifyComplete();
        }

        @Test
        void missingElements() {
            StepVerifier.create(FluxWalker.zipOptional(Flux.just(1, 2, 3), Flux.just(3, 4, 5)))
                    .expectNext(OptionalTuple2.left(1))
                    .expectNext(OptionalTuple2.left(2))
                    .expectNext(OptionalTuple2.same(3))
                    .expectNext(OptionalTuple2.right(4))
                    .expectNext(OptionalTuple2.right(5))
                    .verifyComplete();
        }
    }

    @Nested
    class FilterZipOptional {

        @Test
        void getOriginalLeft() {
            final Flux<Integer> actualLeft = FluxWalker.zipOptional(Flux.just(1, 2, 3), Flux.just(3, 4, 5))
                    .filter(OptionalTuple2.leftIsPresent())
                    .map(OptionalTuple2::getLeft);

            StepVerifier.create(actualLeft)
                    .expectNext(1, 2, 3)
                    .verifyComplete();
        }

        @Test
        void getOriginalRight() {
            final Flux<Integer> actualRight = FluxWalker.zipOptional(Flux.just(1, 2, 3), Flux.just(3, 4, 5))
                    .filter(OptionalTuple2.rightIsPresent())
                    .map(OptionalTuple2::getRight);

            StepVerifier.create(actualRight)
                    .expectNext(3, 4, 5)
                    .verifyComplete();
        }
    }
}