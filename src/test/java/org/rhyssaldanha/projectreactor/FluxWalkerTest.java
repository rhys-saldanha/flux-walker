package org.rhyssaldanha.projectreactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class FluxWalkerTest {

    @Nested
    class ZipOptional {

        @DisplayName("should return an empty flux if both sources are empty")
        @Test
        void empty() {
            StepVerifier.create(FluxWalker.<Integer>zipOptional(Flux.empty(), Flux.empty()))
                    .verifyComplete();
        }

        @DisplayName("when all elements in both sources match")
        @Test
        void allElementsMatch() {
            final Flux<Integer> numbers = Flux.just(1, 2, 3);

            StepVerifier.create(FluxWalker.zipOptional(numbers, numbers))
                    .expectNext(OptionalTuple2.same(1))
                    .expectNext(OptionalTuple2.same(2))
                    .expectNext(OptionalTuple2.same(3))
                    .verifyComplete();
        }

        @DisplayName("when some elements are missing from both sources")
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

        @DisplayName("can filter to values that are contained in both sources")
        @Test
        void getBothContainSame() {
            final Flux<Integer> same = FluxWalker.zipOptional(Flux.just(1, 2, 3), Flux.just(3, 4, 5))
                    .transform(FluxWalker.containSame());

            StepVerifier.create(same)
                    .expectNext(3)
                    .verifyComplete();
        }

        @DisplayName("can filter to values that are uniquely in the left source")
        @Test
        void getUniqueLeft() {
            final Flux<Integer> uniqueLeft = FluxWalker.zipOptional(Flux.just(1, 2, 3), Flux.just(3, 4, 5))
                    .transform(FluxWalker.uniqueLeft());

            StepVerifier.create(uniqueLeft)
                    .expectNext(1, 2)
                    .verifyComplete();
        }

        @DisplayName("can filter to values that are uniquely in the right source")
        @Test
        void getUniqueRight() {
            final Flux<Integer> uniqueRight = FluxWalker.zipOptional(Flux.just(1, 2, 3), Flux.just(3, 4, 5))
                    .transform(FluxWalker.uniqueRight());

            StepVerifier.create(uniqueRight)
                    .expectNext(4, 5)
                    .verifyComplete();
        }
    }
}