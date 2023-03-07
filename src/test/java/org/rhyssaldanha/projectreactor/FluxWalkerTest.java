package org.rhyssaldanha.projectreactor;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class FluxWalkerTest {

    @Nested
    class BothContain {

        @Test
        void emptyFluxes() {
            StepVerifier.create(FluxWalker.<Integer>bothContain(Flux.empty(), Flux.empty()))
                    .verifyComplete();
        }

        @Test
        void allElementsMatch() {
            StepVerifier.create(FluxWalker.bothContain(Flux.just(1, 2, 3), Flux.just(1, 2, 3)))
                    .expectNext(1, 2, 3)
                    .verifyComplete();
        }
    }
}