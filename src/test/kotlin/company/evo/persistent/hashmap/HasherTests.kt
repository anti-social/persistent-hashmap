package company.evo.persistent.hashmap

import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll
import io.kotlintest.specs.StringSpec

import java.util.Random

class HasherTests : StringSpec() {
    init {
        "knuth32 covers all buckets" {
            forAll(1_000_000, primes, Gen.int()) { capacity, v ->
                val hash = Knuth32.hash(v)
                var ix = hash % capacity
                val ixs = mutableSetOf<Int>()
                for (probe in 0 until capacity * 10) {
                    ix = Knuth32.probe(probe + 1, ix, hash, capacity)
                    ixs.add(ix)
                    if (ixs.size == capacity) {
                        break
                    }
                }

                ixs.size == capacity
            }
        }
    }

    companion object {
        private val RANDOM = Random()
        private const val FIRST_N_PRIMES = 10

        private val primes = object : Gen<Int> {
            override fun constants(): Iterable<Int> {
                return PRIMES.asIterable().take(FIRST_N_PRIMES)
            }

            override fun random(): Sequence<Int> = generateSequence {
                PRIMES[RANDOM.nextInt(FIRST_N_PRIMES)]
            }
        }
    }
}