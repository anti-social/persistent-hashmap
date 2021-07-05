package company.evo.persistent.hashmap

import io.kotest.core.spec.style.StringSpec
import io.kotest.property.Arb
import io.kotest.property.RandomSource
import io.kotest.property.Sample
import io.kotest.property.arbitrary.int
import io.kotest.property.forAll

class HasherTests : StringSpec() {
    init {
        "knuth32 covers all buckets" {
            forAll(100, primes, Arb.int()) { capacity, v ->
                val hash = Knuth32.hash(v)
                var ix = hash % capacity
                val probedIxs = BooleanArray(capacity)
                for (probe in 0 until capacity * 2) {
                    ix = Knuth32.probe(probe + 1, ix, hash, capacity)
                    probedIxs[ix] = true
                }

                probedIxs.all { it }
            }
        }
    }

    companion object {
        private val MAX_PRIME = 100_663_319
        private val MAX_PRIME_IX = PRIMES.binarySearch(MAX_PRIME)

        private val primes = object : Arb<Int>() {
            override fun edgecase(rs: RandomSource): Int? = null

            override fun sample(rs: RandomSource): Sample<Int> {

                return Sample(PRIMES[rs.random.nextInt(MAX_PRIME_IX)])
            }
        }
    }
}