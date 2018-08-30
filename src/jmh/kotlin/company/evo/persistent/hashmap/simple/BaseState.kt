package company.evo.persistent.hashmap.simple

import java.util.Random

import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

const val MAX_ENTRIES = 20_000_000
const val IDX_SEED = 1L
const val SEED = 2L

@State(Scope.Benchmark)
open class BaseState {
    companion object {
        val ixs: IntArray = Random(IDX_SEED)
                .ints(0, MAX_ENTRIES)
                .limit(1000)
                .toArray()
        val longsKeys: LongArray = Random(SEED)
                .longs(0, Int.MAX_VALUE.toLong())
                .limit(MAX_ENTRIES.toLong())
                .toArray()
        val intKeys: IntArray = Random(SEED)
                .ints(Int.MIN_VALUE, Int.MAX_VALUE)
                .limit(MAX_ENTRIES.toLong())
                .toArray()
        val doubleValues: DoubleArray = Random(SEED)
                .doubles()
                .limit(MAX_ENTRIES.toLong())
                .toArray()
        val shortValues: IntArray = Random(SEED)
                .ints(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt())
                .limit(MAX_ENTRIES.toLong())
                .toArray()
    }

    @Param("100000", "1000000", "10000000", "20000000")
    protected var entries: Int = 0
}
