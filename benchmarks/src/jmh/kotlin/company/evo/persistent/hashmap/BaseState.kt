package company.evo.persistent.hashmap

import org.openjdk.jmh.annotations.Level
import java.util.Random

import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State

const val MAX_KEY = 1_000_000_000
const val LIMIT = 1_000_000
const val SEED = 1L

@State(Scope.Benchmark)
abstract class BaseState {
    enum class LookupMode {
        SEQ,
        RND,
        MISSING_SEQ,
        MISSING_RND
    }

    @Param("random:1_000_000")
    // @Param("random:1_000_000", "random:10_000_000", "random:50_000_000")
    protected var dataSetSpec: String = ""

    lateinit var dataSet: HashMapBenchmarkDataSet

    @Param("SEQ", "RND", "MISSING_SEQ", "MISSING_RND")
    protected var lookupMode: LookupMode = LookupMode.RND

    @Setup(Level.Trial)
    fun setUpDataSet() {
        dataSet = HashMapBenchmarkDataSet.load(dataSetSpec, lookupMode)
    }

    abstract class HashMapBenchmarkDataSet(val numEntries: Int, lookupMode: LookupMode) {
        private val random = Random(SEED)

        val keys: IntArray = random
            .ints(0, MAX_KEY)
            .limit(numEntries.toLong())
            .toArray()
        val values: FloatArray = random
            .doubles()
            .limit(numEntries.toLong())
            .toArray()
            .map { it.toFloat() }
            .toFloatArray()

        val lookupKeys: IntArray = when (lookupMode) {
            LookupMode.RND -> {
                keys.copyOf()
                    .also { shuffleKeys(it) }
            }
            LookupMode.SEQ -> {
                keys.copyOf()
                    .also { it.sort() }
            }
            LookupMode.MISSING_RND -> {
                val uniqueKeys = keys.toHashSet()
                random
                    .ints(0, MAX_KEY)
                    .filter { k -> k !in uniqueKeys }
                    .limit(LIMIT.toLong())
                    .toArray()
            }
            LookupMode.MISSING_SEQ -> {
                val uniqueKeys = keys.toHashSet()
                random
                    .ints(0, MAX_KEY)
                    .filter { k -> k !in uniqueKeys }
                    .limit(LIMIT.toLong())
                    .toArray()
                    .also { it.sort() }
            }
        }

        companion object {
            fun load(spec: String, lookupMode: LookupMode): HashMapBenchmarkDataSet {
                val (dataSetType, dataSetSpec) = spec.split(":", limit = 2).let {
                    if (it.size < 2) {
                        throw IllegalArgumentException("Invalid data set string format")
                    }
                    it[0] to it[1]
                }
                return when (dataSetType) {
                    "random" -> {
                        val numEntries = Integer.parseInt(dataSetSpec.replace("_", ""))
                        RandomHashMapBenchmarkDataSet(dataSetSpec, lookupMode)
                    }
                    else -> throw IllegalArgumentException("Unknown data set type: $dataSetType")
                }
            }
        }

        private fun shuffleKeys(array: IntArray) {
            for (ix in array.indices) {
                val swapIx = random.nextInt(ix + 1)
                val v = array[ix]
                array[ix] =  array[swapIx]
                array[swapIx] = v
            }
        }

        class RandomHashMapBenchmarkDataSet(numEntries: Int, lookupMode: LookupMode) :
            HashMapBenchmarkDataSet(numEntries, lookupMode)
    }

    protected fun initMap(mapPut: (Int, Float) -> Unit) {
        dataSet.keys.withIndex().forEach { (ix, key) ->
            mapPut(key, dataSet.values[ix])
        }
    }
}
