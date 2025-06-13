package dev.evo.persistent.hashmap

import java.nio.file.Paths
import java.util.Random
import java.util.zip.GZIPInputStream

import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State

const val MAX_KEY = 1_000_000_000
const val LOOKUP_LIMIT = 1_000_000
const val SEED = 1L

@State(Scope.Benchmark)
abstract class BaseState {
    enum class LookupMode {
        SEQ,
        RND,
        MISSING_SEQ,
        MISSING_RND
    }

    @Param(
        "random:1_000_000",
        "random:10_000_000"
        // "random:50_000_000"
        // "file:benchmark_data.txt.gz;limit=2_000_000"
        // "file:benchmark_data.txt.gz;limit=10_000_000"
        // "file:benchmark_data.txt.gz;limit=20_000_000"
        // "file:benchmark_data.txt.gz"
    )
    protected var dataSetSpec: String = ""

    lateinit var dataSet: HashMapBenchmarkDataSet

    abstract var lookupMode: LookupMode

    @Setup(Level.Trial)
    fun setUpDataSet() {
        dataSet = HashMapBenchmarkDataSet.load(dataSetSpec, lookupMode)
    }

    class HashMapBenchmarkDataSet(val keys: IntArray, val values: FloatArray, lookupMode: LookupMode) {
        val lookupKeys: IntArray = when (lookupMode) {
            LookupMode.RND -> {
                keys.copyOf()
                    .also(::shuffleKeys)
                    .copyOfRange(0, LOOKUP_LIMIT)
            }
            LookupMode.SEQ -> {
                keys.copyOf()
                    .also(::shuffleKeys)
                    .copyOfRange(0, LOOKUP_LIMIT)
                    .also(IntArray::sort)
            }
            LookupMode.MISSING_RND -> {
                val uniqueKeys = keys.toHashSet()
                random
                    .ints(0, MAX_KEY)
                    .filter { k -> k !in uniqueKeys }
                    .limit(LOOKUP_LIMIT.toLong())
                    .toArray()
            }
            LookupMode.MISSING_SEQ -> {
                val uniqueKeys = keys.toHashSet()
                random
                    .ints(0, MAX_KEY)
                    .filter { k -> k !in uniqueKeys }
                    .limit(LOOKUP_LIMIT.toLong())
                    .toArray()
                    .also { it.sort() }
            }
        }

        companion object {
            private val random = Random(SEED)

            fun load(spec: String, lookupMode: LookupMode): HashMapBenchmarkDataSet {
                val (dataSetType, dataSetSpec) = spec.split(":", limit = 2).let {
                    if (it.size < 2) {
                        throw IllegalArgumentException("Invalid data set string format")
                    }
                    it[0] to it[1]
                }
                return when (dataSetType) {
                    "random" -> {
                        loadRandom(dataSetSpec, lookupMode)
                    }
                    "file" -> {
                        loadFile(dataSetSpec, lookupMode)
                    }
                    else -> throw IllegalArgumentException("Unknown data set type: $dataSetType")
                }
            }

            private fun loadRandom(spec: String, lookupMode: LookupMode): HashMapBenchmarkDataSet {
                val options = Options.parse(spec, listOf(OptionType.INT))
                val numEntries = options.args[0] as Int
                val keys = HashSet<Int>(numEntries)
                val keysStream = random.ints().iterator()
                while (keys.size < numEntries) {
                    keys.add(keysStream.nextInt())
                }
                val values = random
                    .doubles()
                    .limit(numEntries.toLong())
                    .toArray()
                    .map { it.toFloat() }
                    .toFloatArray()
                return HashMapBenchmarkDataSet(keys.toIntArray(), values, lookupMode)
            }

            private fun loadFile(spec: String, lookupMode: LookupMode): HashMapBenchmarkDataSet {
                val options = Options.parse(
                    spec, listOf(OptionType.STRING), mapOf("limit" to OptionType.INT)
                )

                val projectDir = Paths.get(System.getProperty("project.dir"))
                val fileName = options.args[0] as String
                val filePath = projectDir.resolve(fileName)
                val fileReader = if (filePath.fileName.toString().endsWith(".gz")) {
                    GZIPInputStream(filePath.toFile().inputStream()).bufferedReader()
                } else {
                    filePath.toFile().bufferedReader()
                }

                var numEntries: Int? = options.kwargs["limit"] as? Int
                val header = fileReader.readLine().trim(' ', '#')
                for (h in header.split(' ')) {
                    val (k, v) = h.split('=', limit = 2)
                    if (k == "rows" && numEntries == null) {
                        numEntries = Integer.parseInt(v)
                        break
                    }
                }

                checkNotNull(numEntries)
                val keys = IntArray(numEntries)
                val values = FloatArray(numEntries)
                var i = 0
                for (line in fileReader.lineSequence()) {
                    if (line.isBlank()) {
                        continue
                    }
                    val (k, v) = line.split('=', limit = 2)
                    keys[i] = Integer.parseInt(k)
                    values[i] = java.lang.Float.parseFloat(v)
                    i++
                    if (i == numEntries) {
                        break
                    }
                }
                return HashMapBenchmarkDataSet(keys, values, lookupMode)
            }
        }

        class Options(val args: List<*>, val kwargs: Map<String, *>) {
            companion object {
                fun parse(
                    spec: String,
                    argsTypes: List<OptionType>? = null,
                    kwargsTypes: Map<String, OptionType>? = null
                ): Options {
                    val specParts = spec.split(';', limit = 2)
                    val argsSpec = specParts.getOrNull(0)
                    val kwSpec = specParts.getOrNull(1)

                    val args = mutableListOf<Any>()
                    if (argsSpec != null) {
                        for ((ix, v) in argsSpec.split(',').withIndex()) {
                            val type = argsTypes?.getOrNull(ix)
                            args.add(type?.convert(v) ?: v)
                        }
                    }

                    val kwargs = mutableMapOf<String, Any>()
                    if (kwSpec != null) {
                        for (opt in kwSpec.split(',')) {
                            val (k, v) = opt.split('=', limit = 2)
                            kwargs[k] = kwargsTypes?.get(k)?.convert(v) ?: v
                        }
                    }

                    return Options(args, kwargs)
                }
            }
        }

        enum class OptionType {
            INT, STRING;

            fun convert(v: String): Any {
                return when (this) {
                    INT -> Integer.parseInt(v.replace("_", ""))
                    STRING -> v
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
    }

    protected fun initMap(mapPut: (Int, Float) -> Unit) {
        dataSet.keys.withIndex().forEach { (ix, key) ->
            mapPut(key, dataSet.values[ix])
        }
    }
}
