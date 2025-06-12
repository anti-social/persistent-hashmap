package dev.evo.persistent.hashmap

import dev.evo.persistent.VersionedMmapDirectory
import dev.evo.persistent.hashmap.straight.StraightHashMapEnv
import dev.evo.persistent.hashmap.straight.StraightHashMapRO
import dev.evo.persistent.hashmap.straight.StraightHashMapROEnv
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Int_Short
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Int_Int
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Int_Long
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Int_Double
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Int_Float
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Long_Short
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Long_Int
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Long_Long
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Long_Double
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Long_Float

import java.nio.file.Paths

fun main(args: Array<String>) {
    val hashmapDir = Paths.get(args[0])
    val envBuilder = VersionedMmapDirectory.openReadOnly(hashmapDir, StraightHashMapEnv.VERSION_FILENAME).use { dir ->
        val mapHeader = StraightHashMapROEnv.readMapHeader(dir)
        when (mapHeader.keySerializer) {
            Serializer_Int -> when (mapHeader.valueSerializer) {
                Serializer_Short -> StraightHashMapEnv.Builder(StraightHashMapType_Int_Short)
                Serializer_Int -> StraightHashMapEnv.Builder(StraightHashMapType_Int_Int)
                Serializer_Long -> StraightHashMapEnv.Builder(StraightHashMapType_Int_Long)
                Serializer_Float -> StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                Serializer_Double -> StraightHashMapEnv.Builder(StraightHashMapType_Int_Double)
                else -> throw IllegalStateException("Unsupported value serializer: ${mapHeader.valueSerializer}")
            }
            Serializer_Long -> when (mapHeader.valueSerializer) {
                Serializer_Short -> StraightHashMapEnv.Builder(StraightHashMapType_Long_Short)
                Serializer_Int -> StraightHashMapEnv.Builder(StraightHashMapType_Long_Int)
                Serializer_Long -> StraightHashMapEnv.Builder(StraightHashMapType_Long_Long)
                Serializer_Float -> StraightHashMapEnv.Builder(StraightHashMapType_Long_Float)
                Serializer_Double -> StraightHashMapEnv.Builder(StraightHashMapType_Long_Double)
                else -> throw IllegalStateException("Unsupported value serializer: ${mapHeader.valueSerializer}")
            }
            else -> {
                throw IllegalStateException("Unsupported key serializer: ${mapHeader.keySerializer}")
            }
        }
    }
    envBuilder.openReadOnly(hashmapDir).use { env ->
        env.getCurrentMap().use { map ->
            val header = map.header
            println("Path: ${map.name}")
            println("Version: ${env.getCurrentVersion()}")
            println("Key serializer: ${header.keySerializer::class.simpleName}")
            println("Value serializer: ${header.valueSerializer::class.simpleName}")
            println("Hasher: ${header.hasher::class.simpleName}")
            println("Capacity: ${map.capacity}")
            println("Max entries: ${map.maxEntries}")
            println("Max allowed distance: ${map.maxDistance}")
            println("Size: ${map.size()}")
            println("Tombstones: ${map.tombstones()}")
            println("Total: ${map.size() + map.tombstones()}")
            println("Load percent: ${(map.size() + map.tombstones()) * 100 / map.capacity}%")
            val stat = map.stat()
            println("Max continuous block: ${stat.maxContinuousBlockLength}")
            println("Max element distance: ${stat.maxDist}")
        }
    }
}
