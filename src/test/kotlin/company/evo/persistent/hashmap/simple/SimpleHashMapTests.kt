package company.evo.persistent.hashmap.simple

import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.FloatSerializer
import company.evo.persistent.hashmap.IntSerializer
import company.evo.persistent.hashmap.Serializer
import company.evo.persistent.util.withTempDir
import io.kotlintest.shouldBe

import io.kotlintest.specs.StringSpec
import java.nio.ByteBuffer

class SimpleHashMapTests : StringSpec() {
    init {
        "test" {
            withTempDir { tmpDir ->
                SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                        .open(tmpDir)
                        .use { env ->
                            val map = env.openMap()
                            println(map)
                            map.put(1, 1.1F)
                            map.get(1, 0.0F) shouldBe 1.1F
                        }
            }
        }

//        "test" {
//            val buffer = ByteBuffer.allocate(4096)
//            val map = SimpleHashMapImpl(
//                    0L, buffer, BucketLayout(IntSerializer(), FloatSerializer())
//            )
//        }
    }
}
