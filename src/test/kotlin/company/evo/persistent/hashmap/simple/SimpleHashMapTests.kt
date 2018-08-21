package company.evo.persistent.hashmap.simple

import company.evo.persistent.util.withTempDir
import io.kotlintest.shouldBe

import io.kotlintest.specs.StringSpec

class SimpleHashMapTests : StringSpec() {
    init {
        "test" {
            withTempDir { tmpDir ->
                SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                        .open(tmpDir)
                        .use { env ->
                            val map = env.getMap()
                            println(map)
                            map.put(1, 1.1F)
                            map.get(1, 0.0F) shouldBe 1.1F
                        }
            }
        }
    }
}
