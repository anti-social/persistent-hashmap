package company.evo.persistent.hashmap

import io.kotlintest.TestCaseConfig
import io.kotlintest.TestResult
import io.kotlintest.extensions.TestCaseExtension
import io.kotlintest.extensions.TestCaseInterceptContext
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.FunSpec

import java.nio.file.Files
import java.nio.file.Path

import org.apache.commons.io.FileUtils.deleteDirectory

class SimpleHashMapTests : FunSpec() {
    private lateinit var tmpDirPath: Path

    init {
        test("test") {
            1 shouldBe 1
        }

        test("env: single writer, multiple readers")
                .config(extensions = listOf(TempDirInterceptor()))
        {
            SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                    .open(tmpDirPath)
                    .use { env ->
                        env.getCurrentVersion() shouldBe 0L

                        SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                                .openReadOnly(tmpDirPath)
                                .use { roEnv ->
                                    roEnv.getCurrentVersion() shouldBe 0L
                                }

                        shouldThrow<WriteLockException> {
                            SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                                    .open(tmpDirPath)
                        }
                    }

            SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                    .open(tmpDirPath)
                    .use { env ->
                        env.getCurrentVersion() shouldBe 0L
                    }
        }

        test("env: copy map")
                .config(extensions = listOf(TempDirInterceptor()))
        {
            SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                    .open(tmpDirPath)
                    .use { env ->
                        val map = env.getMap()
                        env.getCurrentVersion() shouldBe 0L
                        env.copyMap(map)
                        env.getCurrentVersion() shouldBe 1L

                        SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                                .openReadOnly(tmpDirPath)
                                .use { roEnv ->
                                    roEnv.getCurrentVersion() shouldBe 1L
                                }
                    }
        }
    }

    inner class TempDirInterceptor : TestCaseExtension {

        override fun intercept(
                context: TestCaseInterceptContext,
                test: (TestCaseConfig, (TestResult) -> Unit) -> Unit,
                complete: (TestResult) -> Unit)
        {
            tmpDirPath = Files.createTempDirectory(null)
            val tmpDir = tmpDirPath.toFile()
            tmpDir.deleteOnExit()
            try {
                test(context.config, complete)
            } finally {
                deleteDirectory(tmpDir)
            }
        }
    }
}
