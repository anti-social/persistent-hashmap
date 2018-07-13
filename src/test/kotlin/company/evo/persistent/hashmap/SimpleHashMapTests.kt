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

        test("locking: single writer, multiple readers")
                .config(extensions = listOf(TempDirInterceptor()))
        {
            SimpleHashMapEnv.Builder<Int, Float>().create(tmpDirPath).use {
                shouldThrow<WriteLockException> {
                    SimpleHashMapEnv.Builder<Int, Float>().create(tmpDirPath)
                }

                shouldThrow<WriteLockException> {
                    SimpleHashMapEnv.Builder<Int, Float>().openWritable(tmpDirPath)
                }

                val roMap = SimpleHashMapEnv.Builder<Int, Float>().openReadOnly(tmpDirPath)
                roMap.getVersion() shouldBe 0L
            }

            SimpleHashMapEnv.Builder<Int, Float>().openWritable(tmpDirPath).use {}
        }

        test("open missing")
                .config(extensions = listOf(TempDirInterceptor()))
        {
            SimpleHashMapEnv.Builder<Int, Float>().openWritable(tmpDirPath).use {}
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
