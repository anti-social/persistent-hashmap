package company.evo.persistent.util

import java.nio.file.Files

import org.apache.commons.io.FileUtils.deleteDirectory
import java.nio.file.Path

class TempDir : AutoCloseable {
    val path: Path = Files.createTempDirectory(null)
    private val tmpDir = path.toFile()

    init {
        tmpDir.deleteOnExit()
    }

    override fun close() {
        deleteDirectory(tmpDir)
    }
}

fun withTempDir(body: (Path) -> Unit) = TempDir().use { dir ->
    body(dir.path)
}
