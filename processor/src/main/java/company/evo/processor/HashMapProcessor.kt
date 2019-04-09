package company.evo.processor

import java.nio.file.Path
import java.nio.file.Paths
import javax.annotation.processing.*
import javax.lang.model.SourceVersion
import javax.lang.model.element.PackageElement
import javax.lang.model.element.TypeElement
import javax.tools.Diagnostic

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.SOURCE)
annotation class KeyValueTemplate(
        val keyTypes: Array<String>,
        val valueTypes: Array<String>
)

@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes("company.evo.processor.KeyValueTemplate")
@SupportedOptions(HashMapProcessor.KAPT_KOTLIN_GENERATED_OPTION_NAME)
class HashMapProcessor : AbstractProcessor() {
    companion object {
        const val KAPT_KOTLIN_GENERATED_OPTION_NAME = "kapt.kotlin.generated"
        const val KOTLIN_SOURCE_OPTION_NAME = "kotlin.source"
    }

    override fun process(
            annotations: MutableSet<out TypeElement>?, roundEnv: RoundEnvironment
    ): Boolean {
        val kotlinSourceDir = processingEnv.options[KOTLIN_SOURCE_OPTION_NAME] ?: run {
            error("Missing kotlin.source option")
            return false
        }

        val kaptKotlinGeneratedDir = processingEnv.options[KAPT_KOTLIN_GENERATED_OPTION_NAME] ?: run {
            error(
                    "Can't find the target directory for generated Kotlin files"
            )
            return false
        }

        val annotatedElements = roundEnv.getElementsAnnotatedWith(KeyValueTemplate::class.java)
        for(element in annotatedElements) {
            element as? TypeElement ?: continue

            val origClassName = element.simpleName
            val (mainType, origKeyType, origValueType) = origClassName.split("_")
            val packageElement = element.enclosingElement
            if (packageElement is PackageElement) {
                val packagePath = packageElement.qualifiedName.toString().split(".").run {
                    Paths.get(
                            first(),
                            *slice(1 until size).toTypedArray()
                    )
                }
                val absPackagePath = Paths.get(kotlinSourceDir).resolve(packagePath)
                val absSrcFilePath = absPackagePath.resolve("${origClassName}.kt")
                val srcFileContent = absSrcFilePath.toFile().readText()
                val srcKeyFileContent = getAliasTypePath(
                        absPackagePath.resolve("keyTypes"), origKeyType
                ).toFile().readText()
                val srcValueFileContent = getAliasTypePath(
                        absPackagePath.resolve("valueTypes"), origValueType
                ).toFile().readText()

                val annotation = element.getAnnotation(KeyValueTemplate::class.java)
                val outputDir = Paths.get(
                        kaptKotlinGeneratedDir,
                        packagePath.toString()
                )
                annotation.keyTypes.forEach { keyType ->
                    annotation.valueTypes.forEach { valueType ->
                        generateFile(
                                outputDir, srcFileContent,
                                mainType, origKeyType, origValueType,
                                keyType, valueType
                        )
                        generateTypeAliasFile(
                                outputDir.resolve("keyTypes"),
                                srcKeyFileContent, origKeyType, keyType
                        )
                        generateTypeAliasFile(
                                outputDir.resolve("valueTypes"),
                                srcValueFileContent, origValueType, valueType
                        )
                    }
                }
            }
        }

        return true
    }

    private fun generateFile(
            outputDir: Path, templateFileContent: String,
            mainType: String, origKeyType: String, origValueType: String,
            keyType: String, valueType: String
    ) {
        val origKeyValueType = "_${origKeyType}_${origValueType}"
        val keyValueType = "_${keyType}_${valueType}"
        val generatedFileName = "${mainType}${keyValueType}.kt"
        outputDir.resolve(generatedFileName).toFile().apply {
            parentFile.mkdirs()
            writeText(
                    templateFileContent
                            .replace(
                                    ".keyTypes.$origKeyType.*",
                                    ".keyTypes.$keyType.*"
                            )
                            .replace(
                                    ".valueTypes.$origValueType.*",
                                    ".valueTypes.$valueType.*"
                            )
                            .replace(origKeyValueType, keyValueType)
            )
        }
    }

    private fun generateTypeAliasFile(
            outputDir: Path, templateFileContent: String,
            origType: String, type: String
    ) {
        getAliasTypePath(outputDir, type).toFile().apply {
            if (exists()) {
                return
            }
            parentFile.mkdirs()
            writeText(templateFileContent.replace(origType, type))
        }
    }

    private fun getAliasTypePath(dir: Path, type: String): Path {
        return dir
                .resolve(type)
                .resolve("${type.toLowerCase()}.kt")
    }

    private fun error(message: Any) {
        processingEnv.messager.printMessage(Diagnostic.Kind.ERROR, message.toString())
    }

    private fun warn(message: Any) {
        processingEnv.messager.printMessage(Diagnostic.Kind.WARNING, message.toString())
    }
}
