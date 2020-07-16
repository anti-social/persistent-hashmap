package company.evo.processor

import java.io.FileNotFoundException
import java.nio.file.Path
import java.nio.file.Paths
import javax.annotation.processing.*
import javax.lang.model.SourceVersion
import javax.lang.model.element.PackageElement
import javax.lang.model.element.TypeElement
import javax.tools.Diagnostic

@Target(AnnotationTarget.CLASS)
// @Retention(AnnotationRetention.SOURCE)
annotation class KeyValueTemplate(
        val keyTypes: Array<String>,
        val valueTypes: Array<String>
)

@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes("company.evo.processor.KeyValueTemplate")
@SupportedOptions(
        HashMapProcessor.KAPT_KOTLIN_GENERATED_OPTION_NAME,
        HashMapProcessor.KOTLIN_SOURCE_OPTION_NAME
)
class HashMapProcessor : AbstractProcessor() {
    companion object {
        const val KAPT_KOTLIN_GENERATED_OPTION_NAME = "kapt.kotlin.generated"
        const val KOTLIN_SOURCE_OPTION_NAME = "kotlin.source"
    }

    class TemplateFile(
            val dir: Path,
            val name: String
    ) {
        val filePath = dir.resolve(name)
        val content = filePath.toFile().readText()
    }

    data class ReplaceRule(val old: String, val new: String) {
        fun apply(s: String) = s.replace(old, new)
    }

    override fun process(
            annotations: MutableSet<out TypeElement>?, roundEnv: RoundEnvironment
    ): Boolean {
        val kotlinSourceDir = Paths.get(
            processingEnv.options[KOTLIN_SOURCE_OPTION_NAME] ?: run {
                error("Missing kotlin.source option")
                return false
            }
        )

        val kaptKotlinGeneratedDir = Paths.get(
            processingEnv.options[KAPT_KOTLIN_GENERATED_OPTION_NAME] ?: run {
                error("Can't find the target directory for generated Kotlin files")
                return false
            }
        )

        val annotatedElements = roundEnv.getElementsAnnotatedWith(KeyValueTemplate::class.java)
        for(element in annotatedElements) {
            element as? TypeElement ?: continue
            info("Found annotated class: $element")

            val origClassName = element.simpleName
            val (mainType, origKeyType, origValueType) = origClassName.split("_")
            val packageElement = element.enclosingElement
            if (packageElement !is PackageElement) {
                continue
            }

            val packagePath = packagePath(packageElement.qualifiedName.toString())
            val absPackagePath = kotlinSourceDir.resolve(packagePath)
            val outputDir = kaptKotlinGeneratedDir.resolve(packagePath)

            val template = try {
                TemplateFile(absPackagePath, "${origClassName}.kt")
            } catch (ex: FileNotFoundException) {
                // We don't strip annotation from generated files
                // so ignore file not found error
                return false
            }
            info("Template file: ${template.filePath}, ${template.filePath.toFile().lastModified()}")

            val keyTypesImportSuffix = ".keyTypes.$origKeyType.*"
            val keyTypesPackage = findImportPath(template.content, keyTypesImportSuffix).resolve("keyTypes")
            val keyTypesDir = kotlinSourceDir.resolve(keyTypesPackage)

            val valueTypesImportSuffix = ".valueTypes.$origValueType.*"
            val valueTypesPackage = findImportPath(template.content, valueTypesImportSuffix).resolve("valueTypes")
            val valueTypesDir = kotlinSourceDir.resolve(valueTypesPackage)

            val keyTemplate: TemplateFile
            val valueTemplate: TemplateFile
            try {
                keyTemplate = TemplateFile(
                    keyTypesDir.resolve(origKeyType),
                    getTypeFileName(origKeyType)
                )
                valueTemplate = TemplateFile(
                    valueTypesDir.resolve(origValueType),
                    getTypeFileName(origValueType)
                )
            } catch (ex: FileNotFoundException) {
                error("File not found: $ex")
                return false
            }

            val annotation = element.getAnnotation(KeyValueTemplate::class.java)
            annotation.keyTypes.forEach { keyType ->
                annotation.valueTypes.forEach { valueType ->
                    val origKeyValueType = "_${origKeyType}_${origValueType}"
                    val keyValueType = "_${keyType}_${valueType}"
                    val generatedFileName = "${mainType}${keyValueType}.kt"
                    generateFile(
                            outputDir = outputDir,
                            generatedFileName = generatedFileName,
                            template = template,
                            replaceRules = listOf(
                                    // Imports
                                    ReplaceRule(keyTypesImportSuffix, ".keyTypes.$keyType.*"),
                                    ReplaceRule(valueTypesImportSuffix, ".valueTypes.$valueType.*"),
                                    // Hash map new
                                    ReplaceRule(origKeyValueType, keyValueType)
                            )
                    )
                    generateTypeAliasFile(
                            outputDir = kaptKotlinGeneratedDir
                                .resolve(keyTypesPackage)
                                .resolve(keyType),
                            generatedFileName = getTypeFileName(keyType),
                            template = keyTemplate,
                            origType = origKeyType,
                            type = keyType
                    )
                    generateTypeAliasFile(
                            outputDir = kaptKotlinGeneratedDir
                                .resolve(valueTypesPackage)
                                .resolve(valueType),
                            generatedFileName = getTypeFileName(valueType),
                            template = valueTemplate,
                            origType = origValueType,
                            type = valueType
                    )
                }
            }
        }

        return true
    }

    private fun packagePath(pkg: String): Path {
        return pkg.split(".").let {
            Paths.get(
                it.first(),
                *it.slice(1 until it.size).toTypedArray()
            )
        }
    }

    private fun findImportPath(content: String, end: String): Path {
        for (line in content.lineSequence()) {
            if (!line.startsWith("import ")) {
                continue
            }
            val importParts = line.split(' ', limit = 2)
            val import = importParts[1]
            if (!import.endsWith(end)) {
                continue
            }
            return packagePath(import.removeSuffix(end))
        }
        throw IllegalArgumentException("Cannot found import of: $end")
    }

    private fun generateFile(
            outputDir: Path,
            generatedFileName: String,
            template: TemplateFile,
            replaceRules: List<ReplaceRule>
    ) {
        if(template.dir.resolve(generatedFileName).toFile().exists()) {
            return
        }
        outputDir.resolve(generatedFileName).toFile().apply {
            if (exists() && lastModified() > template.filePath.toFile().lastModified()) {
                return
            }

            info("Generating file: $generatedFileName")
            parentFile.mkdirs()
            val generatedContent = replaceRules.fold(template.content) { generatedContent, rule ->
                rule.apply(generatedContent)
            }
            writeText(
                    "// Generated from ${template.name}\n" +
                    generatedContent
            )
        }
    }

    private fun generateTypeAliasFile(
            outputDir: Path, generatedFileName: String, template: TemplateFile,
            origType: String, type: String
    ) {
        if (template.dir.resolve(generatedFileName).toFile().exists()) {
            return
        }
        outputDir.resolve(generatedFileName).toFile().apply {
            if (exists()) {
                return
            }
            parentFile.mkdirs()
            writeText(template.content.replace(origType, type))
        }
    }

    private fun getTypeFileName(type: String) = "${type.toLowerCase()}.kt"

    private fun error(message: Any) {
        processingEnv.messager.printMessage(Diagnostic.Kind.ERROR, message.toString())
    }

    private fun info(message: Any) {
        processingEnv.messager.printMessage(Diagnostic.Kind.NOTE, message.toString())
    }
}
