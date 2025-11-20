package util

import java.io.File

object LogCollector {

    private val outputDir = File("temp/test-results").apply { mkdirs() }
    private val logFile = File(outputDir, "test_log_${System.currentTimeMillis()}.log")

    fun write(text: String) {
        logFile.appendText(text + "\n")
    }

    fun println(text: String) {
        write(text)
        kotlin.io.println(text)
    }

    fun getLogPath(): String = logFile.absolutePath
}
