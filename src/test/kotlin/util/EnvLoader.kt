package util

import io.github.cdimascio.dotenv.dotenv
import java.io.File

object EnvLoader {

    val dotenv = dotenv {
        directory = findProjectRoot()
        filename = ".env"
        ignoreIfMalformed = false
        ignoreIfMissing = false
    }

    private fun findProjectRoot(): String {
        // Caminho atual (pode ser /temp durante testes)
        var dir = File(System.getProperty("user.dir"))

        // Sobe diretórios até encontrar .env
        repeat(10) {
            if (File(dir, ".env").exists()) {
                return dir.absolutePath
            }
            dir = dir.parentFile ?: return System.getProperty("user.dir")
        }

        return System.getProperty("user.dir")
    }

    fun get(name: String): String =
        dotenv[name]
            ?: error("Variável '$name' não encontrada no .env")
}
