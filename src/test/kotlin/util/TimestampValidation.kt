package util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TimestampValidation {

    private val FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

    /**
     * ✅ Valida se o timestamp existe e é válido
     * @return LocalDateTime parseado
     */
    fun validarTimestampPresente(
        campo: String,
        valor: String?
    ): LocalDateTime {

        require(!valor.isNullOrBlank()) {
            "❌ Campo '$campo' não pode ser nulo ou vazio"
        }

        return try {
            LocalDateTime.parse(valor, FORMATTER)
        } catch (ex: Exception) {
            error("❌ Campo '$campo' com formato inválido: $valor")
        }
    }

    /**
     * ✅ Valida relação entre início e fim
     */
    fun validarOrdemTemporal(
        startedAt: LocalDateTime,
        completedAt: LocalDateTime
    ) {
        require(completedAt.isAfter(startedAt)) {
            "❌ completed_at deve ser posterior a started_at"
        }
        LogCollector.println("✔ Validação com sucesso de timestamp 'started_at' menor que 'completed_at'\n")
    }

    /**
     * ✅ Valida que started_at não mudou
     */
    fun validarStartedAtImutavel(
        startedInicial: LocalDateTime,
        startedFinal: LocalDateTime
    ) {
        require(startedInicial == startedFinal) {
            """
            ❌ started_at foi alterado durante o processamento
               inicial = $startedInicial
               final   = $startedFinal
            """.trimIndent()
        }
        LogCollector.println("✔ Validação com sucesso de timestamp 'started_at' não mudou\n")
    }

    /**
     * ✅ Valida ciclo completo (atalho)
     */
    fun validarCicloCompleto(
        startedAtStr: String?,
        completedAtStr: String?
    ): Pair<LocalDateTime, LocalDateTime> {

        val startedAt = validarTimestampPresente("started_at", startedAtStr)
        val completedAt = validarTimestampPresente("completed_at", completedAtStr)

        validarOrdemTemporal(startedAt, completedAt)

        return startedAt to completedAt
    }
}
