package util

import redis.clients.jedis.params.ScanParams

/**
 * Utilitário seguro e performático para testes com Redis.
 *
 * Estratégia:
 *  - Primeiro tenta KEYS (rápido, ideal para ambiente de teste)
 *  - Se bloqueado → fallback automático para SCAN
 *  - Evita loops infinitos e timeouts
 */
object RedisUtils {


    private val jedis get() = RedisClient.jedis

    // ------------------------------------------------------------
    // 1. Limpa todas as chaves da data
    // ------------------------------------------------------------
    fun cleanupDate(date: String) {
        val patterns = listOf(
            "imusic:*:*:$date:rows",
            "imusic:*:*:$date:totals"
        )

        var totalRemovidas = 0

        for (pattern in patterns) {
            val keys = getRedisKeys(pattern)

            if (keys.isNotEmpty()) {
                jedis.del(*keys.toTypedArray())
                println("🧹 Limpando Redis → '$pattern' → removidas ${keys.size} chaves")
                totalRemovidas += keys.size
            } else {
                println("🧹 Nenhuma chave encontrada para '$pattern'")
            }
        }

        println("🧹 LIMPEZA FINALIZADA PARA $date → Total removido: $totalRemovidas")
    }


    // ------------------------------------------------------------
    // 2. KEYS → SCAN fallback
    // ------------------------------------------------------------
    fun getRedisKeys(pattern: String): List<String> {

        println("🔎 [TEST] SCAN → buscando '$pattern'...")

        val keys = mutableListOf<String>()
        var cursor = "0"

        val scanParams = ScanParams()
            .match(pattern)
            .count(10_000) // alto = mais rápido

        do {
            val scan = jedis.scan(cursor, scanParams)
            cursor = scan.cursor
            keys += scan.result
        } while (cursor != "0")

        println("🔎 SCAN retornou ${keys.size} chaves para '$pattern'")
        return keys.sorted()
    }

    // ------------------------------------------------------------
    // 3. Detecta plataformas no redis
    // ------------------------------------------------------------
    fun detectarPlataformas(date: String): Set<String> {
        //val keys = getRedisKeys("imusic:*:*:$date:rows")
        val keys = getRedisKeys("imusic:dashes:*:$date:rows")

        return keys.mapNotNull { key ->
            val partes = key.split(":")
            partes.getOrNull(2) // posição da plataforma
        }.toSet().also {
            println("📌 Plataformas detectadas para $date → $it")
        }
    }
}
