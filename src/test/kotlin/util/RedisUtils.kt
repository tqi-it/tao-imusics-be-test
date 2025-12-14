package util

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions
import redis.clients.jedis.params.ScanParams
import util.ListsConstants.SCHEMA_SUMARIZADO
import util.ListsConstants.TSV_COLUMNS
import java.io.File

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
            "imusic:*:$date:*",       // cobre topplaysremunerado
            "imusic:*:*:$date:*",     // cobre dashes:plataforma
            "imusic:*:$date",         // se existir
            "imusic:*:$date:rows",
            "imusic:*:$date:meta",
            "imusic:*:$date:totals"
        )

        var total = 0

        for (pattern in patterns) {
            val keys = getRedisKeys(pattern)
            if (keys.isNotEmpty()) {
                jedis.del(*keys.toTypedArray())
                println("🧹 Removidas ${keys.size} chaves para '$pattern'")
                total += keys.size
            } else {
                println("🧹 Nenhuma chave encontrada para '$pattern'")
            }
        }

        println("🧹 LIMPEZA COMPLETA PARA $date → Total removido = $total")
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

    /**
     *Função Test Redis
     */
    fun jsonToMap(json: String): Map<String, Any?> {
        val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true)
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)

        return mapper.readValue(json, object : TypeReference<Map<String, Any?>>() {})
    }


    /**
     *Função para validar Schema das Lists e Hashes
     */
    fun validarSchemaRedis(key: String, type: String) {

        val jedis = RedisClient.jedis

        when (type) {

            // -------------------------------------------------------------
            //  ✅ VALIDAR HASH (META)
            // -------------------------------------------------------------
            "hash" -> {

                LogCollector.println("\n────────────────────────────────────────────")
                LogCollector.println("🧩 Validando SCHEMA HASH → $key")

                val data = jedis.hgetAll(key)
                require(data.isNotEmpty()) { "❌ Hash vazia: $key" }

                // Determinar schema esperado
                val expected = when {
                    key.startsWith("imusic:dashes") ->
                        ListsConstants.HASH_FIELDS_BY_DASHES

                    key.startsWith("imusic:topalbuns") ||
                            key.startsWith("imusic:topplaysremunerado") ->
                        ListsConstants.HASH_FIELDS_BY_ALBUM_BY_PLAYREMUNERADO

                    else ->
                        ListsConstants.HASH_FIELDS_BY_ALL
                }.toSet()

                val redisFields = data.keys

                LogCollector.println("🔍 Esperado: $expected")
                LogCollector.println("🔍 Redis:    $redisFields")

                // 🔴 FALTANDO CAMPOS
                val faltando = expected - redisFields
                require(faltando.isEmpty()) {
                    "❌ Campos faltando no HASH ($key): $faltando"
                }

                // 🔴 CAMPOS EXTRAS
                val extras = redisFields - expected
                require(extras.isEmpty()) {
                    "❌ Campos extras inesperados no HASH ($key): $extras"
                }

                LogCollector.println("✔ SCHEMA HASH OK para $key")
            }

            // -------------------------------------------------------------
            //  ✅ VALIDAR LIST (ROWS)
            // -------------------------------------------------------------
            "list" -> {

                if (key.startsWith("imusic:dashes")) {

                    LogCollector.println("\n────────────────────────────────────────────")
                    LogCollector.println("🧩 Validando SCHEMA LIST → $key")

                    val size = jedis.llen(key)
                    require(size > 0) { "❌ Lista vazia: $key" }

                    val sampleJson = jedis.lrange(key, 0, 0).first()
                    val map = jsonToMap(sampleJson)

                    val expectedColumns = TSV_COLUMNS.toSet()
                    val redisColumns = map.keys.toSet()

                    LogCollector.println("🔍 Esperado: $expectedColumns")
                    LogCollector.println("🔍 Redis:    $redisColumns")

                    // 🔴 FALTANDO
                    val faltando = expectedColumns - redisColumns
                    require(faltando.isEmpty()) {
                        "❌ Colunas faltando no LIST ($key): $faltando"
                    }

                    // 🔴 EXTRAS
                    val extras = redisColumns - expectedColumns
                    require(extras.isEmpty()) {
                        "❌ Colunas inesperadas no LIST ($key): $extras"
                    }

                    LogCollector.println("✔ SCHEMA LIST OK para $key")
                }
                else {
                    LogCollector.println("✔ LIST de agrupamento não precisa validar schema → $key")
                }
            }

            else -> error("❌ Tipo inesperado de key: $type ($key)")
        }
    }

    fun validarSchemaRedisSumarizado(summaryKey: String) {

        val jedis = RedisClient.jedis

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🧩 Validando SCHEMA do Redis → $summaryKey")

        val tipo = summaryKey.split(":").getOrNull(1)
            ?: error("❌ Não foi possível extrair o tipo da key: $summaryKey")

        val expectedColumns = SCHEMA_SUMARIZADO[tipo]
            ?: error("❌ Não existe SCHEMA cadastrado para '$tipo'")

        // Lê 1 json de exemplo
        val json = jedis.lrange(summaryKey, 0, 0).firstOrNull()
            ?: error("❌ Lista vazia: $summaryKey")

        val map = jsonToMap(json)

        val redisColumns = map.keys.toSet()
        val expectedSet = expectedColumns.toSet()

        LogCollector.println("🔍 Esperado: $expectedSet")
        LogCollector.println("🔍 Redis:    $redisColumns")

        // 1️⃣ Esperado → Redis (faltando campos)
        val faltando = expectedSet - redisColumns
        require(faltando.isEmpty()) {
            "❌ Faltando campos no Redis para '$summaryKey': $faltando"
        }

        // 2️⃣ Redis → Esperado (campos inesperados)
        val extras = redisColumns - expectedSet
        require(extras.isEmpty()) {
            "❌ Campos extras inesperados no Redis para '$summaryKey': $extras"
        }

        LogCollector.println("✔ SCHEMA OK para $summaryKey")
    }



    /**
     * Função Valida que o campo total_items da hash é igual à quantidade de elementos da LIST.
     *
     * Exemplo de chaves:
     *  - Hash: imusic:dashes:Amazon:2025-11-17:meta
     *  - List: imusic:dashes:Amazon:2025-11-17:rows
     */
    fun validarRowCountConsistente(keyHash: String, keyList: String) {

        if (!keyHash.startsWith("imusic:dashes")) {
            val jedis = RedisClient.jedis
            val hash = jedis.hgetAll(keyHash)
            require(hash.isNotEmpty()) { "❌ Hash vazia ao validar total_items: $keyHash" }

            val rowCountHash = hash["total_items"]?.toIntOrNull()
                ?: error("❌ Campo total_items ausente ou inválido na hash → $keyHash")

            val listSize = jedis.llen(keyList)
            require(listSize > 0) { "❌ Lista vazia ao comparar total_items: $keyList" }

            LogCollector.println("🔎 Validando total_items → HASH=$rowCountHash | LIST=$listSize")

            Assertions.assertEquals(
                rowCountHash.toLong(),
                listSize,
                "❌ Divergência: total_items na hash ($rowCountHash) ≠ quantidade de items na lista ($listSize)\n" +
                        "Hash → $keyHash\nList → $keyList"
            )

            LogCollector.println("   ✔ total_items consistente (total_items == $rowCountHash)")

        }

    }

    /**
     *Função para imprimir qualquer chave do Redis
     */
    fun printRedisKeyContentToFile(key: String) {
        val jedis = RedisClient.jedis

        val outputDir = File("temp/redis-dump")
        if (!outputDir.exists()) outputDir.mkdirs()

        val sanitizedKey = key.replace(":", "_")
        val file = File(outputDir, "$sanitizedKey.txt")

        file.bufferedWriter().use { out ->

            out.appendLine("🔑 REDIS DUMP — KEY: $key")
            out.appendLine("==============================================")
            out.appendLine("Tipo da chave: ${jedis.type(key)}\n")

            when (jedis.type(key)) {

                "hash" -> {
                    val data = jedis.hgetAll(key)
                    out.appendLine("📌 HASH (${data.size} campos):")

                    data.forEach { (k, v) ->
                        val safeVal = if (v.length > 500) v.take(500) + "...(truncated)" else v
                        out.appendLine(" • $k = $safeVal")
                    }
                }

                "list" -> {
                    val size = jedis.llen(key)
                    val limit = 200L
                    out.appendLine("📌 LISTA ($size elementos, mostrando no máximo $limit):")

                    val items = jedis.lrange(key, 0, limit - 1)

                    items.forEachIndexed { i, item ->
                        val safeItem = if (item.length > 500) item.take(500) + "...(truncated)" else item
                        out.appendLine("[$i] $safeItem")
                    }

                    if (size > limit) {
                        out.appendLine("... ($size - $limit itens não mostrados)")
                    }
                }

                "string" -> {
                    val value = jedis.get(key)
                    val safeVal = if (value != null && value.length > 500)
                        value.take(500) + "...(truncated)"
                    else value

                    out.appendLine("📌 STRING:")
                    out.appendLine(safeVal)
                }

                else -> out.appendLine("⚠ Tipo inesperado no Redis: ${jedis.type(key)}")
            }

            out.appendLine("\n✔ Arquivo gerado com sucesso.")
        }

        LogCollector.println("📄 Key salva em: ${file.absolutePath}")
    }


    /**
     * 🔥 Compara 10 primeiras linhas do Redis com 10 primeiras linhas do TSV
     *     - Valida número de colunas
     *     - Valida valores campo a campo
     */
    fun compararRedisComTsv(chave: String, tsvFile: File) {

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 5: Iniciando validação entre Redis X Arquivo...")
        val jedis = RedisClient.jedis

        LogCollector.println("\n🔍 Comparando Redis x TSV para: $chave")

        // 1️⃣ Buscar até 50 itens do Redis
        val redisRawList = jedis.lrange(chave, 0, 49)

        Assertions.assertTrue(redisRawList.isNotEmpty(), "Redis '${chave}' não possui dados!")

        val redisParsed = redisRawList.map { jsonToMap(it) }


        // 2️⃣ Ler até 50 linhas do TSV
        val tsvLines = tsvFile.readLines().drop(1).take(50)
        Assertions.assertTrue(tsvLines.isNotEmpty(), "TSV está vazio: ${tsvFile.name}")

        val tsvParsed = tsvLines.map { line ->
            val cols = line.split("\t")

            Assertions.assertEquals(
                TSV_COLUMNS.size, cols.size,
                "Número inválido de colunas no TSV (${tsvFile.name})"
            )

            TSV_COLUMNS.zip(cols).toMap()
        }

        // 3️⃣ Comparar quantidade de linhas (limitadas a 50)
        Assertions.assertEquals(
            tsvParsed.size, redisParsed.size,
            "Quantidades diferentes entre Redis e TSV em $chave"
        )
        LogCollector.println("✔ Quantidade OK → ${tsvParsed.size} registros")

        // 4️⃣ Comparar campo a campo com logs SOMENTE em caso de divergência
        tsvParsed.zip(redisParsed).forEachIndexed { index, (tsvRow, redisRow) ->

            TSV_COLUMNS.forEach { col ->

                val tsvVal = tsvRow[col]?.trim()
                val redisVal = redisRow[col]?.toString()?.trim()

                if (tsvVal != redisVal) {
                    LogCollector.println("\n────────────────────────────────────────────")
                    LogCollector.println("❌ Divergência detectada na comparação Redis x TSV")
                    LogCollector.println("Chave : $chave")
                    LogCollector.println("Linha : $index")
                    LogCollector.println("Coluna: $col")
                    LogCollector.println("TSV   : $tsvVal")
                    LogCollector.println("Redis : $redisVal")
                    LogCollector.println("────────────────────────────────────────────\n")
                }

                Assertions.assertEquals(
                    tsvVal, redisVal,
                    "Divergência na coluna '$col' (linha $index) da chave $chave"
                )
            }
        }


        LogCollector.println("✔ Conteúdo válido → TSV x Redis ($chave)\n")
    }

    fun localizarArquivoTsv(redisKey: String): File {

        // 🛑 Esses players não geram TSV, apenas dados agregados
        if (
            redisKey.startsWith("imusic:topalbuns") ||
            redisKey.startsWith("imusic:topplaysremunerado")
        ) {
            LogCollector.println("ℹ Nenhum TSV deve existir para '$redisKey' → dados agregados.")
            throw NoSuchElementException("TSV não aplicável para chave $redisKey")
        }

        Assertions.assertTrue(File(Data.DIR_TEMP).exists(), "Diretório /tmp não existe!")

        val parts = redisKey.split(":")
        val platform = parts[2]
        val date = parts[3]

        val prefix = "iMusics_${platform}_"
        val suffix = "_${date}.tsv"

        val encontrado = File(Data.DIR_TEMP).listFiles()
            ?.firstOrNull { file ->
                val nome = file.name
                nome.startsWith(prefix) && nome.endsWith(suffix)
            }
            ?: error(
                """
            ❌ Nenhum arquivo TSV correspondente encontrado no /tmp!
            → Redis key: $redisKey
            → Procurado:
                 prefix = $prefix
                 suffix = $suffix
            """.trimIndent()
            )

        LogCollector.println("📄 Arquivo TSV localizado → ${encontrado.name}")

        return encontrado
    }



}

