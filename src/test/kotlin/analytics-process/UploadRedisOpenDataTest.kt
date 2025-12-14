package `analytics-process`

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.restassured.RestAssured
import org.junit.jupiter.api.Assertions.assertTrue
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import org.awaitility.Awaitility
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.core.annotation.MergedAnnotations.Search
import redis.clients.jedis.JedisPooled
import util.*
import util.ListsConstants.SUMMARY_RULES
import java.io.File
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import util.RedisUtils.getRedisKeys
import util.RedisClient.jedis
import util.RedisUtils.cleanupDate
import util.Data.Companion.BASE_URL_ANALYTICS
import util.Data.Companion.DIR_SUMMARY_DUMP
import util.Data.Companion.NUMBER_OF_STREAMS
import util.ProcessStatus.aguardarProcessoCompleto
import util.RedisUtils.compararRedisComTsv
import util.RedisUtils.localizarArquivoTsv
import util.RedisUtils.validarSchemaRedisSumarizado
import util.StartProcess.PostStartProcess


class UploadRedisOpenDataTest {

    companion object {
        private var token: String = ""

        @JvmStatic
        @BeforeAll
        fun setup() {
            RestAssured.baseURI = BASE_URL_ANALYTICS
            val response = givenOauth()
            token = response.jsonPath().getString("token")
            assertNotNull(token, "Token não deve ser nulo")
        }

    }


    /**
     * 🔥 Função Test Redis — o que este teste valida
     *
     * Este teste garante que, após a execução completa do processo de ingestão,
     * todos os dados abertos foram corretamente enviados, estruturados e estão
     * consistentes entre os arquivos .tsv gerados e os dados armazenados no Redis.
     *
     * ✔ Fluxo validado pelo teste:
     *
     * 1️⃣ Dispara o processo de ingestão via /start-process
     *     - Envia a data desejada
     *     - Valida que o processo iniciou com sucesso
     *
     * 2️⃣ Aguarda a conclusão do processo
     *     - Verifica periodicamente /process-status usando Awaitility
     *     - Só avança quando status = "completed"
     *
     * 3️⃣ Valida a entrega de dados ao Redis
     *     - Lista todas as chaves com padrão:
     *           imusic:*:<date>:*
     *     - Garante que pelo menos uma chave foi criada
     *
     * 4️⃣ Valida estrutura de cada chave encontrada no Redis
     *     - Para HASH:
     *         ✔ Deve existir
     *         ✔ Não pode estar vazia
     *     - Para LIST:
     *         ✔ Deve existir
     *         ✔ Deve ter elementos (>0)
     *         ✔ Carrega amostras (até 3 itens) e imprime no log
     *
     * 5️⃣ Localiza o arquivo .tsv correspondente no diretório /tmp
     *     - Converte informações da chave Redis (platform/date)
     *     - Encontra o arquivo real com matching (ex: iMusics_Amazon_2025-11-15.tsv)
     *
     * 6️⃣ Compara dados abertos:
     *     - Carrega o arquivo TSV linha a linha
     *     - Carrega a lista correspondente no Redis
     *     - Compara:
     *         ✔ quantidade de registros
     *         ✔ conteúdo de cada linha
     *     - Exporta JSON temporário para facilitar debug em caso de falha
     *
     * 7️⃣ Valida integridade total dos dados
     *     - Qualquer divergência de conteúdo → falha o teste
     *     - Qualquer chave inesperada (string, set, zset) → falha
     *     - Qualquer chave vazia → falha
     *
     * ✔ Este teste certifica que:
     *     - O processo executou sem erro
     *     - As chaves esperadas foram criadas corretamente
     *     - Os dados abertos estão consistentes entre Redis e TSV
     *     - Não existem chaves vazias ou tipos incorretos
     *     - O pipeline de ingestão gera arquivos válidos e os dados publicados no
     *       Redis correspondem exatamente ao conteúdo processado
     *
     * ➕ Benefícios:
     *     - Garante integridade ponta-a-ponta
     *     - Garante que Redis não recebeu dados duplicados, vazios ou corrompidos
     *     - Detecta divergências nos pipelines de ETL
     *     - Serve como teste de regressão completo do processo de ingestão
     */

    @Test
    @Tag("smokeTests") // TPF-70
    @Timeout(value = 45, unit = TimeUnit.MINUTES)
    fun `CN8 - Validar entrega dos dados abertos no Redis 'process_file_to_redis'`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        //val date = LocalDate.now().plusDays(-2).format(formatter)
        var startDate ="2025-11-03"
        var endDate ="2025-11-04"

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN8 - Validar entrega dos dados abertos no Redis 'process_file_to_redis'")
        LogCollector.println("📅 Data utilizada: $startDate e $endDate")
        LogCollector.println("════════════════════════════════════════════════════\n")

        LogCollector.println("🚀 PASSO 1: Startando processamento dos períodos: $startDate | $endDate ...")
        val response = PostStartProcess(
            startDate = startDate,
            endDate = endDate,
            token = token)
        assertTrue(response?.extract()?.statusCode() == 200)
        assertEquals("Process started (background)", response?.extract()?.jsonPath()?.getString("message"))


        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 2: Aguardando conclusão do processamento...")
        Awaitility.await()
            .atMost(90, TimeUnit.MINUTES)
            .pollInterval(5, TimeUnit.MINUTES)
            .ignoreExceptions()
            .until {
                val resp = given()
                    .header("authorization", "Bearer $token")
                    .header("origin", "http://localhost")
                    .get("/process-status")
                    .then()
                    .extract()

                val status = resp.jsonPath().getString("status") ?: ""
                val msg = resp.jsonPath().getString("message") ?: ""

                LogCollector.println("🔄 Status → $status | msg: $msg")
                status.equals("completed", ignoreCase = true)
            }


        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 3: Validando Redis\n")
        val keys = getRedisKeys("imusic:*:$startDate:*")
        assertTrue(keys.isNotEmpty(), "Nenhuma chave encontrada no Redis para $startDate")

        LogCollector.println("📌 Chaves encontradas:")
        keys.forEach { LogCollector.println(" → $it") }

        // ============================================================================
        //   🔥 NOVA LÓGICA — GRUPO POR PLATAFORMA E VALIDAR TUDO
        // ============================================================================

        val players = keys
            .map { it.split(":")[2] }
            .distinct()

        players.forEach { player ->
            val metaKey = keys.firstOrNull { it.contains(":$player:") && it.endsWith(":meta") }
            val rowsKey = keys.firstOrNull { it.contains(":$player:") && it.endsWith(":rows") }

            if (metaKey == null || rowsKey == null) {
                LogCollector.println("ℹ Ignorando player '$player' — não possui meta/rows completos!")
                return@forEach
            }

            LogCollector.println("\n============================================================")
            LogCollector.println("🎧 VALIDANDO PLAYER: $player")
            LogCollector.println("============================================================")

            LogCollector.println("META → $metaKey")
            LogCollector.println("ROWS → $rowsKey\n")

            RedisUtils.validarSchemaRedis(metaKey, "hash")
            RedisUtils.validarRowCountConsistente(metaKey, rowsKey)
            RedisUtils.validarSchemaRedis(rowsKey, "list")

            // apenas se a lista não é de agregação
            if (!player.contains("topalbuns") && !player.contains("topplaysremunerado")) {
                val tsvFile = localizarArquivoTsv(rowsKey)
                compararRedisComTsv(rowsKey, tsvFile)
            }

            RedisUtils.printRedisKeyContentToFile(metaKey)
            RedisUtils.printRedisKeyContentToFile(rowsKey)
        }


        // Finalização
        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 6: Validando status final do processamento\n")
        ProcessStatus.aguardarProcessoCompleto(token = token)
        LogCollector.println("\n✔ Execução finalizada com sucesso garantindo ate a etapa 'Finalizado'.\n")
    }

    @Test
    @Tag("smokeTests") // TPF-68
    @Timeout(value = 45, unit = TimeUnit.MINUTES)
    fun `CN9 - Validar entrega dos dados abertosXagrupados no Redis 'sumarize_tops'`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = "2025-11-12"//LocalDate.now().plusDays(-60).format(formatter)

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN9 - Validar entrega dos dados abertosXagrupados no Redis 'sumarize_tops'")
        LogCollector.println("📅 Data utilizada: $date")
        LogCollector.println("════════════════════════════════════════════════════\n")

        // 🔥 Limpa o Redis ANTES de iniciar para melhorar a performance do teste
        cleanupDate(date)
        println("Redis após cleanup:")
        getRedisKeys("imusic:*:$date:*").forEach { println(" - $it") }
        println("ANTES DE INICIAR O PROCESSO:")
        jedis.lrange("imusic:topplaysremunerado:$date:rows", 0, 5)
            .forEach { println(it) }


        val startResponse = PostStartProcess (startDate = date, endDate = date, token = token)
        startResponse?.extract()?.jsonPath()?.getBoolean("success")?.let { assertTrue(it) }

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 1: Processo iniciado...")

        // Aguarda liberar Redis
        aguardarProcessoCompleto(token = token)

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🕵️‍♂ PASSO 2: Validando sumarizações TOP no Redis...")

        val plataformas = detectarPlataformas(date)
        assertTrue(plataformas.isNotEmpty(), "Nenhuma plataforma encontrada para $date!")
        LogCollector.println("📌 Plataformas detectadas: $plataformas")


        // 1) Carregar dados brutos por plataforma (imusic:*:<PLATAFORMA>:<DATA>:rows)
        val rawRowsByPlatform = plataformas.associateWith { plataforma ->
            //val keyRows = getRedisKeys("imusic:*:${plataforma}:${date}:rows").first()
            val keyRows = getRedisKeys("imusic:dashes:${plataforma}:${date}:rows")
                .firstOrNull()
                ?: error("❌ Nenhum RAW encontrado em dashes para $plataforma")

            // 1️⃣ Carrega TUDO em lista (seguro)
            println("Cheguei aqui no loadRawRowsWindowed")
            // 🚨 DEBUG: limita a quantidade de registros para testar sem travar 50 mil passou
            //val rows: List<Map<String, Any?>> = rawSeq.take(100_000).toList()

            val rows = loadRawRowsWindowed(jedis, keyRows, plataforma)
            //val rows = loadRawRows(jedis, keyRows, plataforma)
            rows

        }

        // 3) Validar cada sumarização para cada plataforma
        plataformas.forEach { plataforma ->
            // 3.1) Definir regras de sumarização
            SUMMARY_RULES.forEach { (prefix, campo, metric) ->
                val summaryKey =
                    when (prefix) {
                        "topalbuns" ->
                            "imusic:topalbuns:${date}:rows"

                        "topplaysremunerado" ->
                            "imusic:topplaysremunerado:${date}:rows"

                        else ->
                            "imusic:${prefix}:${plataforma}:${date}:rows"
                    }

                validarSchemaRedisSumarizado(summaryKey)

                LogCollector.println("\n🔎 Validando sumarização → $summaryKey")
                validarSumarizacao(
                    summaryKey,
                    rawRowsByPlatform[plataforma]!!,
                    campo,
                    DIR_SUMMARY_DUMP
                )
            }
        }

        LogCollector.println("\n🎉 Todas as sumarizações validadas com sucesso!")
        LogCollector.println("✔ Execução finalizada com sucesso garantindo ate a etapa 'sumarize_tops'.\n")
    }

    @Test
    @Tag("test")
    @Disabled("Somente Testes de consulta no Redis")
    fun SearchRedis(){
        val rawDirect = jedis.lrange("imusic:topplaysremunerado:2025-11-11:rows", 0, 10)
        rawDirect.forEach { println("DIRECT RAW = $it") }

        rawDirect.take(20).forEach { row ->
            println("REDIS PARSED => $row")
        }
    }

    /**
     *Função para Paginar de 50 em 50 mil linhas os dados do Redis
     * → Carrega TUDO em memória retornando List completa na memória
     */
    fun readLargeRedisListPaged(
        jedis: JedisPooled,
        key: String,
        pageSize: Int = 50000
    ): List<String> {

        println("Iniciando paginação Redis para key=$key pageSize=$pageSize")

        val result = mutableListOf<String>()

        var start = 0L
        val step = pageSize.toLong()

        while (true) {
            val end = start + step - 1

            println("➡️  Lendo página: start=$start end=$end")

            val page = jedis.lrange(key, start, end)

            if (page.isEmpty()) {
                println("✅ page vazia -> fim da paginação")
                break
            }

            result.addAll(page)
            start += step
        }

        println("🏁 Paginação FINALIZADA")
        return result
    }

    /**
     *Função para Carregar de 50 em 50 mil linhas os dados do Redis
     * → Stream LAZY (Sequence)
         * Não acumula nada na memória
         * Itera pageSize por vez (ex: 50.000 itens)
         * Consegue processar milhões de registros sem travar
         * Já parseia JSON → Map corretamente a cada elemento
     */
    fun loadRawRowsWindowed(
        jedis: JedisPooled,
        key: String,
        plataforma: String,
        pageSize: Int = 25_000
    ): Sequence<Map<String, Any?>> = sequence {

        var start = 0L
        val step = pageSize.toLong()

        while (true) {
            val end = start + step - 1

            println("➡️  Lendo janela: start=$start end=$end")

            val page = jedis.lrange(key, start, end)
            if (page.isEmpty()) {
                println("✅ page vazia -> fim")
                break
            }

            for (json in page) {
                val map = RedisUtils.jsonToMap(json).toMutableMap()
                map["plataform"] = plataforma
                yield(map)
            }

            start += step
        }
        println("🏁 Carregamento de Paginação FINALIZADA")
    }

    /**
     *Função para identificar as Plataformas a serem processadas
     */
    fun detectarPlataformas(date: String): Set<String> {
        val keys = getRedisKeys("imusic:*:*:$date:rows")

        return keys.mapNotNull { key ->
            val partes = key.split(":")
            if (partes.size >= 5) partes[2] else null
        }.toSet()
    }

    /**
    🧠 Objetivo: A função valida se a sumarização gravada no Redis está correta, comparando:
        - o que está no Redis
        - com o que deveria estar, calculado novamente no teste (ground truth)
    Ela garante que a lógica real de sumarização do pipeline está funcionando exatamente como foi especificado.

    A validação ocorre em 3 dimensões:
        - As chaves agrupadas são as mesmas
        - A quantidade de grupos é igual (tamanho da sumarização)
        - O valor somado (number_of_streams) por grupo é igual
     */
    fun validarSumarizacao(
        summaryKey: String,
        rawRows: Sequence<Map<String, Any?>>,
        campo: String,
        dumpDir: String
    ) {
        val jedis = RedisClient.jedis

        LogCollector.println("📌 Iniciando validação detalhada → $summaryKey")

        // 1. Paginação Redis
        val redisSummary = readLargeRedisListPaged(jedis, summaryKey)
            .map { RedisUtils.jsonToMap(it) }

        // 2. Config do agrupamento
        val config = getSumarizacaoConfig(summaryKey, campo)

        // 3. Recalcula sumarização
        val expected = recalcularSumarizacao(rawRows, config)

        // 4. Converte Redis para Map (mesmo padrão do Python)
        //println("### RAW redisSummary (first) = ${redisSummary.first()}")
        val redisMap = redisSummary.associate { row ->

            // Normaliza TODAS as chaves vindas do Redis para lowercase
            val normalized = row.mapKeys { (k, _) -> k.lowercase() }

            // Tratamento do "date" conforme o Python
            val dateVal = (normalized["date"]?.toString()?.trim()
                ?: config.dateFixed
                ?: "")

            // Monta a chave exatamente como no Python
            val key = config.groupFields.joinToString("|") { field ->
                when (field) {
                    "date" -> dateVal ?: ""
                    else -> row[field]?.toString()?.trim() ?: ""
                }
            }

            val streams = normalized[NUMBER_OF_STREAMS.lowercase()]?.toString()?.toIntOrNull() ?: 0
            key to streams
        }


        // 5. Dumps
        //println("### BEFORE SAVE: redisMap[1003715472151|2025-11-11] = ${redisMap["1003715472151|2025-11-11"]}")
        saveJsonToFile(dumpDir, "${summaryKey}_expected.json", expected)
        saveJsonToFile(dumpDir, "${summaryKey}_from_redis.json", redisMap)

        // 6. Validação de quantidade
        LogCollector.println("➡ expected: ${expected.size}, redis: ${redisMap.size}")
        assertEquals(expected.size, redisMap.size)

        // ---> Chamada do relatório HTML aqui <---
        GenerateHtmlReportFromDumps().report(
            summaryKey = summaryKey,
            campo = campo
        )

        // 7. Comparação lado a lado
        LogCollector.println("📌 Diferenças detectadas (se houver):")

        var diffs = 0

        expected.forEach { (key, expectedStreams) ->
            val redisStreams = redisMap[key]

            if (expectedStreams != redisStreams) {
                diffs++
                LogCollector.println(
                    """
                ---
                ❌ Divergência encontrada
                key: $key
                expected_streams: $expectedStreams
                redis_streams   : $redisStreams
                ---
                """.trimIndent()
                )
            }
        }

        if (diffs == 0) {
            LogCollector.println("✔ Nenhuma divergência encontrada")
        } else {
            LogCollector.println("⚠ Total de divergências: $diffs")
        }

        assertEquals(0, diffs, "Foram encontradas divergências na sumarização")
    }


    /**
     * Objetivo: Recalcular por conta própria o agrupamento verdadeiro
        - Agrupar: por um ou mais campos (definidos pela configuração da sumarização)
        - Somar number_of_streams: dos registros brutos (rawRows) dentro de cada grupo
        - Gerar um mapa: onde a chave é o agrupamento e o valor é o total somado.

       Como o calculo é feito:
        - 📂 1. Entrada: rawRows (dados brutos)
        - ⚙️ 2. Configuração de sumarização (getSumarizacaoConfig)
                Essa função analisa a key do Redis e devolve:
                    quais campos devem ser agrupados
                    qual campo será somado
        - ➕ 3. Agrupamento e soma (o cálculo correto)
                PASSO 1 — Criar chave de agrupamento para cada row
                PASSO 2 — Somar os valores para cada registro
                        pega number_of_streams
                        adiciona ao total do grupo correspondente
        - 🏁 4. Resultado final (expected)
                    a key é o agrupamento
                    o value é o total somado

    Agrupar os dados brutos por um ou mais campos e somar number_of_streams dentro de cada grupo.

     */
    fun recalcularSumarizacao(
        raw: Sequence<Map<String, Any?>>,
        config: SumarizacaoConfig
    ): Map<String, Int> {

        val acumulado = mutableMapOf<String, Int>()

        raw.forEach { row ->

            // --- 1. Filtragem igual ao Python ---
            for (required in config.requiredFields) {
                val valor = row[required.lowercase()]?.toString()?.trim()
                if (valor.isNullOrEmpty()) {
                    return@forEach // pula como continue
                }
            }

            // --- 2. Corrige comportamento do "date" igual ao Python ---
            var dateVal = row["date"]?.toString()?.trim()
            if (dateVal.isNullOrEmpty()) {
                // Se não existir no JSON, usar o date vindo da própria key
                dateVal = config.dateFixed // você deve incluir isso na config
            }

            // --- 3. Monta a chave de agrupamento exatamente igual ao Python ---
            val key = config.groupFields.joinToString("|") { field ->
                when (field) {
                    "date" -> dateVal ?: ""
                    else -> row[field]?.toString()?.trim() ?: ""
                }
            }

            // --- 4. Soma streams ---
            val streams = row[NUMBER_OF_STREAMS]?.toString()?.toIntOrNull() ?: 0

            acumulado[key] = acumulado.getOrDefault(key, 0) + streams
        }

        return acumulado
    }
    data class SumarizacaoConfig(
        val groupFields: List<String>,
        val requiredFields: List<String> = emptyList(),
        val dateFixed: String? = null
    )

    /**
     *Função configuração usada na sumarização
     */
    fun getSumarizacaoConfig(key: String, campo: String): SumarizacaoConfig {
        val k = key.lowercase()

        return when {

            // 🔥 Sempre coloque os mais específicos primeiro
            k.contains("topplaysremunerado") && !k.contains("topregioes") ->
                SumarizacaoConfig(
                    groupFields = listOf("asset_id", "territory", "date"),
                    requiredFields = listOf("asset_id", "territory")
                )

            k.contains("topregioes") ->
                SumarizacaoConfig(
                    groupFields = listOf("asset_id", "territory", "plataform", "date"),
                    requiredFields = listOf("asset_id", "territory")
                )

            // 🔥 genérico só depois dos específicos
            k.contains("topplays") ->
                SumarizacaoConfig(
                    groupFields = listOf("asset_id", "date"),
                    requiredFields = listOf("asset_id")
                )

            k.contains("topalbum") ->
                SumarizacaoConfig(
                    groupFields = listOf("upc", "plataform", "date"),
                    requiredFields = listOf("upc")
                )

            k.contains("topalbuns") ->
                SumarizacaoConfig(
                    groupFields = listOf("upc", "date"),
                    requiredFields = listOf("upc")
                )

            k.contains("topplaylist") ->
                SumarizacaoConfig(
                    groupFields = listOf("asset_id", "plataform", "stream_source", "stream_source_uri", "date"),
                    requiredFields = listOf("asset_id")
                )

            k.contains("topplataform") ->
                SumarizacaoConfig(
                    groupFields = listOf("asset_id", "plataform", "date"),
                    requiredFields = listOf("asset_id")
                )

            else -> error("Tipo desconhecido: $key")
        }
    }

    /**
     *Função para geração e interpretação do JSON
     */
    fun saveJsonToFile(dir: String, fileName: String, data: Any) {
        val folder = File(dir)
        if (!folder.exists()) folder.mkdirs()
        val mapper = jacksonObjectMapper()
        File(folder, fileName).writeText(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data))
    }





}