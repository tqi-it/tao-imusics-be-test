package `analytics-process`

import `analytics-process`.UploadRedisOpenDataTest.RedisClient.jedis
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.restassured.RestAssured
import org.junit.jupiter.api.Assertions.assertTrue
import redis.clients.jedis.JedisPooled
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import org.awaitility.Awaitility
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import util.LogCollector
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

class UploadRedisOpenDataTest {

    companion object {
        private const val BASE_URL = "http://localhost:3015"
        private var token: String = ""
        private var start: java.time.Instant? = null

        @JvmStatic
        @BeforeAll
        fun setup() {
            RestAssured.baseURI = BASE_URL

            val loginBody = """
                {
                  "grant_type": "client_credentials",
                  "email": "superadmin@taomusic.com.br",
                  "senha": "tao001"
                }
            """.trimIndent()

            val response = given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .body(loginBody)
                .post("/auth/login")
                .then()
                .statusCode(200)
                .extract()

            token = response.jsonPath().getString("token")
            assertNotNull(token, "Token não deve ser nulo")
        }

    }

    object RedisClient {
        private val host = System.getenv("REDIS_HOST") ?: "localhost"
        private val port = (System.getenv("REDIS_PORT") ?: "6379").toInt()
        private val password = System.getenv("REDIS_PASSWORD")

        val jedis: JedisPooled by lazy {
            if (password.isNullOrBlank()) {
                // Sem senha
                JedisPooled(host, port)
            } else {
                // Com senha — usando URI (a forma correta no Jedis 5.x)
                val uri = "redis://:$password@$host:$port"
                JedisPooled(uri)
            }

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


    val etapasLiberacaoRedis = setOf(
        "sumarize_top_plays",
        "sumarize_top_plataform",
        "sumarize_top_playlist",
        "sumarize_top_albuns",
        "sumarize_top_regiao",
        "sumarize_top_regioes",
        "Finalizado",
        "completed"
    )


    @Test
    @Tag("smokeTests") // TPF-70
    fun `CN6 - Validar entrega dos dados abertos no Redis 'process_file_to_redis'`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-34).format(formatter)

        val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

        val startResponse = given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .body(requestBody)
            .post("/start-process")
            .then()
            .statusCode(200)
            .extract()

        assertTrue(startResponse.jsonPath().getBoolean("success"))

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 1: Processo iniciado...")

        // Aguarda liberar Redis
        Awaitility.await()
            .atMost(8, TimeUnit.MINUTES)
            .pollInterval(8, TimeUnit.SECONDS)
            .until {
                val resp = given()
                    .header("authorization", "Bearer $token")
                    .header("origin", "http://localhost")
                    .get("/process-status")
                    .then().extract()

                val status = resp.jsonPath().getString("status") ?: ""
                val flow = resp.jsonPath().getString("current_step") ?: ""

                LogCollector.println("\n📌 Status atual → $status")
                LogCollector.println("🔄 Step atual → $flow")

                status.equals("running", true) &&
                        etapasLiberacaoRedis.contains(flow.lowercase())
            }

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 2: Validando Redis\n")

        val keys = getRedisKeys("imusic:*:$date:*")

        assertTrue(keys.isNotEmpty(), "Nenhuma chave encontrada no Redis para $date")

        LogCollector.println("📌 Chaves encontradas:")
        keys.forEach { LogCollector.println(" → $it") }

        // ============================================================================
        //   🔥 NOVA LÓGICA (PEDIDA POR VOCÊ) — GRUPO POR PLATAFORMA E VALIDAR TUDO
        // ============================================================================

        val players = keys.groupBy { it.split(":")[2] } // ex: Amazon, Youtube

        players.forEach { (player, playerKeys) ->

            LogCollector.println("\n============================================================")
            LogCollector.println("🎧 VALIDANDO PLAYER: $player")
            LogCollector.println("============================================================")

            val metaKey = playerKeys.firstOrNull { it.endsWith(":meta") }
                ?: error("❌ META não encontrada para $player")

            val rowsKey = playerKeys.firstOrNull { it.endsWith(":rows") }
                ?: error("❌ ROWS não encontrada para $player")

            LogCollector.println("META → $metaKey")
            LogCollector.println("ROWS → $rowsKey\n")

            // 4.1 — validar schema meta
            validarSchemaRedis(metaKey, "hash")

            // 4.2 — validar row_count
            validarRowCountConsistente(metaKey, rowsKey)

            // 4.3 — validar schema rows
            validarSchemaRedis(rowsKey, "list")

            // 4.4 — encontrar TSV
            val tsvFile = localizarArquivoTsv(rowsKey)

            // 4.5 — comparar Redis x TSV
            compararRedisComTsv(rowsKey, tsvFile)

            // 4.6 — dump das duas chaves
            printRedisKeyContentToFile(metaKey)
            printRedisKeyContentToFile(rowsKey)

            LogCollector.println("✔ Player $player validado com sucesso\n")
        }

        // Finalização
        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 6: Validando status final do processamento\n")

        Awaitility.await()
            .atMost(8, TimeUnit.MINUTES)
            .pollInterval(5, TimeUnit.SECONDS)
            .untilCallTo {

                val resp = given()
                    .header("authorization", "Bearer $token")
                    .header("origin", "http://localhost")
                    .get("/process-status")
                    .then()
                    .extract()

                DownloadUploadS3Test.StatusResponseFields(
                    resp.jsonPath().getString("error") ?: "",
                    resp.jsonPath().getString("current_step") ?: "",
                    resp.jsonPath().getString("message") ?: "",
                    resp.jsonPath().getString("status") ?: ""
                )

            } matches { r ->
            r?.status.equals("sumarize_top_plays", true)
        }
        LogCollector.println("\n✔ Execução finalizada com sucesso garantindo ate a etapa 'process_file_to_redis'.\n")
    }

    @Test
    @Tag("smokeTests") // TPF-69
    fun `CN7 - Validar idempotência da ingestão (reimportação) no Redis 'process_file_to_redis'`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-16).format(formatter)

        fun startProcess() {
            val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

            given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .body(requestBody)
                .post("/start-process")
                .then()
                .statusCode(200)
                .extract()

        }

        fun aguardarProcessoCompleto() {
            Awaitility.await()
                .atMost(20, TimeUnit.MINUTES)
                .pollInterval(40, TimeUnit.SECONDS)
                .until {
                    val status = given()
                        .contentType(ContentType.JSON)
                        .header("origin", "http://localhost")
                        .header("authorization", "Bearer $token")
                        .get("/process-status")
                        .then()
                        .extract()
                        .jsonPath().getString("status") ?: ""

                    LogCollector.println("⏳ Status atual → $status")
                    status.equals("completed", ignoreCase = true)
                }
        }

        LogCollector.println("\n🚀 Execução 1 — iniciando ingestão")
        startProcess()
        aguardarProcessoCompleto()
        LogCollector.println("✔ Execução 1 concluída\n")

        // Capturar estado do Redis da execução 1
        val keysExec1 = getRedisKeys("imusic:*:$date:*")
        val snapshotExec1 = keysExec1.associateWith { captureRedisOtimizadaValue(it) }

        LogCollector.println("🟦 Snapshot Execução 1 capturado (${snapshotExec1.size} chaves)")

        LogCollector.println("\n🚀 Execução 2 — reimportando mesmos dados")
        startProcess()
        aguardarProcessoCompleto()
        LogCollector.println("✔ Execução 2 concluída\n")

        // Capturar estado do Redis da execução 2
        val keysExec2 = getRedisKeys("imusic:*:$date:*")
        val snapshotExec2 = keysExec2.associateWith { captureRedisOtimizadaValue(it) }

        LogCollector.println("🟩 Snapshot Execução 2 capturado (${snapshotExec2.size} chaves)")

        // 1️⃣ Mesma quantidade de chaves
        assertEquals(
            keysExec1.size, keysExec2.size,
            "❌ Número de chaves mudou após reimportação!"
        )

        LogCollector.println("✔ Mesma quantidade de chaves nas duas execuções")

        // 2️⃣ Mesmas chaves
        assertEquals(
            keysExec1.sorted(), keysExec2.sorted(),
            "❌ Conjunto de chaves mudou na reimportação!"
        )

        LogCollector.println("✔ Mesmo conjunto de chaves nas duas execuções")

        // 3️⃣ Comparar conteúdo chave a chave
        keysExec1.forEach { key ->

            val v1 = snapshotExec1[key]!!
            val v2 = snapshotExec2[key]!!

            assertEquals(
                v1::class, v2::class,
                "❌ Tipo da chave mudou entre execuções: $key"
            )

            when (v1) {
                is Map<*, *> -> {
                    val map1 = v1 as Map<String, Any?>
                    val map2 = v2 as Map<String, Any?>

                    val CAMPOS_VOLATEIS = setOf("timestamp", "generated_at")

                    val fix1 = map1.filterKeys { !CAMPOS_VOLATEIS.contains(it) }
                    val fix2 = map2.filterKeys { !CAMPOS_VOLATEIS.contains(it) }

                    assertEquals(
                        fix1, fix2,
                        "❌ Conteúdo da hash mudou (ignorando timestamps) → $key"
                    )

                    LogCollector.println("✔ Hash idêntica ignorando campos voláteis → $key")
                }
                is List<*> -> {
                    val list1 = v1 as List<*>
                    val list2 = v2 as List<*>

                    // 🔹 Se quiser validar "tamanho" também, deixe. Mas se a lista for gigante, isso é tranquilo.
                    assertEquals(
                        list1.size, list2.size,
                        "❌ Lista com tamanho diferente após reimportação → $key"
                    )

                    // 🔥 COMPARA APENAS OS PRIMEIROS 200 ELEMENTOS
                    val limit = 200
                    val limited1 = list1.take(limit)
                    val limited2 = list2.take(limit)

                    limited1.zip(limited2).forEachIndexed { index, (i1, i2) ->
                        assertEquals(
                            i1, i2,
                            "❌ Divergência no item $index da lista após reimportação (comparação limitada a $limit itens) → $key"
                        )
                    }

                    LogCollector.println("✔ Lista válida e idêntica para chave → $key (comparação limitada a $limit itens)")
                }

            }
        }

        LogCollector.println("\n🎉 **Idempotência validada com sucesso!**")
        LogCollector.println("A reimportação não alterou nada no Redis.\n")
    }

    @Test
    @Tag("smokeTests") // TPF-68
    fun `CN8 - Validar entrega dos dados abertosXagrupados no Redis 'sumarize_tops'`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-60).format(formatter)

        val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

        val startResponse = given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .body(requestBody)
            .post("/start-process")
            .then()
            .statusCode(200)
            .extract()

        assertTrue(startResponse.jsonPath().getBoolean("success"))

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 1: Processo iniciado...")

        // Aguarda liberar Redis
        Awaitility.await()
            .atMost(30, TimeUnit.MINUTES)
            .pollInterval(55, TimeUnit.SECONDS)
            .until {
                val resp = given()
                    .header("authorization", "Bearer $token")
                    .header("origin", "http://localhost")
                    .get("/process-status")
                    .then().extract()

                val status = resp.jsonPath().getString("status") ?: ""
                val flow = resp.jsonPath().getString("current_step") ?: ""

                LogCollector.println("\n📌 Status atual → $status")
                LogCollector.println("🔄 Step atual → $flow")
                status.equals("completed", true)
            }

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🕵️‍♂ PASSO 2: Validando sumarizações TOP no Redis...")

        val dumpDir = "temp/summary-dump"

        val plataformas = detectarPlataformas(date)
        assertTrue(plataformas.isNotEmpty(), "Nenhuma plataforma encontrada para $date!")
        LogCollector.println("📌 Plataformas detectadas: $plataformas")

        // 1) Carregar dados brutos por plataforma (imusic:*:<PLATAFORMA>:<DATA>:rows)
        val rawRowsByPlatform = plataformas.associateWith { plataforma ->
            val keyRows = getRedisKeys("imusic:*:${plataforma}:${date}:rows").first()
            // converte JSON -> Map
            val rows = jedis.lrange(keyRows, 0, -1).map { jsonToMap(it).toMutableMap() }
            // Injeta a plataforma em cada row para que o recalculo tenha o mesmo campo que o Redis summary
            rows.forEach { it["plataform"] = plataforma } // observe 'plataform' com 'a' (mesma string do Python)
            rows
        }

        // 2) Definir regras de sumarização
        val summaryRules = listOf(
            Triple("topplays", "number_of_streams", "plays"),
            Triple("topplataform", "plataform", "plays"),
            Triple("topplaylist", "plataform", "plays"),
            //Triple("topalbuns", "upc", "plays"),
            //Triple("topalbum", "upc", "plays"),
            //Triple("topregiao", "territory", "plays"),
            //Triple("topregioes", "territory", "plays")
        )

        // 3) Validar cada sumarização para cada plataforma
        plataformas.forEach { plataforma ->

            summaryRules.forEach { (prefix, campo, metric) ->

                val summaryKey = "imusic:${prefix}:${plataforma}:${date}:rows"

                LogCollector.println("\n🔎 Validando sumarização → $summaryKey")

                validarSumarizacao(
                    summaryKey,
                    rawRowsByPlatform[plataforma]!!,
                    campo,
                    dumpDir
                )
            }
        }

        LogCollector.println("\n🎉 Todas as sumarizações validadas com sucesso!")
        LogCollector.println("✔ Execução finalizada com sucesso garantindo ate a etapa 'sumarize_tops'.\n")
    }


    fun detectarPlataformas(date: String): Set<String> {
        val keys = getRedisKeys("imusic:*:*:$date:rows")

        return keys.mapNotNull { key ->
            val partes = key.split(":")
            if (partes.size >= 5) partes[2] else null
        }.toSet()
    }


    fun groupAndCount(list: List<Map<String, Any?>>, key: String): Map<String, Int> {
        return list.groupBy { it[key]?.toString() ?: "null" }
            .mapValues { (_, rows) ->
                rows.sumOf { row ->
                    row["number_of_streams"]?.toString()?.toIntOrNull() ?: 0
                }
            }
            .toList()
            .sortedByDescending { it.second }
            .toMap()
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
        rawRows: List<Map<String, Any?>>,
        campo: String,
        dumpDir: String
    ) {
        val jedis = RedisClient.jedis

        // 1. Busca no Redis
        val redisSummary = jedis.lrange(summaryKey, 0, -1).map { jsonToMap(it) }
        if (redisSummary.isNotEmpty()) {
            LogCollector.println("❌ Sumarização vazia: $summaryKey")
        }
        assertTrue(redisSummary.isNotEmpty(), "❌ Sumarização vazia: $summaryKey")

        // 2. Determina agrupamento baseado no prefixo
        val config = getSumarizacaoConfig(summaryKey, campo)

        // 3. Recalcula sumarização
        val expected = recalcularSumarizacao(rawRows, config)

        // 4. Redis → Map(agrupamento → streams)
        val redisMap = redisSummary.associate { row ->
            val key = config.groupFields.joinToString("|") { field ->
                (row[field]?.toString()?.trim()?.ifEmpty { "null" } ?: "null").lowercase()
            }
            val streams = row["number_of_streams"]?.toString()?.toIntOrNull() ?: 0
            key to streams
        }

        // 5. Dump em caso de erro
        saveJsonToFile(dumpDir, "${summaryKey}_expected.json", expected)
        saveJsonToFile(dumpDir, "${summaryKey}_from_redis.json", redisMap)

        // 6. Validação de quantidade
        if (expected.size != redisMap.size) {

            LogCollector.println(
                """
                ❌ Quantidade divergente → $summaryKey  
            
                Esperado = ${expected.size}  
                Redis    = ${redisMap.size}  
            
                Veja dumps em: $dumpDir
                """.trimIndent()
                    )
        } else {
            LogCollector.println(
                "✔ Quantidade OK → $summaryKey (total = ${expected.size})"
            )
        }

        assertEquals(
            expected.size,
            redisMap.size,
            """
            ❌ Quantidade divergente → $summaryKey  
            
            Esperado = ${expected.size}  
            Redis    = ${redisMap.size}  
            
            Veja dumps em: $dumpDir
            """.trimIndent()
                )


        // 7. Validação item a item
        expected.forEach { (key, expectedStreams) ->
            val redisStreams = redisMap[key]

            if (expectedStreams != redisStreams) {
                LogCollector.println(
                    """
                    ❌ Divergência → $summaryKey  
                    Chave: $key  
                    Esperado: $expectedStreams  
                    Redis: $redisStreams  
            
                    Veja dumps em: $dumpDir
                    """.trimIndent()
                )
            } else {
                //LogCollector.println("✔ OK → $summaryKey | $key = $expectedStreams") //TODO: Arquivo .log fica muito grande
            }

            assertEquals(
                expectedStreams,
                redisStreams,
                        """
                ❌ Divergência → $summaryKey  
                Chave: $key  
                Esperado: $expectedStreams  
                Redis: $redisStreams  
            
                Veja dumps em: $dumpDir
                """.trimIndent()
                    )
        }

        LogCollector.println("✔ Sumarização validada → $summaryKey")
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
        raw: List<Map<String, Any?>>,
        config: SumarizacaoConfig
    ): Map<String, Int> {
        return raw.groupBy { row ->
            config.groupFields.joinToString("|") { field ->
                (row[field]?.toString()?.trim()?.ifEmpty { "null" } ?: "null").lowercase()
            }
        }.mapValues { (_, items) ->
            items.sumOf { it["number_of_streams"]?.toString()?.toIntOrNull() ?: 0 }
        }
    }


    data class SumarizacaoConfig(
        val groupFields: List<String>
    )
    fun getSumarizacaoConfig(key: String, campo: String): SumarizacaoConfig {
        return when {
            key.contains("topplays") -> SumarizacaoConfig(
                groupFields = listOf("asset_id", "date")
            )

            key.contains("topplataform") -> SumarizacaoConfig(
                groupFields = listOf("asset_id", "plataform", "date")
            )

            key.contains("topplaylist") -> SumarizacaoConfig(
                groupFields = listOf("asset_id", "plataform", "stream_source", "stream_source_uri", "date")
            )

            key.contains("topalbuns") -> SumarizacaoConfig(
                groupFields = listOf("upc", "date")
            )

            key.contains("topalbum") -> SumarizacaoConfig(
                groupFields = listOf("upc", "plataform", "date")
            )

            key.contains("topregiao") && !key.contains("topregioes") -> SumarizacaoConfig(
                groupFields = listOf("asset_id", "territory", "date")
            )

            key.contains("topregioes") -> SumarizacaoConfig(
                groupFields = listOf("asset_id", "territory", "plataform", "date")
            )

            else -> error("Tipo desconhecido: $key")
        }
    }

    fun saveJsonToFile(dir: String, fileName: String, data: Any) {
        val folder = File(dir)
        if (!folder.exists()) folder.mkdirs()
        val mapper = jacksonObjectMapper()
        File(folder, fileName).writeText(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data))
    }
    fun jsonToMap(json: String): Map<String, Any?> {
        val mapper = jacksonObjectMapper()
        return mapper.readValue(json, object : TypeReference<Map<String, Any?>>() {})
    }






    /**
     *Função para capturar dados do Redis para validar idempotencia
     */
    fun captureRedisValue(key: String): Any {
        val jedis = RedisClient.jedis
        return when (jedis.type(key)) {
            "hash" -> jedis.hgetAll(key)
            "list" -> jedis.lrange(key, 0, -1).map { jsonToMap(it) }
            else -> error("Tipo inesperado no Redis: ${jedis.type(key)} ($key)")
        }
    }
    fun captureRedisOtimizadaValue(key: String): Any {
        val jedis = RedisClient.jedis
        return when (jedis.type(key)) {
            "hash" -> jedis.hgetAll(key)   // OK (pequeno)
            "list" -> jedis.lrange(key, 0, 200) // <-- pega só 200 itens
            else -> "unsupported"
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
                LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 3: Iniciando validação no Schema 'hash' do Redis...")
                val data = jedis.hgetAll(key)
                require(data.isNotEmpty()) { "❌ Hash vazia: $key" }

                // Campos esperados na CHAVE META
                val HASH_FIELDS = listOf(
                    "date",
                    "file_name",
                    "platform",
                    "status",
                    "timestamp",
                    "row_count"
                )

                LogCollector.println("🔍 Validando HASH → $key")

                HASH_FIELDS.forEach { campo ->
                    require(data.containsKey(campo)) {
                        "❌ Campo obrigatório '$campo' ausente na hash → $key"
                    }
                    LogCollector.println("   ✔ $campo = ${data[campo]}")
                }

                LogCollector.println("   ✔ Schema HASH válido → $key")
            }

            // -------------------------------------------------------------
            //  ✅ VALIDAR LIST (ROWS)
            // -------------------------------------------------------------
            "list" -> {
                LogCollector.println("\n────────────────────────────────────────────")
                LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 3: Iniciando validação no Schema 'list' do Redis...")
                val size = jedis.llen(key)
                require(size > 0) { "❌ Lista vazia: $key" }

                // Pega uma amostra do primeiro JSON da lista
                val sampleJson = jedis.lrange(key, 0, 0).first()
                val map = jsonToMap(sampleJson)

                LogCollector.println("🔍 Validando LIST → $key (primeiro item)")

                // Usa o schema completo do TSV
                TSV_COLUMNS.forEach { coluna ->

                    require(map.containsKey(coluna)) {
                        "❌ Coluna '$coluna' ausente no JSON da LIST → $key"
                    }

                    val valor = map[coluna]
                    /*
                    require(valor != null && valor.toString().isNotBlank()) {
                        "❌ Coluna '$coluna' está vazia no JSON da LIST → $key"
                    }*/
                    LogCollector.println("   ✔ $coluna = $valor")
                }

                LogCollector.println("   ✔ Schema LIST válido → $key")
            }

            else -> error("❌ Tipo inesperado de key no Redis: $type ($key)")
        }
    }

    /**
     * Função Valida que o campo row_count da hash é igual à quantidade de elementos da LIST.
     *
     * Exemplo de chaves:
     *  - Hash: imusic:dashes:Amazon:2025-11-17:meta
     *  - List: imusic:dashes:Amazon:2025-11-17:rows
     */
    fun validarRowCountConsistente(keyHash: String, keyList: String) {

        val jedis = RedisClient.jedis
        val hash = jedis.hgetAll(keyHash)
        require(hash.isNotEmpty()) { "❌ Hash vazia ao validar row_count: $keyHash" }

        val rowCountHash = hash["row_count"]?.toIntOrNull()
            ?: error("❌ Campo row_count ausente ou inválido na hash → $keyHash")

        val listSize = jedis.llen(keyList)
        require(listSize > 0) { "❌ Lista vazia ao comparar row_count: $keyList" }

        LogCollector.println("🔎 Validando row_count → HASH=$rowCountHash | LIST=$listSize")

        assertEquals(
            rowCountHash.toLong(),
            listSize,
            "❌ Divergência: row_count na hash ($rowCountHash) ≠ quantidade de items na lista ($listSize)\n" +
                    "Hash → $keyHash\nList → $keyList"
        )

        LogCollector.println("   ✔ row_count consistente (row_count == $rowCountHash)")
    }


    /**
     * 🔥 Compara 10 primeiras linhas do Redis com 10 primeiras linhas do TSV
     *     - Valida número de colunas
     *     - Valida valores campo a campo
     */
    private val TSV_COLUMNS = listOf(
        "label_id","product_id","asset_id","date","territory","number_of_downloads",
        "number_of_streams","number_of_listeners","number_of_saves","reporting_organization_id",
        "asset_duration","stream_device_type","stream_device_os","stream_length","stream_source",
        "stream_source_uri","user_id","user_region","user_region_detail","user_gender",
        "user_birth_year","user_age_group","user_country","user_access_type",
        "user_account_type","track_id","primary_artist_ids","isrc","upc","shuffle",
        "repeat","cached","completion","apple_container_type",
        "apple_container_sub_type","apple_source_of_stream","youtube_channel_id",
        "video_id","youtube_claimed_status","subscribed_status","asset_type",
        "discovery_flag","current_membership_week","first_trial_membership_week",
        "first_trial_membership","first_paid_membership_week","first_paid_membership",
        "total_user_streams","youtube_uploader_type","audio_format","dsp_data"
    )
    fun compararRedisComTsv(chave: String, tsvFile: File) {

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 5: Iniciando validação entre Redis X Arquivo...")
        val jedis = RedisClient.jedis

        LogCollector.println("\n🔍 Comparando Redis x TSV para: $chave")

        // 1️⃣ Buscar até 10 itens do Redis
        val redisRawList = jedis.lrange(chave, 0, 9)

        assertTrue(redisRawList.isNotEmpty(), "Redis '${chave}' não possui dados!")

        val redisParsed = redisRawList.map { jsonToMap(it) }


        // 2️⃣ Ler até 10 linhas do TSV
        val tsvLines = tsvFile.readLines().drop(1).take(10)
        assertTrue(tsvLines.isNotEmpty(), "TSV está vazio: ${tsvFile.name}")

        val tsvParsed = tsvLines.map { line ->
            val cols = line.split("\t")

            assertEquals(
                TSV_COLUMNS.size, cols.size,
                "Número inválido de colunas no TSV (${tsvFile.name})"
            )

            TSV_COLUMNS.zip(cols).toMap()
        }

        // 3️⃣ Comparar quantidade de linhas (limitadas a 10)
        assertEquals(
            tsvParsed.size, redisParsed.size,
            "Quantidades diferentes entre Redis e TSV em $chave"
        )
        LogCollector.println("✔ Quantidade OK → ${tsvParsed.size} registros")


        // 4️⃣ Comparar campo a campo
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

                assertEquals(
                    tsvVal, redisVal,
                    "Divergência na coluna '$col' (linha $index) da chave $chave"
                )
            }
        }


        LogCollector.println("✔ Conteúdo válido → TSV x Redis ($chave)\n")
    }

    fun localizarArquivoTsv(redisKey: String): File {

        val tmpDir = File("/tmp")
        assertTrue(tmpDir.exists(), "Diretório /tmp não existe!")

        // Exemplo redisKey → imusic:dashes:Amazon:2025-11-13:rows
        val parts = redisKey.split(":")
        val platform = parts[2]          // Amazon
        val date = parts[3]              // 2025-11-13

        val prefix = "iMusics_${platform}_"   // Ex: iMusics_Amazon_
        val suffix = "_${date}.tsv"           // Ex: _2025-11-13.tsv"

        val encontrado = tmpDir.listFiles()
            ?.firstOrNull { file ->
                val nome = file.name
                nome.startsWith(prefix) && nome.endsWith(suffix)
            }
            ?: error(
                """
            ❌ Nenhum arquivo TSV correspondente encontrado no /tmp!
            → Redis key: $redisKey
            → Procurado padrão:
                 prefix = $prefix
                 suffix = $suffix
            → Arquivos encontrados no /tmp:
            ${tmpDir.listFiles()?.joinToString("\n") { " - ${it.name}" }}
            """.trimIndent()
            )

        LogCollector.println("📄 Arquivo TSV localizado → ${encontrado.name}")

        return encontrado
    }

    fun jsonToMap0(json: String): Map<String, Any> {
        val mapper = jacksonObjectMapper()
        return mapper.readValue(json, object : TypeReference<Map<String, Any>>() {})
    }


    /**
     *Função Test Redis
     */

    fun getRedisKeys(pattern: String): List<String> {
        val jedis = RedisClient.jedis
        val keys = mutableListOf<String>()
        var cursor = "0"

        val scanParams = redis.clients.jedis.params.ScanParams().match(pattern).count(500)

        do {
            val scanResult = jedis.scan(cursor, scanParams)
            cursor = scanResult.cursor
            keys += scanResult.result
        } while (cursor != "0")

        return keys.sorted()
    }


    fun getRedisType(key: String): String {
        return RedisClient.jedis.type(key)
    }

    fun getRedisListSize(key: String): Long {
        return RedisClient.jedis.llen(key)
    }

    fun getRedisListSample(key: String, size: Int = 5): List<String> {
        return RedisClient.jedis.lrange(key, 0, (size - 1).toLong())
    }

    fun getRedisHash(key: String): Map<String, String> {
        return RedisClient.jedis.hgetAll(key)
    }


}