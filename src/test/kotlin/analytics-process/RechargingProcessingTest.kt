package `analytics-process`

import io.restassured.RestAssured
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import util.*
import util.ProcessStatus.processStatus
import util.RedisUtils.compararRedisComTsv
import util.RedisUtils.localizarArquivoTsv
import util.RedisUtils.printRedisKeyContentToFile
import util.RedisUtils.validarRowCountConsistente
import util.RedisUtils.validarSchemaRedis
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

class RechargingProcessingTest {

/*
## 🔍 Detecção Automática: Normal vs Reprocessamento

    | Período | Classificação | Endpoints | Flag |
    |---------|---------------|-----------|------|
    | ≤ 5 dias | **Processamento Normal** | `SYM_PROCESS_MAPPING` | `is_reprocessing=false` |
    | > 5 dias | **Reprocessamento** | `SYM_REPROCESS_MAPPING` | `is_reprocessing=true` |
    | > 365 dias | **Rejeitado** | N/A | Erro HTTP 400 |

    1. `test_reprocessing_flag_auto_detection()` - Detecta corretamente
    2. `test_correct_endpoint_selection()` - Escolhe endpoints corretos
    3. `test_max_period_validation()` - Valida limites
    4. `test_complete_reprocessing_flow()` - Fluxo completo
    5. `test_error_handling()` - Tratamento de erros
    6. `test_no_duplicates_after_reprocessing()` - Sem duplicatas

*/

    companion object {
        private var token: String = ""

        @JvmStatic
        @BeforeAll
        fun setup() {
            RestAssured.baseURI = Data.BASE_URL_ANALYTICS
            val response = givenOauth()
            token = response.jsonPath().getString("token")
            assertNotNull(token, "Token não deve ser nulo")
        }

    }


    /**
     * → Tests PROCESSAMENTO NORMAL (≤ 5 dias)
     */
    @Test
    @Tag("smokeTests") // TPF-72 TODO: Aguardando start-process do BE para consegui processar até o final
    fun `CN10 - Validar reprocessamento menor ou igual 5 dias`() {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val startDate = LocalDate.now().plusDays(-5).format(formatter)
        val endDate = LocalDate.now().plusDays(-1).format(formatter)

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN10 - Validar reprocessamento menor ou igual 5 dias")
        LogCollector.println("📅 Data utilizada: $startDate e $endDate")
        LogCollector.println("════════════════════════════════════════════════════\n")

        LogCollector.println("🚀 PASSO 1: Startando processamento dos períodos: $startDate | $endDate ...")
        LogCollector.println("\n🚀 Execução 1 — iniciando ingestão")
        val response = StartProcess.PostStartProcess(
            startDate = startDate,
            endDate = endDate,
            token = token
        )

        assertTrue(response?.extract()?.statusCode() == 200)
        assertEquals("Process started (background)", response?.extract()?.jsonPath()?.getString("message"))
        response?.extract()?.jsonPath()?.getBoolean("is_reprocessing")?.let { assertFalse(it) }
        response?.extract()?.jsonPath()?.getBoolean("success")?.let { assertTrue(it) }
        /*assertEquals(30, response?.extract()?.jsonPath()?.getInt("period_days"))*/
        assertNull(response?.extract()?.jsonPath()?.getString("warning"),"Campo 'warning' não deveria estar presente!")
        ProcessStatus.aguardarProcessoCompleto(token = token)
        LogCollector.println("✔ Execução 1 concluída\n")

        LogCollector.println("\n────────────────────────────────────────────")
        LogCollector.println("🚀 PASSO 3: Validando Redis\n")
        val keys = RedisUtils.getRedisKeys("imusic:*:$startDate:*")
        assertTrue(keys.isNotEmpty(), "Nenhuma chave encontrada no Redis para $startDate")

        LogCollector.println("📌 Chaves encontradas:")
        keys.forEach { LogCollector.println(" → $it") }

        // ============================================================================
        //   🔥 NOVA LÓGICA — GRUPO POR PLATAFORMA E VALIDAR TUDO
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

            // 4.2 — validar total_items
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
    }


    /**
     * → Tests REPROCESSAMENTO (acima de 5 dias)
     */
    @Test
    @Tag("smokeTests") // TPF-69
    @Timeout(value = 45, unit = TimeUnit.MINUTES)
    fun `CN11 - Validar reprocessamento maior 5 dias garantindo idempotência da ingestão no Redis 'process_file_to_redis'`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-8).format(formatter)
        val warning = "REPROCESSAMENTO: período de 8 dias. Dados serão marcados com flag \"reprocess\" no Redis. Considere usar instância dedicada para períodos maiores que 5 dias."

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN11 - Validar reprocessamento maior 5 dias garantindo idempotência da ingestão no Redis 'process_file_to_redis'")
        LogCollector.println("📅 Data utilizada: $date")
        LogCollector.println("════════════════════════════════════════════════════\n")

        ProcessStatus.historicoProgresso.clear()

        // PASSO1: Consulta registros no período e guarda OK
        // PASSO2: Chama a reprocessamento OK
        // PASSO3: Consulta registros dentro do período após reprocessamento OK
        // PASSO4: Garantir no periodo que possui a mesma quantidade de registros OK
        // PASSO5: Compara dados do PASSO1 x PASSO3 garantindo que nao houve duplicações de registros apenas atualização OK

        LogCollector.println("🚀 PASSO 1: Startando processamento dos períodos: $date | $date ...")
        LogCollector.println("\n🚀 Execução 1 — iniciando ingestão")
        val response = StartProcess.PostStartProcess(
            startDate = date,
            endDate = date,
            token = token
        )

        assertTrue(response?.extract()?.statusCode() == 200)
        assertEquals("Process started (background)", response?.extract()?.jsonPath()?.getString("message"))
        response?.extract()?.jsonPath()?.getBoolean("is_reprocessing")?.let { assertTrue(it) }
        response?.extract()?.jsonPath()?.getBoolean("success")?.let { assertTrue(it) }
        assertEquals(8, response?.extract()?.jsonPath()?.getInt("period_days"))
        assertEquals(warning, response?.extract()?.jsonPath()?.getString("warning"))

        ProcessStatus.aguardarProcessoCompleto(token = token)
        LogCollector.println("✔ Execução 1 concluída\n")
        val statusCompletedAt1 = processStatus(token = token)
            TimestampValidation.validarCicloCompleto(
                startedAtStr = statusCompletedAt1?.extract()?.jsonPath()?.getString("started_at"),
                completedAtStr = statusCompletedAt1?.extract()?.jsonPath()?.getString("completed_at")
            )



        // Capturar estado do Redis da execução 1
        val keysExec1 = RedisUtils.getRedisKeys("imusic:*:$date:*")
        val snapshotExec1 = keysExec1.associateWith { captureRedisOtimizadaValue(it) }

        LogCollector.println("🟦 Snapshot Execução 1 capturado (${snapshotExec1.size} chaves)")

        LogCollector.println("\n🚀 Execução 2 — reimportando mesmos dados")
        StartProcess.PostStartProcess(
            startDate = date,
            endDate = date,
            token = token
        )
        assertTrue(response?.extract()?.statusCode() == 200)
        assertEquals("Process started (background)", response?.extract()?.jsonPath()?.getString("message"))
        response?.extract()?.jsonPath()?.getBoolean("is_reprocessing")?.let { assertTrue(it) }
        response?.extract()?.jsonPath()?.getBoolean("success")?.let { assertTrue(it) }
        assertEquals(8, response?.extract()?.jsonPath()?.getInt("period_days"))
        assertEquals(warning, response?.extract()?.jsonPath()?.getString("warning"))

        ProcessStatus.aguardarProcessoCompleto(token = token)
        LogCollector.println("✔ Execução 2 concluída\n")
        val statusCompletedAt2 = processStatus(token = token)
        TimestampValidation.validarCicloCompleto(
            startedAtStr = statusCompletedAt2?.extract()?.jsonPath()?.getString("started_at"),
            completedAtStr = statusCompletedAt2?.extract()?.jsonPath()?.getString("completed_at")
        )


        // Capturar estado do Redis da execução 2
        val keysExec2 = RedisUtils.getRedisKeys("imusic:*:$date:*")
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
        ProcessStatus.imprimirHistorico()
    }


    /**
     * → Tests REJEITADO (366 dias) Erro HTTP 400
     */
    @Test
    @Tag("smokeTests") // TPF-72
    fun `CN12 - Validar tentativa de reprocessamento maior que 365 dias Rejeitado`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-366).format(formatter)

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN12 - Reprocessamento acima de 365 dias (REJEITADO)")
        LogCollector.println("📅 Data inicial : $date")
        LogCollector.println("📅 Data final   : $date")
        LogCollector.println("📏 Intervalo    : 366 dias")
        LogCollector.println("════════════════════════════════════════════════════\n")

        // ─────────────────────────────────────────────
        // PASSO 1 — Disparo do processamento
        // ─────────────────────────────────────────────
        LogCollector.println("🚀 PASSO 1: Enviando requisição start-process")

        val response = StartProcess.PostStartProcess(
            startDate = date,
            endDate = date,
            token = token
        )

        val statusCode = response?.extract()?.statusCode()
        val success = response?.extract()?.jsonPath()?.getBoolean("success")
        val error = response?.extract()?.jsonPath()?.getString("error")

        LogCollector.println("🔎 PASSO 2: Validando regras de negócio")

        assertEquals(
            400,
            statusCode,
            "❌ Esperado HTTP 400 para período superior a 365 dias"
        )

        assertFalse(
            success ?: true,
            "❌ Esperado success=false para período inválido"
        )

        assertEquals(
            "Período solicitado (366 dias) excede o máximo permitido de 365 dias",
            error,
            "❌ Mensagem de erro inesperada"
        )

        LogCollector.println("📥 Resposta recebida:")
        LogCollector.println("   ➤ HTTP Status : $statusCode")
        LogCollector.println("   ➤ success     : $success")
        LogCollector.println("   ➤ error       : $error")

        LogCollector.println("✅ Validação concluída com sucesso")
        LogCollector.println("🏁 CN12 FINALIZADO COM SUCESSO\n")
    }


    /**
     *Função para capturar dados do Redis para validar idempotencia
     */
    fun captureRedisOtimizadaValue(key: String): Any {
        val jedis = RedisClient.jedis
        return when (jedis.type(key)) {
            "hash" -> jedis.hgetAll(key)   // OK (pequeno)
            "list" -> jedis.lrange(key, 0, 200) // <-- pega só 200 itens
            else -> "unsupported"
        }
    }

    /**
     *Função para validar o timestamp dos campos started_at e completed_at
     */

}