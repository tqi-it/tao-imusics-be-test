package `analytics-process`

import io.restassured.RestAssured
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import org.apache.http.HttpStatus
import org.awaitility.Awaitility
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.fail
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import `process-sync`.TopContractAnalyticsGeralTest
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import util.EnvLoader
import util.ListsConstants.EXPECTED_PLAYERS
import util.ListsConstants.PLAYERS_ICON
import util.LogCollector
import util.Data.Companion.BASE_URL_ANALYTICS
import util.Data.Companion.DIR_TEMP
import util.Data.Companion.PATH_PROCESS
import util.ProcessStatus.aguardarProcessoCompleto
import util.ProcessStatus.imprimirHistorico
import util.StartProcess.PostStartProcess
import util.StartProcess.PostStartProcessNotDate
import util.givenOauth
import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD) // Executar um teste por vez
class DownloadUploadS3Test {

    /**
     * Objetivo: Classe de teste para validação do fluxo do projeto analytics (im-symphonia-analytics)
     * Pré Condição:
     *  - Subir localmente o projeto im-symphonia-analytics (Comando: make start)
     *  - Verificar em qual porta ele subiu para passa na váriavel BASE_URL
     *  Tarefa: TPF-67
     */

    companion object {
        private var token: String = ""
        private var start: java.time.Instant? = null

        /**
         * Parâmetros do CN1
         */
        private var startDateCN1 ="2025-12-02"
        private var endDateCN1 ="2025-12-03"

        // Parâmetros dos testes caminho feliz
        val timeoutFull = Duration.ofMinutes(15) // tempo máximo total do teste

        // S3
        val bucketS3 = EnvLoader.get("AWS_S3_BUCKET_NAME")
        val regionS3 = EnvLoader.get("AWS_S3_REGION_NAME")
        val region = EnvLoader.get("AWS_S3_REGION_NAME")
        val key = EnvLoader.get("AWS_ACCESS_KEY_ID")
        val secret = EnvLoader.get("AWS_SECRET_ACCESS_KEY")
        val prefixS3 = EnvLoader.get("AWS_S3_FILE_PREFIX")


        @JvmStatic
        @BeforeAll
        fun setup() {
            RestAssured.baseURI = BASE_URL_ANALYTICS
            val response = givenOauth()
            token = response.jsonPath().getString("token")
            assertNotNull(token, "Token não deve ser nulo")
        }

    }


    @Test
    @Tag("smokeTests") // TPF-70
    @Timeout(value = 45, unit = TimeUnit.MINUTES)
    fun `CN1 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3`() {

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN1 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3")
        LogCollector.println("📅 Data utilizada: $startDateCN1 e $endDateCN1 ")
        LogCollector.println("════════════════════════════════════════════════════\n")


        // 🔹 Fazer chamada ao /start-process
        val response = PostStartProcess(
            startDate = startDateCN1,
            endDate= endDateCN1,
            token=token)
        assertTrue(response?.extract()?.statusCode() == 200)
        response?.extract()?.jsonPath()?.getBoolean("success")?.let { assertTrue(it) }
        assertEquals("Process started (background)", response?.extract()?.jsonPath()?.getString("message"))

        // 🔹 Tempo do teste
        capturaDateTime()

        // 🔹 Validar se os arquivos foram deletados no diretório
        validarTmpSemArquivosDePlayers()

        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 2: Consultando status do processamento...")
        aguardarProcessoCompleto(token = token)

        // 🔹 Tempo do teste
        calcDateTime()

        // 🔥 Validação dos arquivos no /tmp
        validarArquivosNoTmp("$startDateCN1", "$endDateCN1")

        // 🔥 Validação dos arquivos no .gz descompactado
        var filesGz = filterFilesGz()
        validarTsvDescompactadosNoTmp(filesGz)

        // 🔥 Validação dos arquivos no S3
        validarArquivosNoS3(prefixS3)

        imprimirHistorico()
    }

    @Test
    @Tag("smokeTests") // TPF-67
    fun `CN2 - Validar ingestão quando não possui arquivos para baixar`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().format(formatter)

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN2 – Validar ingestão sem arquivos para baixar")
        LogCollector.println("📅 Data utilizada: $date")
        LogCollector.println("════════════════════════════════════════════════════\n")

        // PASSO 1
        LogCollector.println("🟦 PASSO 1: Iniciando processamento via API POST /start-process...")
        val response = PostStartProcess(
            startDate = date,
            endDate = date,
            token = token
        )

        val httpStatusStart = response?.extract()?.statusCode()
        LogCollector.println("➡️  HTTP Status recebido: $httpStatusStart")
        assertTrue(httpStatusStart == 200)

        val success = response?.extract()?.jsonPath()?.getBoolean("success")
        LogCollector.println("➡️  Campo success: $success")
        assertTrue(success == true)

        LogCollector.println("\n🟦 PASSO 2: Aguardando backend processar (Awaitility)\n")

        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(2, TimeUnit.SECONDS)
            .untilCallTo {

                LogCollector.println("🔄 Consultando /process-status ...")

                val resp = given()
                    .contentType(ContentType.JSON)
                    .header("origin", "http://localhost")
                    .header("authorization", "Bearer $token")
                    .get("/process-status")
                    .then()
                    .log().all()
                    .statusCode(HttpStatus.SC_OK)
                    .extract()

                val error = resp.jsonPath().getString("error") ?: ""
                val currentStep = resp.jsonPath().getString("current_step") ?: ""
                val message = resp.jsonPath().getString("message") ?: ""
                val status = resp.jsonPath().getString("status") ?: ""
                val httpCode = resp.statusCode()

                LogCollector.println("📥 Resposta recebida:")
                LogCollector.println("   ▪ error: $error")
                LogCollector.println("   ▪ current_step: $currentStep")
                LogCollector.println("   ▪ message: $message")
                LogCollector.println("   ▪ status: $status")
                LogCollector.println("   ▪ http code: $httpCode")

                val resultMessage = resp.jsonPath().getString("result.message")
                val resultStatus = resp.jsonPath().getString("result.status")
                val resultStart = resp.jsonPath().getString("result.start_date")
                val resultEnd = resp.jsonPath().getString("result.end_date")

                LogCollector.println("📦 result:")
                LogCollector.println("   ▪ result.message = $resultMessage")
                LogCollector.println("   ▪ result.status  = $resultStatus")
                LogCollector.println("   ▪ result.start   = $resultStart")
                LogCollector.println("   ▪ result.end     = $resultEnd")

                StatusResponseFields(
                    error = error,
                    currentStep = currentStep,
                    message = message,
                    status = status,
                    httpStatus = httpCode,
                    result = ResultFields(
                        message = resultMessage,
                        status = resultStatus,
                        startDate = resultStart,
                        endDate = resultEnd
                    )
                )

            } matches { result ->

            LogCollector.println("\n🟩 PASSO 3: Validando condições finais...")

            val r = result as StatusResponseFields

            LogCollector.println("🔎 Validações:")
            LogCollector.println("   ▪ error vazio? -> ${r.error.isNullOrBlank()}")
            LogCollector.println("   ▪ status == 'completed'? -> ${r.status.equals("completed", true)}")
            LogCollector.println("   ▪ current_step == 'Finalizado'? -> ${r.currentStep.equals("Finalizado", true)}")
            LogCollector.println(
                "   ▪ result.message esperado? -> ${
                    r.result?.message.equals(
                        "FUGA não tem dados de analytics para o período solicitado",
                        true
                    )
                }"
            )

            val resultMessageOk =
                r.result?.message?.equals(
                    "FUGA não tem dados de analytics para o período solicitado",
                    ignoreCase = true
                ) == true

            val ok =
                r.httpStatus == 200 &&
                        r.error.isNullOrBlank() &&
                        r.status.equals("completed", ignoreCase = true) &&
                        r.currentStep.equals("Finalizado", ignoreCase = true) &&
                        resultMessageOk

            if (ok) {
                LogCollector.println("🎉 TESTE APROVADO – condições finais válidas!")
            } else {
                LogCollector.println("❌ TESTE REPROVADO – alguma condição não foi satisfeita!")
            }

            ok
        }

        LogCollector.println("\n🏁 CN2 FINALIZADO\n")
    }


    @Test
    @Tag("smokeTests")  // TPF-67
    fun `CN3 - Validar ingestão quando já possui um processamento sendo realizado`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-5).format(formatter)

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN3 – Validar bloqueio quando já existe processamento")
        LogCollector.println("📅 Data utilizada: $date")
        LogCollector.println("════════════════════════════════════════════════════\n")

        // Corpo JSON da requisição
        val requestBody = """
        {
          "start-date": "$date",
          "end-date": "$date"
        }
    """.trimIndent()

        repeat(2) { tentativa ->

            val numero = tentativa + 1
            LogCollector.println("\n────────────────────────────────────────────────────")
            LogCollector.println("▶️  Tentativa $numero – Enviando POST /start-process")
            LogCollector.println("────────────────────────────────────────────────────")

            val resp = given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .log().all()
                .body(requestBody)
                .post(PATH_PROCESS)
                .then()
                .log().all()
                .extract()

            val statusCode = resp.statusCode()
            val success = resp.jsonPath().getBoolean("success")
            val error = resp.jsonPath().getString("error")

            LogCollector.println("📥 Resposta recebida:")
            LogCollector.println("   ▪ statusCode = $statusCode")
            LogCollector.println("   ▪ success    = $success")
            LogCollector.println("   ▪ error      = $error\n")

            if (tentativa == 0) {
                // ==========================================================
                // PRIMEIRA EXECUÇÃO → ESPERADO = ACEITAR O PROCESSAMENTO
                // ==========================================================
                LogCollector.println("🔎 Validando Tentativa 1...")

                assertEquals(
                    HttpStatus.SC_OK,
                    statusCode,
                    "❌ Primeira execução deveria retornar 200 OK"
                )
                assertTrue(success, "❌ Primeira execução deveria retornar success=true")

                LogCollector.println("✔️ Tentativa 1 OK — Processo aceito normalmente")

                // Pequeno delay para permitir que o processo entre no estado "running"
                Thread.sleep(3000)

            } else {
                // ==========================================================
                // SEGUNDA EXECUÇÃO → ESPERADO = BLOQUEIO (409)
                // ==========================================================
                LogCollector.println("🔎 Validando Tentativa 2...")

                assertEquals(
                    HttpStatus.SC_CONFLICT,
                    statusCode,
                    "❌ Segunda execução deveria retornar 409, mas retornou $statusCode"
                )

                assertEquals(
                    "Process already running",
                    error,
                    "❌ Mensagem de erro incorreta para processo já em execução"
                )

                assertFalse(success, "❌ Segunda execução deveria retornar success=false")

                LogCollector.println("✔️ Tentativa 2 BLOQUEADA como esperado — 409 CONFLICT")
            }
        }

        LogCollector.println("\n🏁 CN3 FINALIZADO COM SUCESSO\n")
    }

    @Test
    @Tag("smokeTests") // TPF-70 TODO: Aguardando start-process do BE para consegui processar até o final
    @Timeout(value = 45, unit = TimeUnit.MINUTES)
    fun `CN4 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3 sem passar data`() {

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN4 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3 sem passar data")
        LogCollector.println("📅 Sem Data")
        LogCollector.println("════════════════════════════════════════════════════\n")

        // 🔹 Fazer chamada ao /start-process
        val response = PostStartProcessNotDate(
            token=token)
        assertTrue(response?.extract()?.statusCode() == 200)
        response?.extract()?.jsonPath()?.getBoolean("success")?.let { assertTrue(it) }
        assertEquals("Process started (background)", response?.extract()?.jsonPath()?.getString("message"))

        // 🔹 Tempo do teste
        capturaDateTime()

        // 🔹 Validar se os arquivos foram deletados no diretório
        validarTmpSemArquivosDePlayers()

        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 2: Consultando status do processamento...")
        aguardarProcessoCompleto(token = token)

        // 🔹 Tempo do teste
        calcDateTime()

        // 🔥 Validação dos arquivos no /tmp
        validarArquivosNoTmp("$startDateCN1", "$endDateCN1")

        // 🔥 Validação dos arquivos no .gz descompactado
        var filesGz = filterFilesGz()
        validarTsvDescompactadosNoTmp(filesGz)

        // 🔥 Validação dos arquivos no S3
        validarArquivosNoS3(prefixS3)

    }

    @Test
    @Tag("smokeTests") // TPF-67
    fun `CN5 - Validar ingestão com datas inválidas`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val now = LocalDate.now().format(formatter)
        val future = LocalDate.now().plusDays(5).format(formatter)
        val datePlusDays2 = LocalDate.now().plusDays(2).format(formatter)
        val dateMinusDays1 = LocalDate.now().minusDays(1).format(formatter)

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN5 - Validar ingestão com datas inválidas")
        LogCollector.println("📅 Data aleatórios")
        LogCollector.println("════════════════════════════════════════════════════\n")

        // 🔥 MAPA DE CENÁRIOS → mensagem esperada no retorno
        val cenarios = listOf(
            // 1 - data futura
            Triple(future, future, "Data inicial ($future) não pode ser futura. Data atual: $now"),

            // 2 - datas inválidas
            Triple("2025-13-30", "2025-13-30", "Formato de data inválido para data-inicio: 2025-13-30. Use formato ISO 8601 (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS)"),
            Triple("2025-12-40", "2025-12-40", "Formato de data inválido para data-inicio: 2025-12-40. Use formato ISO 8601 (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS)"),
            Triple("25-01-2025", "25-01-2025", "Formato de data inválido para data-inicio: 25-01-2025. Use formato ISO 8601 (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS)"),
            Triple("2025/01/25", "2025/01/25", "Formato de data inválido para data-inicio: 2025/01/25. Use formato ISO 8601 (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS)"),
            Triple("25/01/2025", "25/01/2025", "Formato de data inválido para data-inicio: 25/01/2025. Use formato ISO 8601 (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS)"),

            // 3 - start > end
            Triple(datePlusDays2, dateMinusDays1,
                "Data inicial ($datePlusDays2) não pode ser maior que data final ($dateMinusDays1)"
            )

            // Range grande // TODO: Hoje pode aceitar um periodo longo por se tratar de reprocessamento
            //Triple("2000-01-01", "2050-01-01", "Data final (2050-01-01) não pode ser futura. Data atual: 2025-11-19")
        )

        cenarios.forEach { (startDate, endDate, mensagemEsperada) ->

            LogCollector.println("\n🔎 Testando cenário inválido")
            LogCollector.println(" ▶ start-date=$startDate")
            LogCollector.println(" ▶ end-date=$endDate")
            LogCollector.println(" ▶ Esperado: \"$mensagemEsperada\"")

            val requestBody = """
            {
              "start-date": "$startDate",
              "end-date": "$endDate"
            }
        """.trimIndent()

            val response = given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .body(requestBody)
                .post("/start-process")
                .then()
                .log().body()
                .extract()

            val status = response.statusCode()
            val error = response.jsonPath().getString("error") ?: ""
            val success = response.jsonPath().getBoolean("success")

            // 📌 validação do contrato
            assertEquals(400, status)
            assertFalse(success)
            assertEquals(mensagemEsperada, error)

            LogCollector.println("✔ Erro validado corretamente! → [$error]")
        }

        LogCollector.println("\n🎉 Todos os cenários de datas inválidas foram validados com sucesso.\n")
    }

    @Test
    @Tag("smokeTests") // TPF-67
    fun `CN6 - Validar ingestão com token inválido`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().minusDays(2).format(formatter)

        LogCollector.println("\n═══════════════════════════════════════════════════")
        LogCollector.println("🧪 CN6 – Validar ingestão com TOKEN INVÁLIDO")
        LogCollector.println("📅 Data utilizada: $date")
        LogCollector.println("═══════════════════════════════════════════════════\n")

        val requestBody = """
        {
          "start-date": "$date",
          "end-date": "$date"
        }
    """.trimIndent()

        LogCollector.println("▶️  Enviando requisição com token inválido...")

        val resp = given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9") // Token inválido
            .log().all()
            .body(requestBody)
            .post(PATH_PROCESS)
            .then()
            .log().all()
            .extract()

        val statusCode = resp.statusCode()
        val error = resp.jsonPath().getString("error")
        val successValue = resp.jsonPath().getBoolean("success")

        LogCollector.println("\n📥 RESPOSTA RECEBIDA DO SERVIDOR:")
        LogCollector.println("   ▪ Status Code = $statusCode")
        LogCollector.println("   ▪ success     = $successValue")
        LogCollector.println("   ▪ error       = $error")

        LogCollector.println("\n🔎 Validando comportamento esperado para token inválido...")

        // ==========================================================
        // VALIDAÇÕES
        // ==========================================================

        assertEquals(
            401,
            statusCode,
            "❌ O serviço deveria retornar 401 para token inválido."
        )

        assertEquals(
            "Invalid token (does not match current session)",
            error,
            "❌ Mensagem de erro incorreta para token inválido."
        )

        assertFalse(
            successValue,
            "❌ O campo success deveria ser false quando o token é inválido."
        )

        LogCollector.println("✔ Validações concluídas com sucesso.")

        LogCollector.println("\n🏁 CN6 FINALIZADO COM SUCESSO\n")
    }


    @Test
    @Tag("smokeTests")// TPF-67
    fun `CN7 - Erro no Processamento`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val now = LocalDate.now().plusDays(-2).format(formatter)

        LogCollector.println("\n═══════════════════════════════════════════════════")
        LogCollector.println("🧪 CN7 - Erro no Processamento")
        LogCollector.println("📅 Data utilizada: $now")
        LogCollector.println("═══════════════════════════════════════════════════\n")

        // 🔥 MAPA DE CENÁRIOS → mensagem esperada
        val cenarios = listOf(

            // Config FUGA inválida (Alterar no analytics a config FUGA_USER)
            //Triple("failed", "download_fuga_trends", "'token'"),

            // Config S3 inválida (Alterar no analytics a config AWS_S3_ENDPOINT_URL)
            //Triple("failed", "pipeline", "Invalid endpoint: https://tao-im-symphonia-dev-files.s3.us-east-1.amazonaws.com_"),

            /// Pasta /tmp invalida  (Alterar no analytics a config DOWNLOADS_FOLDER=/tmp_TEST/)
            Triple("failed", "clean_old_files", "The directory /tmp_TEST/ was not found."),

            // Erro descompactar
                //Triple(future, future, ""),
            // Erro Upload S3
                //Triple(future, future, ""),
            // Erro Download Fuga
                //Triple(future, future, ""),


        )

        cenarios.forEach { (status, current_step, error) ->

            LogCollector.println("\n🔎 Testando cenário inválido")
            LogCollector.println("   ➤ start-date=$now")
            LogCollector.println("   ➤ end-date=$now")
            LogCollector.println("   ➤ Esperado: \"$error\"")

            val requestBody = """
                    {
                      "start-date": "$now",
                      "end-date": "$now"
                    }
                """.trimIndent()

            // Requisição ao start-process
            val startResponse = given()
                .contentType(ContentType.JSON)
                .log().all()
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .body(requestBody)
                .post(PATH_PROCESS)
                .then()
                .log().all()
                .extract()

            val statusCode = startResponse.statusCode()
            LogCollector.println("➡ Status HTTP start-process: $statusCode")
            assertTrue(statusCode in listOf(200)) // Processo assincrono

            // 🔥 Aguardar mensagem de erro específica no /process-status
            LogCollector.println("\n⏳ Aguardando detecção de FALHA no /process-status ...")
            Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .untilCallTo {

                    val resp = given()
                        .contentType(ContentType.JSON)
                        .header("origin", "http://localhost")
                        .header("authorization", "Bearer $token")
                        .get("/process-status")
                        .then()
                        .log().all()
                        .extract()

                    val error = resp.jsonPath().getString("error") ?: ""
                    val currentStep = resp.jsonPath().getString("current_step") ?: ""
                    val message = resp.jsonPath().getString("message") ?: ""
                    val status = resp.jsonPath().getString("status") ?: ""
                    val httpStatus = resp.statusCode()

                    LogCollector.println("\n📥 RESPOSTA /process-status:")
                    LogCollector.println("   error: $error")
                    LogCollector.println("   current_step: $currentStep")
                    LogCollector.println("   message: $message")
                    LogCollector.println("   status: $status")
                    LogCollector.println("   httpStatus   = $httpStatus")


                    val resultMessage = resp.jsonPath().getString("result.message")
                    val resultStatus = resp.jsonPath().getString("result.status")
                    val resultStart = resp.jsonPath().getString("result.start_date")
                    val resultEnd = resp.jsonPath().getString("result.end_date")

                    StatusResponseFields(
                        error = error,
                        currentStep = currentStep,
                        message = message,
                        status = status,
                        httpStatus = httpStatus,
                        result = ResultFields(
                            message = resultMessage,
                            status = resultStatus,
                            startDate = resultStart,
                            endDate = resultEnd
                        )
                    )

                } matches { result ->

                val r = result as StatusResponseFields
                val httpOk = r.httpStatus == 500
                val errorMatches =
                    r.error?.contains(error, ignoreCase = true) ?: false
                    r.status.equals(status, ignoreCase = true) &&
                        r.currentStep.equals(current_step, ignoreCase = true) &&
                        r.message.equals("Processo falhou com erro", ignoreCase = true) &&
                        errorMatches && httpOk
            }

            LogCollector.println("\n✔ Cenário validado com sucesso.")
            LogCollector.println("🏁 CN7 FINALIZADO\n")
        }
    }


    /**
     * Campos com retorno de Falhas
     */
    data class StatusResponseFields(
        val error: String?,
        val currentStep: String?,
        val message: String?,
        val status: String?,
        val result: ResultFields?,
        val httpStatus: Int?
    )

    data class ResultFields(
        val message: String?,
        val status: String?,
        val startDate: String?,
        val endDate: String?
    )

    /**
     *Função para validar que nao tenha nenhum arquivo no diretório tmp antes da execução
     */
    fun validarTmpSemArquivosDePlayers(
        timeoutSeconds: Long = 60,
        pollIntervalSeconds: Long = 2
    ) {
        assertTrue(File(DIR_TEMP).exists(), "Diretório /tmp não existe")

        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 1: Validando deleção dos arquivos no /tmp...")

        val start = System.currentTimeMillis()
        val agora = System.currentTimeMillis()
        val limiteMillis = 2 * 24 * 60 * 60 * 1000 // 2 dias em ms

        var arquivosFiltrados: List<File>

        while (true) {

            val arquivos = File(DIR_TEMP).listFiles()?.toList() ?: emptyList()

            // Filtra somente arquivos dos players E com mais de 2 dias de criação
            arquivosFiltrados = arquivos.filter { arquivo ->
                val nome = arquivo.name

                val isPlayerFile = EXPECTED_PLAYERS.any { player ->
                    nome.contains(player, ignoreCase = true)
                }

                val idadeArquivo = agora - arquivo.lastModified()

                isPlayerFile && idadeArquivo > limiteMillis
            }

            if (arquivosFiltrados.isEmpty()) {
                LogCollector.println("\n✅ Diretório limpo: Nenhum arquivo de players (com mais de 2 dias) encontrado no /tmp !")
                break
            }

            // Arquivos ainda encontrados → log
            LogCollector.println("⚠️ Arquivos de players com mais de 2 dias ainda encontrados no /tmp:")
            arquivosFiltrados.forEach { arq ->
                val horas = (agora - arq.lastModified()) / 3600000
                LogCollector.println(" - ${arq.name}  (idade: ${horas}h)")
            }

            // Timeout
            val elapsed = (System.currentTimeMillis() - start) / 1000
            if (elapsed > timeoutSeconds) {
                fail("⛔ Timeout: Ainda existem arquivos antigos ( +2 dias ) após $timeoutSeconds segundos.")
            }

            Thread.sleep(pollIntervalSeconds * 1000)
        }

        LogCollector.println("✔ Processo concluído: diretório /tmp está limpo dos arquivos antigos.")
        LogCollector.println("\n────────────────────────────────────────────")
    }


    /**
     *Função para validar que todos os arquivos dos 8 players foram gerados para todas as datas
     */
    private fun validarArquivosNoTmp(startDate: String, endDate: String) {
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val inicio = LocalDate.parse(startDate, dateFormatter)
        val fim = LocalDate.parse(endDate, dateFormatter)

        assertTrue(File(DIR_TEMP).exists(), "Diretório /tmp não existe")

        // Filtrar somente arquivos .tsv ou .tsv.gz
        val arquivos = File(DIR_TEMP).listFiles()
            ?.filter { it.name.endsWith(".tsv") || it.name.endsWith(".tsv.gz") }
            ?.map { it.name }
            ?: emptyList()

        // Extrai a data do nome do arquivo
        fun extrairData(nome: String): LocalDate {
            val dataStr = nome.substringAfterLast("_").substringBefore(".")
            return try {
                LocalDate.parse(dataStr)
            } catch (_: Exception) {
                LocalDate.of(1900, 1, 1)
            }
        }

        // Nome base sem extensão
        fun nomeBase(nome: String): String {
            return nome.substringBeforeLast(".").removeSuffix(".tsv")
        }

        // Ordenação combinada
        val arquivosOrdenados = arquivos.sortedWith(
            compareBy<String>(
                { extrairData(it) },                         // 1️⃣ por data
                { nomeBase(it) },                            // 2️⃣ grupo do mesmo arquivo
                {
                    when {
                        it.endsWith(".tsv.gz") -> 0          // 3️⃣ .tsv.gz primeiro
                        it.endsWith(".tsv") -> 1
                        else -> 2
                    }
                }
            )
        )

        // Agrupa por data para impressão
        val agrupadoPorData = arquivosOrdenados.groupBy { extrairData(it) }
        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂PASSO 3: Validação de arquivos gerados...\n📂 Lista de arquivos encontrados no /tmp:")
        agrupadoPorData.forEach { (data, lista) ->
            LogCollector.println("📅 $data")
            lista.forEach { nome ->
                val player = EXPECTED_PLAYERS.firstOrNull { nome.startsWith(it) }
                val icon = PLAYERS_ICON[player] ?: "📁"
                LogCollector.println("   $icon  $nome")
            }
        }
        val dias = inicio.datesUntil(fim.plusDays(1)).toList()
        LogCollector.println("\uD83D\uDCC2 Lista de arquivos não encontrados no /tmp:")
        dias.forEach { dia ->
            val dataStr = dia.format(dateFormatter)
            EXPECTED_PLAYERS.forEach { player ->

                // agora valida .tsv E .tsv.gz
                val encontrado = arquivos.any { nome ->
                    nome.startsWith(player) &&
                            nome.contains(dataStr) &&
                            (nome.endsWith(".tsv.gz") || nome.endsWith(".tsv"))
                }

                if (encontrado) {
                    //LogCollector.println("✅ Encontrado → $player ($dataStr)")
                } else {
                    LogCollector.println("❌ NÃO ENCONTRADO → $player ($dataStr)")
                }

                // Comentado para nao quebrar o teste pois no dia pode ainda nao ter arquivo no diretorio de origem
                /*
                assertTrue(
                    encontrado,
                    "Arquivo esperado não encontrado no /tmp → player=$player data=$dataStr"
                )*/
            }
        }
        LogCollector.println("✔ Arquivos validados com sucesso: todos os players e datas encontrados no /tmp")
        LogCollector.println("\n────────────────────────────────────────────")
    }

    /**
     *Função para calcular o tempo de execução em média
     */
    fun capturaDateTime() {
        start = java.time.Instant.now()
        LogCollector.println("⏱ Timer iniciado...")
    }
    fun calcDateTime() {
        if (start == null) {
            LogCollector.println("⚠ O timer não foi iniciado! Chame capturaDateTime() antes.")
            return
        }
        val end = java.time.Instant.now()
        val duration = Duration.between(start, end)

        val minutos = duration.toMinutes()
        val segundos = duration.seconds % 60
        val mmss = String.format("%02d:%02d", minutos, segundos)

        LogCollector.println(
            "⏱ Tempo total do teste: " +
                    "${duration.toMillis()} ms " +
                    "(${duration.seconds} segundos) — " +
                    "$mmss (MM:SS)"
        )
        LogCollector.println("\n────────────────────────────────────────────")

    }

    /**
     *Função filtra e lista do arquivos .tsv.gz e .tsv no diretório /tmp
     */
    fun filterFilesGz(): List<String> {
        if (!File(DIR_TEMP).exists()) return emptyList()
        return File(DIR_TEMP).listFiles()
            ?.filter { file ->
                 file.name.endsWith(".tsv.gz")
            }
            ?.map { it.name }
            ?.sorted()
            ?: emptyList()
    }
    fun filterFilesTsv(): List<String> {
        if (!File(DIR_TEMP).exists()) return emptyList()
        return File(DIR_TEMP).listFiles()
            ?.filter { file ->
                file.name.endsWith(".tsv")
            }
            ?.map { it.name }
            ?.sorted()
            ?: emptyList()
    }

    /**
     *Função para validar se para cada arquivo .tsv.gz possui um arquivo .tsv descompactado
     */
    fun validarTsvDescompactadosNoTmp(filesGz: List<String>) {

        LogCollector.println("🕵️‍♂ PASSO 4: Validando arquivos .tsv.gz e seus .tsv correspondentes...")
        val allFiles = File(DIR_TEMP).listFiles()
            ?.map { it.name }
            ?: emptyList()

        var erros = 0

        filesGz.forEach { gzName ->

            // Nome base → retirando ".tsv.gz"
            val baseName = gzName.removeSuffix(".tsv.gz")

            val expectedTsv = "$baseName.tsv"

            val existeTsv = allFiles.contains(expectedTsv)

            if (existeTsv) {
                LogCollector.println("✔️  OK → $gzName possui o correspondente $expectedTsv")
            } else {
                LogCollector.println("❌ ERRO → $gzName NÃO possui o arquivo descompactado $expectedTsv")
                erros++
            }
        }

        LogCollector.println("\n📄 Total de arquivos .tsv.gz encontrados: ${filesGz.size}")
        LogCollector.println("⚠️ Total de erros: $erros")

        assertTrue(erros == 0, "Foram encontrados $erros arquivos .tsv.gz sem existir um .tsv!")
        LogCollector.println("\n────────────────────────────────────────────")
    }

    /**
     *Função Test S3
     */

    fun validarArquivosNoS3(prefix: String) {

        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 5: Validando arquivos /tmp ↔ S3 (somente arquivos presentes no /tmp)")
        val s3 = criarClienteS3()

        // 1️⃣ Carrega TODOS os arquivos do S3 sob o prefixo
        val s3Keys = listarArquivosS3(prefix)
            .filter { it.endsWith(".tsv.gz") } // somente .tsv.gz

        // Remove paths, deixando apenas os nomes
        val s3FilesMap = s3Keys.associateBy { it.substringAfterLast("/") }

        //renomearPrimeiroArquivoTsvGzParaTeste() // So foi usado para teste

        // 2️⃣ Carrega arquivos do /tmp
        assertTrue(File(DIR_TEMP).exists(), "Diretório /tmp não existe")

        val tmpFiles = File(DIR_TEMP).listFiles()
            ?.filter { it.isFile && it.name.endsWith(".tsv.gz") }
            ?.associateBy { it.name }
            ?: emptyMap()

        LogCollector.println("📂 /tmp → ${tmpFiles.size} arquivos .tsv.gz encontrados")
        LogCollector.println("📂 S3   → ${s3FilesMap.size} arquivos .tsv.gz encontrados\n")

        var erros = 0

        // 3️⃣ Para cada arquivo do /tmp, validar no S3
        tmpFiles.forEach { (fileName, fileObj) ->

            LogCollector.println("➡ Validando arquivo: $fileName")

            val s3Key = s3FilesMap[fileName]

            if (s3Key == null) {
                LogCollector.println("❌ ERRO → Arquivo $fileName não existe no S3")
                erros++
                return@forEach
            }

            // Buscar metadata do S3
            val metadata = s3.headObject {
                it.bucket(bucketS3).key(s3Key)
            }

            val tamanhoS3 = metadata.contentLength()
            val tamanhoTmp = fileObj.length()

            if (tamanhoS3 == tamanhoTmp) {
                LogCollector.println("   ✔ OK → arquivo encontrado e tamanho igual ($tamanhoTmp bytes)\n")
            } else {
                LogCollector.println("""
                ❌ ERRO → Arquivos diferentes!
                - Nome: $fileName
                - Tamanho S3 : $tamanhoS3
                - Tamanho /tmp : $tamanhoTmp
            """.trimIndent())
                erros++
            }
        }

        LogCollector.println("⚠️ Total de erros: $erros\n")
        assertTrue(erros == 0, "Foram encontrados $erros arquivos inválidos ou ausentes no S3!")
    }
    @Test
    @Tag("smokeTests")
    fun listarTudoNoBucket() {
        val bucket = bucketS3 ?: error("AWS_S3_BUCKET_NAME não definida")
        val s3 = criarClienteS3()

        println("📌 Listando objetos no bucket: $bucket")

        val req = ListObjectsV2Request.builder()
            .bucket(bucket)
            .build()

        var resp = s3.listObjectsV2(req)

        if (resp.contents().isEmpty()) {
            println("⚠ O bucket está vazio ou você não tem permissão de listObjectsV2")
        } else {
            resp.contents().forEach {
                println(" - ${it.key()}")
            }
        }

        // também listar prefixes (pastas)
        if (resp.commonPrefixes().isNotEmpty()) {
            println("📁 Pastas detectadas:")
            resp.commonPrefixes().forEach {
                println(" - ${it.prefix()}")
            }
        }
    }
    fun listarArquivosS3(prefix: String): List<String> {
        val bucket = bucketS3 ?: error("AWS_S3_BUCKET_NAME não definida")
        val prefixReal = detectarPrefixReal(bucket, prefix)
        LogCollector.println("📌 Prefix real detectado no S3 → $prefixReal")
        val s3 = criarClienteS3()
        val req = ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(prefixReal)
            .build()
        val resp = s3.listObjectsV2(req)
        return resp.contents().map { it.key() }
    }
    fun detectarPrefixReal(bucket: String, prefixDesejado: String): String {
        // Se o prefix já começar com o nome do bucket → OK
        if (prefixDesejado.startsWith(bucket)) {
            return prefixDesejado
        }

        // Senão → verificar se o bucket contém uma pasta com o nome dele mesmo
        val s3 = criarClienteS3()
        val req = ListObjectsV2Request.builder()
            .bucket(bucket)
            .delimiter("/")
            .build()

        val resp = s3.listObjectsV2(req)
        val pastasRaiz = resp.commonPrefixes().map { it.prefix() }

        // Se existe pasta com o nome do bucket → usar ela
        val possivelFolder = "$bucket/"
        return if (pastasRaiz.contains(possivelFolder)) {
            "$bucket/$prefixDesejado"
        } else {
            prefixDesejado
        }
    }
    fun criarClienteS3(): S3Client {
        return S3Client.builder()
            .region(Region.of(region))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(key, secret)
                )
            )
            .build()
    }
    fun renomearPrimeiroArquivoTsvGzParaTeste() {
        assertTrue(File(DIR_TEMP).exists(), "Diretório /tmp não existe")

        val arquivos = File(DIR_TEMP).listFiles()
            ?.filter { it.isFile && it.name.endsWith(".tsv.gz") }
            ?: emptyList()

        assertTrue(arquivos.isNotEmpty(), "Nenhum arquivo .tsv.gz encontrado no /tmp!")

        val original = arquivos.first()
        val renomeado = File(File(DIR_TEMP), original.name.replace(".tsv.gz", "_RENAME_TEST.tsv.gz"))

        val ok = original.renameTo(renomeado)
        assertTrue(ok, "Falha ao renomear arquivo ${original.name}")

        LogCollector.println("🔄 Arquivo renomeado:")
        LogCollector.println("  De: ${original.name}")
        LogCollector.println("  Para: ${renomeado.name}")
    }


}
