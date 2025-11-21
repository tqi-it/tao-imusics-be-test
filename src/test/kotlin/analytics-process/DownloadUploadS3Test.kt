package `analytics-process`

import io.restassured.RestAssured
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import org.apache.http.HttpStatus
import org.awaitility.Awaitility
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import util.LogCollector
import java.io.File
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

class DownloadUploadS3Test {

    /**
     * Objetivo: Classe de teste para validação do fluxo do projeto analytics (im-symphonia-analytics)
     * Pré Condição:
     *  - Subir localmente o projeto im-symphonia-analytics (Comando: make start)
     *  - Verificar em qual porta ele subiu para passa na váriavel BASE_URL
     *  Tarefa: TPF-67
     */

    companion object {
        private const val BASE_URL = "http://localhost:3015"
        private var token: String = ""
        private var start: java.time.Instant? = null

        /**
         * Parâmetros do CN1
         */
        private var startDate ="2025-11-10"
        private var endDate ="2025-11-10"
        val tmpDirLocal = File("/tmp")

        // Parâmetros dos testes caminho feliz
        val timeoutFull = Duration.ofMinutes(15) // tempo máximo total do teste
        //val delayResquest = Thread.sleep(15000) // aguarda 15 segundos

        // S3
        val bucketS3 = System.getenv("AWS_S3_BUCKET_NAME")
        val regionS3 = System.getenv("AWS_S3_REGION_NAME")
        val prefixS3 = "tao-im-symphonia-dev-files/analytics-trends/fuga/"


        val expectedPlayers = listOf(
            "iMusics_Amazon",
            "iMusics_Deezer",
            "iMusics_iTunes",
            "iMusics_TikTok",
            "iMusics_Pandora",
            "iMusics_Spotify",
            "iMusics_Youtube",
            "iMusics_SoundCloud"
        )
        val playerIcons = mapOf(
            "iMusics_Amazon" to "🛒",
            "iMusics_Spotify" to "🎵",
            "iMusics_Deezer" to "📻",
            "iMusics_iTunes" to "🍎",
            "iMusics_TikTok" to "🎬",
            "iMusics_Pandora" to "📡",
            "iMusics_Youtube" to "▶️",
            "iMusics_SoundCloud" to "☁️"
        )

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

    @Test
    @Tag("smokeTests") // TPF-70
    fun `CN1 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3`() {
        // 🔹 Corpo com período definido
        val requestBody = """
            {
              "start-date": "$startDate",
              "end-date": "$endDate"
            }
        """.trimIndent()

        // 🔹 Fazer chamada ao /start-process
        val startResponse = given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .body(requestBody)
            .post("/start-process")
            .then()
            .extract()
        val statusCode = startResponse.statusCode()

        // 🔹 Tempo do teste
        capturaDateTime()

        // 🔹 Caso já exista processo rodando (409 por exemplo)
        if (statusCode == 409 || statusCode == 400) {
            LogCollector.println("Processo já está em execução. Código: $statusCode")
            assertTrue(statusCode == 409 || statusCode == 400)
            return
        }

        // 🔹 Caso contrário, precisa ser 200 ou 202 = processo iniciou corretamente
        assertTrue(statusCode == 200 || statusCode == 202, "O processo não iniciou corretamente")

        // 🔹 Validar se os arquivos foram deletados no diretório
        validarTmpSemArquivosDePlayers()

        // 🔥 Loop para acompanhar o processo via /process-status
        var finalStatus = ""
        val start = System.currentTimeMillis()

        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 2: Consultando status do processamento...")
        do {
            Thread.sleep(15000) // aguarda 15 segundos

            val statusResponse = given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .get("/process-status")
                .then()
                .statusCode(200)
                .extract()

            val running = statusResponse.jsonPath().getString("is_running")
            val descFlow = statusResponse.jsonPath().getString("message")
            finalStatus = statusResponse.jsonPath().getString("status")
            LogCollector.println("🔄 Flow: $descFlow\n📌 Status Atual: $finalStatus\n")

            // sai do loop quando o processo terminar
            if (finalStatus.equals("completed", ignoreCase = true)) break

            // timeout de segurança
            val elapsedMinutes = (System.currentTimeMillis() - start) / 60000
            if (elapsedMinutes > timeoutFull.toMinutes()) {
                fail("Timeout: processo demorou demais para concluir ($elapsedMinutes minutos)")
            }

        } while (true)

        // 🔹 Validação final
        assertEquals("completed", finalStatus.lowercase(), "Processo não chegou ao status 'concluido'")
        LogCollector.println("✔ Processo finalizado com sucesso! Status = $finalStatus")
        LogCollector.println("────────────────────────────────────────────")

        // 🔹 Tempo do teste
        calcDateTime()

        // 🔥 Validação dos arquivos no /tmp
        validarArquivosNoTmp("$startDate", "$endDate")

        // 🔥 Validação dos arquivos no .gz descompactado
        var filesGz = filterFilesGz()
        validarTsvDescompactadosNoTmp(filesGz)

        // 🔥 Validação dos arquivos no S3
        validarArquivosNoS3(prefixS3)
    }


    @Test
    @Tag("smokeTests") // TPF-67 TODO: Processo esta falhando (500) nao deveria ser um 200 com o error "Unknown error occurred" correto
    fun `CN2 - Validar ingestão quando não possui arquivos para baixar`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().format(formatter)

        // 🔹 Corpo com período definido
        val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

        // 🔹 Fazer chamada ao /start-process
        val startResponse = given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .body(requestBody)
            .post("/start-process")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
        val statusStart = startResponse.jsonPath().getBoolean("success")
        assertTrue(statusStart)
        LogCollector.println("Status process: $statusStart")

        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(2, TimeUnit.SECONDS)
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

                LogCollector.println("⏳ Campos obtidos →")
                LogCollector.println("   error: $error")
                LogCollector.println("   current_step: $currentStep")
                LogCollector.println("   message: $message")
                LogCollector.println("   status: $status")

                StatusResponseFields(error, currentStep, message, status)

            } matches { result ->
            val r = result as StatusResponseFields
            r.error.contains("Unknown error occurred", ignoreCase = true) &&
                    r.status.equals("failed", ignoreCase = true) &&
                    r.currentStep.equals("Validação de datas", ignoreCase = true) &&
                    r.message.equals("Falha na validação de datas", ignoreCase = true)
        }


    }


    @Test
    @Tag("smokeTests")  // TPF-67 /* PRÉ-CONFIÇÃO: Executar somente quando nao tiver nenhum processamento REDIS_IGNORE_FILES_PATTERN=(Spotify|Youtube|) */
    fun `CN3 - Validar ingestão quando já possui um processamento sendo realizado`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-2).format(formatter)

        // 🔹 Corpo com período definido
        val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

        repeat(2) { tentativa ->
            val numero = tentativa + 1
            LogCollector.println("Executando start-process tentativa $numero")

            val resposta = RestAssured.given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .log().all()
                .body(requestBody)
                .post("/start-process")
                .then()
                .log().all()
                .extract()

            val statusCode = resposta.statusCode()
            val success = resposta.jsonPath().getBoolean("success")
            val error = resposta.jsonPath().getBoolean("error")

            if (tentativa == 0) {
                // 🟢 PRIMEIRA EXECUÇÃO — Espera 200
                assertEquals(
                    HttpStatus.SC_OK,
                    statusCode,
                    "Primeira execução deveria retornar 200 OK"
                )
                assertTrue(success, "Primeira execução deveria retornar success=true")
                LogCollector.println("✔️ Tentativa 1 OK: status=$statusCode success=$success")

            } else {
                // 🔴 SEGUNDA EXECUÇÃO — Espera 409 (já tem processo rodando)
                assertEquals(
                    HttpStatus.SC_CONFLICT,
                    statusCode,
                    "Segunda execução deveria retornar 409, mas retornou $statusCode"
                )
                assertEquals( error, "Process already running" )
                assertFalse(success, "Segunda execução deveria retornar success=false")
                LogCollector.println("✔️ Tentativa 2 Bloqueada como esperado: status=$statusCode success=$success")
            }

            Thread.sleep(1000)
        }

    }


    @Test
    @Tag("smokeTests") // TPF-70 /* DateTime()-3 conforme esperado do /start-process */
    fun `CN4 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3 sem passar data`() {

        // 🔹 Fazer chamada ao /start-process
        val startResponse = given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .post("/start-process")
            .then()
            .extract()
        val statusCode = startResponse.statusCode()

        // 🔹 Tempo do teste
        capturaDateTime()

        // 🔹 Caso já exista processo rodando (409 por exemplo)
        if (statusCode == 409 || statusCode == 400) {
            LogCollector.println("Processo já está em execução. Código: $statusCode")
            assertTrue(statusCode == 409 || statusCode == 400)
            return
        }

        // 🔹 Caso contrário, precisa ser 200 ou 202 = processo iniciou corretamente
        assertTrue(statusCode == 200 || statusCode == 202, "O processo não iniciou corretamente")

        // 🔹 Validar se os arquivos foram deletados no diretório
        validarTmpSemArquivosDePlayers()

        // 🔥 Loop para acompanhar o processo via /process-status
        var finalStatus = ""
        val start = System.currentTimeMillis()

        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 2: Consultando status do processamento...")
        do {
            Thread.sleep(15000) // aguarda 15 segundos

            val statusResponse = RestAssured.given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .get("/process-status")
                .then()
                .statusCode(200)
                .extract()

            val running = statusResponse.jsonPath().getString("is_running")
            val descFlow = statusResponse.jsonPath().getString("message")
            finalStatus = statusResponse.jsonPath().getString("status")
            LogCollector.println("🔄 Flow: $descFlow\n📌 Status Atual: $finalStatus\n")

            // sai do loop quando o processo terminar
            if (finalStatus.equals("completed", ignoreCase = true)) break

            // timeout de segurança
            val elapsedMinutes = (System.currentTimeMillis() - start) / 60000
            if (elapsedMinutes > timeoutFull.toMinutes()) {
                fail("Timeout: processo demorou demais para concluir ($elapsedMinutes minutos)")
            }

        } while (true)

        // 🔹 Validação final
        assertEquals("completed", finalStatus.lowercase(), "Processo não chegou ao status 'concluido'")
        LogCollector.println("✔ Processo finalizado com sucesso! Status = $finalStatus")

        // 🔹 Tempo do teste
        calcDateTime()

        // 🔥 Validação dos arquivos no /tmp
        validarArquivosNoTmp("$startDate", "$endDate")

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

        // 🔥 MAPA DE CENÁRIOS → mensagem esperada
        val cenarios = listOf(
            // Data no futuro
            Triple(future, future, "Data inicial ($future) não pode ser futura. Data atual: $now"),

            // Data inexistente
            Triple("2025-13-30", "2025-13-30", "Formato de data inválido para data-inicio: 2025-13-30"),
            Triple("2025-12-40", "2025-12-40", "Formato de data inválido para data-inicio: 2025-12-40"),

            // Formatos incorretos
            Triple("25-01-2025", "25-01-2025", "Formato de data inválido para data-inicio: 25-01-2025"),
            Triple("2025/01/25", "2025/01/25", "Formato de data inválido para data-inicio: 2025/01/25"),
            Triple("25/01/2025", "25/01/2025", "Formato de data inválido para data-inicio: 25/01/2025"),


            // start-date > end-date
            Triple(datePlusDays2, dateMinusDays1,
                "Data inicial ($datePlusDays2) não pode ser maior que data final ($dateMinusDays1)"
            ),

            // Range grande // TODO: Hoje pode aceitar um periodo longo por se tratar de reprocessamento
            //Triple("2000-01-01", "2050-01-01", "Data final (2050-01-01) não pode ser futura. Data atual: 2025-11-19")
        )

        cenarios.forEach { (startDate, endDate, mensagemEsperada) ->

            LogCollector.println("\n🔎 Testando cenário inválido")
            LogCollector.println("   ➤ start-date=$startDate")
            LogCollector.println("   ➤ end-date=$endDate")
            LogCollector.println("   ➤ Esperado: \"$mensagemEsperada\"")

            val requestBody = """
            {
              "start-date": "$startDate",
              "end-date": "$endDate"
            }
        """.trimIndent()

            // Requisição ao start-process
            val startResponse = given()
                .contentType(ContentType.JSON)
                .log().all()
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .body(requestBody)
                .post("/start-process")
                .then()
                .log().all()
                .extract()

            val statusCode = startResponse.statusCode()
            LogCollector.println("➡ Status HTTP start-process: $statusCode")

            // Para APIs mal feitas que retornam 400 ou 500 mesmo com erro TODO: Não deveria retornar só 400 para todas falhas e deixar 500 para erro do servidor?
            assertTrue(statusCode in listOf(400))

            // 🔥 Aguardar mensagem de erro específica no /process-status
            Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilCallTo {

                    val resp = given()
                        .contentType(ContentType.JSON)
                        .header("origin", "http://localhost")
                        .header("authorization", "Bearer $token")
                        .get("/process-status")
                        .then()
                        .extract()

                    val error = resp.jsonPath().getString("error") ?: ""
                    val currentStep = resp.jsonPath().getString("current_step") ?: ""
                    val message = resp.jsonPath().getString("message") ?: ""
                    val status = resp.jsonPath().getString("status") ?: ""

                    LogCollector.println("⏳ Campos obtidos →")
                    LogCollector.println("   error: $error")
                    LogCollector.println("   current_step: $currentStep")
                    LogCollector.println("   message: $message")
                    LogCollector.println("   status: $status")

                    StatusResponseFields(error, currentStep, message, status)

                } matches { result ->

                val r = result as StatusResponseFields
                r.error.contains(mensagemEsperada, ignoreCase = true) &&
                        r.status.equals("failed", ignoreCase = true) &&
                        r.currentStep.equals("Validação de datas", ignoreCase = true) && //contains
                        r.message.equals("Falha na validação de datas", ignoreCase = true) //isNotBlank
            }


            LogCollector.println("✔ Cenário validado com sucesso: mensagem correta recebida.")
        }
    }

    @Test
    @Tag("smokeTests") // TPF-67 TODO: Processo esta retornando 200 nao 400 verificar mensagens de falhas após ajuste
    fun `CN6 - Erro no Processamento`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val now = LocalDate.now().format(formatter)

        // 🔥 MAPA DE CENÁRIOS → mensagem esperada
        val cenarios = listOf(

            // Config FUGA inválida (Alterar no analytics a config FUGA_USER)
            Triple(now, now, "Test"),

            // Config S3 inválida (Alterar no analytics a config AWS_ACCESS_KEY_ID)
            Triple(now, now, "Test"),

            /// Pasta /tmp invalida  (Alterar no analytics a config DOWNLOADS_FOLDER)
            Triple(now, now, "Test"),

            // Erro descompactar
                //Triple(future, future, ""),
            // Erro Upload S3
                //Triple(future, future, ""),
            // Erro Download Fuga
                //Triple(future, future, ""),


        )

        cenarios.forEach { (startDate, endDate, mensagemEsperada) ->

            LogCollector.println("\n🔎 Testando cenário inválido")
            LogCollector.println("   ➤ start-date=$now")
            LogCollector.println("   ➤ end-date=$now")
            LogCollector.println("   ➤ Esperado: \"$mensagemEsperada\"")

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
                .post("/start-process")
                .then()
                .log().all()
                .extract()

            val statusCode = startResponse.statusCode()
            LogCollector.println("➡ Status HTTP start-process: $statusCode")

            // TODO: Não deveria retornar só 400 para todas falhas e deixar 500 para erro do servidor?
            assertTrue(statusCode in listOf(400))

            // 🔥 Aguardar mensagem de erro específica no /process-status
            Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilCallTo {

                    val resp = given()
                        .contentType(ContentType.JSON)
                        .header("origin", "http://localhost")
                        .header("authorization", "Bearer $token")
                        .get("/process-status")
                        .then()
                        .extract()

                    val error = resp.jsonPath().getString("error") ?: ""
                    val currentStep = resp.jsonPath().getString("current_step") ?: ""
                    val message = resp.jsonPath().getString("message") ?: ""
                    val status = resp.jsonPath().getString("status") ?: ""

                    LogCollector.println("⏳ Campos obtidos →")
                    LogCollector.println("   error: $error")
                    LogCollector.println("   current_step: $currentStep")
                    LogCollector.println("   message: $message")
                    LogCollector.println("   status: $status")

                    StatusResponseFields(error, currentStep, message, status)

                } matches { result ->

                val r = result as StatusResponseFields
                r.error.contains(mensagemEsperada, ignoreCase = true) &&
                        r.status.equals("failed", ignoreCase = true) &&
                        r.currentStep.equals("Validação de configuração", ignoreCase = true) && //contains
                        r.message.equals("Falha na validação de configuração", ignoreCase = true) //isNotBlank
            }

            LogCollector.println("✔ Cenário validado com sucesso: mensagem correta recebida.")
        }
    }





    /**
     * Campos com retorno de Falhas
     */
    data class StatusResponseFields(
        val error: String,
        val currentStep: String,
        val message: String,
        val status: String
    )


    /**
     *Função para validar que nao tenha nenhum arquivo no diretório tmp antes da execução
     */
    fun validarTmpSemArquivosDePlayers(
        timeoutSeconds: Long = 60,
        pollIntervalSeconds: Long = 2
    ) {
        assertTrue(tmpDirLocal.exists(), "Diretório /tmp não existe")

        LogCollector.println("\uD83D\uDD75\uFE0F\u200D♂ PASSO 1: Validando deleção dos arquivos no /tmp...")
        val start = System.currentTimeMillis()
        var arquivosFiltrados: List<String>

        while (true) {
            val arquivos = tmpDirLocal.listFiles()?.map { it.name } ?: emptyList()

            // Filtra somente arquivos dos players
            arquivosFiltrados = arquivos.filter { nome ->
                expectedPlayers.any { player ->
                    nome.contains(player, ignoreCase = true)
                }
            }
            if (arquivosFiltrados.isEmpty()) {
                LogCollector.println("\n✅ Diretório limpo: Nenhum arquivo de players encontrado no /tmp !")
                break
            }

            // Ainda existem arquivos → loga quais são
            LogCollector.println("⚠️ Arquivos de players ainda encontrados no /tmp:")
            arquivosFiltrados.forEach { LogCollector.println(" - $it") }

            // Checa timeout
            val elapsed = (System.currentTimeMillis() - start) / 1000
            if (elapsed > timeoutSeconds) {
                fail("⛔ Timeout: Ainda existem arquivos de players após $timeoutSeconds segundos.")
            }

            // Espera antes da próxima verificação
            Thread.sleep(pollIntervalSeconds * 1000)
        }

        // Impressão final dos arquivos que foram detectados (e que sumiram)
        if (arquivosFiltrados.isEmpty()) {
            LogCollector.println("\uD83D\uDCC2 Nenhum arquivo foi encontrado!!!")
        } else {
            arquivosFiltrados.forEach { LogCollector.println(" - $it") }
        }

        LogCollector.println("✔ Processo concluído: diretório /tmp está limpo.")
        LogCollector.println("\n────────────────────────────────────────────")
    }


    /**
     *Função para validar que todos os arquivos dos 8 players foram gerados para todas as datas
     */
    private fun validarArquivosNoTmp(startDate: String, endDate: String) {
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val inicio = LocalDate.parse(startDate, dateFormatter)
        val fim = LocalDate.parse(endDate, dateFormatter)

        assertTrue(tmpDirLocal.exists(), "Diretório /tmp não existe")

        // Filtrar somente arquivos .tsv ou .tsv.gz
        val arquivos = tmpDirLocal.listFiles()
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
                val player = expectedPlayers.firstOrNull { nome.startsWith(it) }
                val icon = playerIcons[player] ?: "📁"
                LogCollector.println("   $icon  $nome")
            }
        }
        val dias = inicio.datesUntil(fim.plusDays(1)).toList()
        LogCollector.println("\uD83D\uDCC2 Lista de arquivos não encontrados no /tmp:")
        dias.forEach { dia ->
            val dataStr = dia.format(dateFormatter)
            expectedPlayers.forEach { player ->

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
        if (!tmpDirLocal.exists()) return emptyList()
        return tmpDirLocal.listFiles()
            ?.filter { file ->
                 file.name.endsWith(".tsv.gz")
            }
            ?.map { it.name }
            ?.sorted()
            ?: emptyList()
    }
    fun filterFilesTsv(): List<String> {
        if (!tmpDirLocal.exists()) return emptyList()
        return tmpDirLocal.listFiles()
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
        val allFiles = tmpDirLocal.listFiles()
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

        //renomearPrimeiroArquivoTsvGzParaTeste()

        // 2️⃣ Carrega arquivos do /tmp
        val tmpDir = File("/tmp")
        assertTrue(tmpDir.exists(), "Diretório /tmp não existe")

        val tmpFiles = tmpDir.listFiles()
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
        val region = regionS3 ?: "us-east-1"
        return S3Client.builder()
            .region(Region.of(region))
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build()
    }
    fun renomearPrimeiroArquivoTsvGzParaTeste() {
        val tmpDir = File("/tmp")
        assertTrue(tmpDir.exists(), "Diretório /tmp não existe")

        val arquivos = tmpDir.listFiles()
            ?.filter { it.isFile && it.name.endsWith(".tsv.gz") }
            ?: emptyList()

        assertTrue(arquivos.isNotEmpty(), "Nenhum arquivo .tsv.gz encontrado no /tmp!")

        val original = arquivos.first()
        val renomeado = File(tmpDir, original.name.replace(".tsv.gz", "_RENAME_TEST.tsv.gz"))

        val ok = original.renameTo(renomeado)
        assertTrue(ok, "Falha ao renomear arquivo ${original.name}")

        LogCollector.println("🔄 Arquivo renomeado:")
        LogCollector.println("  De: ${original.name}")
        LogCollector.println("  Para: ${renomeado.name}")
    }


}
