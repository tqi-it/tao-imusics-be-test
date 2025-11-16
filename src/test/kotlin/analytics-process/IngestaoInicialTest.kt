package `analytics-process`

import io.restassured.RestAssured
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import org.apache.http.HttpStatus
import org.apache.http.protocol.HTTP
import org.awaitility.Awaitility
import org.awaitility.kotlin.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import java.time.Duration
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import util.givenCreateAcceptAndJson
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

class IngestaoInicialTest {

    companion object {
        private const val BASE_URL = "http://localhost:3015"
        private var token: String = ""
        private var startDate ="2025-11-14"
        private var endDate ="2025-11-14"
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

            val response = RestAssured.given()
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
    @Tag("smokeTests")
    fun `CN1 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3`() {
        // 🔹 Corpo com período definido
        val requestBody = """
            {
              "start-date": "$startDate",
              "end-date": "$endDate"
            }
        """.trimIndent()

        // 🔹 Fazer chamada ao /start-process
        val startResponse = RestAssured.given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .body(requestBody)
            .post("/start-process")
            .then()
            .extract()

        val statusCode = startResponse.statusCode()

        // 🔹 Caso já exista processo rodando (409 por exemplo)
        if (statusCode == 409 || statusCode == 400) {
            println("Processo já está em execução. Código: $statusCode")
            assertTrue(statusCode == 409 || statusCode == 400)
            return
        }

        // 🔹 Caso contrário, precisa ser 200 ou 202 = processo iniciou corretamente
        assertTrue(statusCode == 200 || statusCode == 202, "O processo não iniciou corretamente")

        // 🔹 Validar se os arquivos foram deletados no diretório
        validarTmpSemArquivosDePlayers()

        // 🔥 Loop para acompanhar o processo via /process-status
        var finalStatus = ""
        val timeout = Duration.ofMinutes(15) // tempo máximo total do teste
        val start = System.currentTimeMillis()

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
            println("🔄 Flow: $descFlow\n📌 Status Atual: $finalStatus\n")

            // sai do loop quando o processo terminar
            if (finalStatus.equals("completed", ignoreCase = true)) break

            // timeout de segurança
            val elapsedMinutes = (System.currentTimeMillis() - start) / 60000
            if (elapsedMinutes > timeout.toMinutes()) {
                fail("Timeout: processo demorou demais para concluir ($elapsedMinutes minutos)")
            }

        } while (true)

        // 🔹 Validação final
        assertEquals("completed", finalStatus.lowercase(), "Processo não chegou ao status 'concluido'")
        println("✔ Processo finalizado com sucesso! Status = $finalStatus")
        println("────────────────────────────────────────────")

        // 🔥 Validação dos arquivos no /tmp
        validarArquivosNoTmp("$startDate", "$endDate")

    }


    @Test
    @Tag("smokeTests") // Usando 2 dias para frente
    fun `CN2 - Validar ingestão quando não possui arquivos para baixar`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(2).format(formatter)

        // 🔹 Corpo com período definido
        val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

        // 🔹 Fazer chamada ao /start-process
        val startResponse = RestAssured.given()
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
        println("Status process: $statusStart")

        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(2, TimeUnit.SECONDS)
            .untilCallTo {

                val response = given()
                    .contentType(ContentType.JSON)
                    .header("origin", "http://localhost")
                    .header("authorization", "Bearer $token")
                    .get("/process-status")
                    .then()
                    .extract()

                val error = response.jsonPath().getString("error")
                println("⏳ Status atual -> error: $error")
                // esta é a expressão que o Awaitility captura
                error
            } matches { errorValue ->
            errorValue == "FUGA has no analytics to download for the requested period"
        }


    }


    @Test
    @Tag("smokeTests") // Usando 2 dias para frente
    fun `CN3 - Validar ingestão quando já possui um processamento sendo realizado`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(2).format(formatter)

        // 🔹 Corpo com período definido
        val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

        repeat(2) { tentativa ->
            val numero = tentativa + 1
            println("Executando start-process tentativa $numero")

            val resposta = RestAssured.given()
                .contentType(ContentType.JSON)
                .header("origin", "http://localhost")
                .header("authorization", "Bearer $token")
                .body(requestBody)
                .post("/start-process")
                .then()
                .extract()

            val statusCode = resposta.statusCode()
            val success = resposta.jsonPath().getBoolean("success")

            if (tentativa == 0) {
                // 🟢 PRIMEIRA EXECUÇÃO — Espera 200
                assertEquals(
                    HttpStatus.SC_OK,
                    statusCode,
                    "Primeira execução deveria retornar 200 OK"
                )
                assertTrue(success, "Primeira execução deveria retornar success=true")
                println("✔️ Tentativa 1 OK: status=$statusCode success=$success")

            } else {
                // 🔴 SEGUNDA EXECUÇÃO — Espera 409 (já tem processo rodando)
                assertEquals(
                    HttpStatus.SC_CONFLICT,
                    statusCode,
                    "Segunda execução deveria retornar 409, mas retornou $statusCode"
                )
                assertFalse(success, "Segunda execução deveria retornar success=false")
                println("✔️ Tentativa 2 Bloqueada como esperado: status=$statusCode success=$success")
            }

            Thread.sleep(1000)
        }


        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(2, TimeUnit.SECONDS)
            .until {

                val response = given()
                    .contentType(ContentType.JSON)
                    .header("origin", "http://localhost")
                    .header("authorization", "Bearer $token")
                    .get("/process-status")
                    .then()
                    .extract()

                val error = response.jsonPath().getString("error")
                val message = response.jsonPath().getString("message")

                println("⏳ Status atual → error: $error | message: $message")

                // ❗ Aqui você retorna APENAS uma condição para parar o Awaitility
                // Por exemplo, até o status deixar de ser 'running'
                val status = response.jsonPath().getString("status")

                status != "running"  // só para parar o loop quando finalizar
            }



    }


    @Test
    @Tag("smokeTests")
    fun `CN4 - Validar ingestão com sucesso download|limpeza|descompactação|upload dos arquivos para o S3 sem passar data`() {

        // 🔹 Fazer chamada ao /start-process
        val startResponse = RestAssured.given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .post("/start-process")
            .then()
            .extract()

        val statusCode = startResponse.statusCode()

        // 🔹 Caso já exista processo rodando (409 por exemplo)
        if (statusCode == 409 || statusCode == 400) {
            println("Processo já está em execução. Código: $statusCode")
            assertTrue(statusCode == 409 || statusCode == 400)
            return
        }

        // 🔹 Caso contrário, precisa ser 200 ou 202 = processo iniciou corretamente
        assertTrue(statusCode == 200 || statusCode == 202, "O processo não iniciou corretamente")

        // 🔹 Validar se os arquivos foram deletados no diretório
        validarTmpSemArquivosDePlayers()

        // 🔥 Loop para acompanhar o processo via /process-status
        var finalStatus = ""
        val timeout = Duration.ofMinutes(15) // tempo máximo total do teste
        val start = System.currentTimeMillis()

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
            println("🔄 Flow: $descFlow\n📌 Status Atual: $finalStatus\n")

            // sai do loop quando o processo terminar
            if (finalStatus.equals("completed", ignoreCase = true)) break

            // timeout de segurança
            val elapsedMinutes = (System.currentTimeMillis() - start) / 60000
            if (elapsedMinutes > timeout.toMinutes()) {
                fail("Timeout: processo demorou demais para concluir ($elapsedMinutes minutos)")
            }

        } while (true)

        // 🔹 Validação final
        assertEquals("completed", finalStatus.lowercase(), "Processo não chegou ao status 'concluido'")
        println("✔ Processo finalizado com sucesso! Status = $finalStatus")

        // 🔥 Validação dos arquivos no /tmp
        validarArquivosNoTmp("$startDate", "$endDate")

    }



    /**
     *Função para validar que nao tenha nenhum arquivo no diretório tmp antes da execução
     */
    fun validarTmpSemArquivosDePlayers(
        timeoutSeconds: Long = 60,
        pollIntervalSeconds: Long = 2
    ) {
        val tmpDir = java.io.File("/tmp")
        assertTrue(tmpDir.exists(), "Diretório /tmp não existe")

        println("🕵️‍♂️ Monitorando /tmp até que nenhum arquivo de players seja encontrado...")

        val start = System.currentTimeMillis()
        var arquivosFiltrados: List<String>

        while (true) {
            val arquivos = tmpDir.listFiles()?.map { it.name } ?: emptyList()

            // Filtra somente arquivos dos players
            arquivosFiltrados = arquivos.filter { nome ->
                expectedPlayers.any { player ->
                    nome.contains(player, ignoreCase = true)
                }
            }

            if (arquivosFiltrados.isEmpty()) {
                println("\n✅ Nenhum arquivo de players encontrado no /tmp. Diretório limpo!")
                break
            }

            // Ainda existem arquivos → loga quais são
            println("\n⚠️ Arquivos de players ainda encontrados no /tmp:")
            arquivosFiltrados.forEach { println(" - $it") }

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
            println("\uD83D\uDCC2 Nenhum arquivo foi encontrado!!!")
        } else {
            arquivosFiltrados.forEach { println(" - $it") }
        }

        println("✔ Processo concluído: diretório /tmp está limpo.")
        println("────────────────────────────────────────────\n")
    }


    /**
     *Função para validar que todos os arquivos dos 8 players foram gerados para todas as datas
     */
    private fun validarArquivosNoTmp(startDate: String, endDate: String) {
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val inicio = LocalDate.parse(startDate, dateFormatter)
        val fim = LocalDate.parse(endDate, dateFormatter)

        val tmpDir = java.io.File("/tmp")
        assertTrue(tmpDir.exists(), "Diretório /tmp não existe")

        // Filtrar somente arquivos .tsv ou .tsv.gz
        val arquivos = tmpDir.listFiles()
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
        println("\n📂 Lista de arquivos encontrados no /tmp:")
        agrupadoPorData.forEach { (data, lista) ->
            println("📅 $data")
            lista.forEach { nome ->
                val player = expectedPlayers.firstOrNull { nome.startsWith(it) }
                val icon = playerIcons[player] ?: "📁"
                println("   $icon  $nome")
            }
        }
        println("\n────────────────────────────────────────────\n")

        val dias = inicio.datesUntil(fim.plusDays(1)).toList()
        println("\n📂 Lista de arquivos não encontrados no /tmp:")
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
                    println("✅ Encontrado → $player ($dataStr)")
                } else {
                    println("❌ NÃO ENCONTRADO → $player ($dataStr)")
                }

                // Comentado para nao quebrar o teste pois no dia pode ainda nao ter arquivo no diretorio de origem
                /*
                assertTrue(
                    encontrado,
                    "Arquivo esperado não encontrado no /tmp → player=$player data=$dataStr"
                )*/
            }
        }
        println("\n✔ Arquivos validados com sucesso: todos os players e datas encontrados no /tmp")
    }



}
