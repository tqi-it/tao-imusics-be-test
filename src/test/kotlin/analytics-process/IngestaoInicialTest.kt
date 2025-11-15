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
        private var startDate ="2025-11-13"
        private var endDate ="2025-11-13"
        val expectedPlayers = listOf(
            "iMusics_Amazon",
            "iMusics_Deezer",
            "iMusics_iTunes",
            //"iMusics_TikTok",
            "iMusics_Pandora",
            //"iMusics_Spotify",
            //"iMusics_Youtube",
            //"iMusics_SoundCloud"
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

        // 🔥 Validação dos arquivos no /tmp
        validarArquivosNoTmp("$startDate", "$endDate")

    }


    @Test
    @Tag("smokeTests")
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

        println("✔ Processo concluído: diretório /tmp está limpo.\n")
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

        val dias = inicio.datesUntil(fim.plusDays(1)).toList()

        dias.forEach { dia ->
            val dataStr = dia.format(dateFormatter)

            expectedPlayers.forEach { player ->

                // agora valida .tsv E .tsv.gz
                val encontrado = arquivos.any { nome ->
                    nome.startsWith(player) &&
                            nome.contains(dataStr) &&
                            (nome.endsWith(".tsv.gz") || nome.endsWith(".tsv"))
                }

                assertTrue(
                    encontrado,
                    "Arquivo esperado não encontrado no /tmp → player=$player data=$dataStr"
                )
            }
        }

        // 🔥 Listar SOMENTE arquivos .tsv e .tsv.gz encontrados
        println("\n📂 Arquivos encontrados no /tmp (${arquivos.size} arquivos):")
        arquivos.forEach { nome ->
            println(" - $nome")
        }

        println("\n✔ Arquivos validados com sucesso: todos os players e datas encontrados no /tmp")
    }



}
