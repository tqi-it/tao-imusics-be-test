package `process-sync`

import `analytics-process`.RechargingProcessingTest
import io.restassured.RestAssured
import io.restassured.http.ContentType
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import util.Data
import util.LogCollector
import util.givenOauth
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

class ProcessingAnalyticsTest {

    /**
     * → Objetivo: Testes voltados para validações dos cenários 200,500,400,401
     *
     * Principais:
         * process-start (Inicia o processamento geral de analytics)
         * start-analytics (Inicia analytics gerais)
         * process-status (Verifica status do processamento)
         * reset-status (Resetar o status do processamento)
     * Por Players
         * start-top-plays
         * start-top-platform
         * start-top-playlist
         * start-top-albuns
         * start-top-album
         * start-top-regiao
         * start-top-regioes
     */

    companion object {
        private var token: String = ""
        private var paramDates = "dataInicial=2024-01-01&dataFinal=2025-11-30"

        @JvmStatic
        @BeforeAll
        fun setup() {
            RestAssured.baseURI = Data.BASE_URL_BACKEND
            val response = givenOauth()
            token = response.jsonPath().getString("token")
            Assertions.assertNotNull(token, "Token não deve ser nulo")
        }

        fun givenTop()=
            RestAssured.given()
                .header("Authorization", "Bearer $token")
                .header("origin", Data.ORIGIN)

    }

    @Test
    @Tag("smokeTests") // TPF-XX
    fun `CN15 - Validar processamento XXXXXX`() {

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val date = LocalDate.now().plusDays(-5).format(formatter)
        val requestBody = """
            {
              "start-date": "$date",
              "end-date": "$date"
            }
        """.trimIndent()

        LogCollector.println("🚀 PASSO 1: Iniciando Processamento normal igual a -5 dias /start-process...")
        val startResponse = RestAssured.given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .log().all()
            .body(requestBody)
            .post(Data.PATH_PROCESS)
            .then()
            .log().all()
            .statusCode(200)
            .extract()

        Assertions.assertTrue(startResponse.jsonPath().getBoolean("success"))
        Assertions.assertFalse(startResponse.jsonPath().getBoolean("is_reprocessing"))
        Assertions.assertEquals(5, startResponse.jsonPath().getInt("period_days"))
        Assertions.assertEquals("Process started (background)", startResponse.jsonPath().getString("message"))
        LogCollector.println("\n────────────────────────────────────────────")


        LogCollector.println("🚀 PASSO 2: Realizando monitoramento do /process-status...")
        Awaitility.await()
            .atMost(40, TimeUnit.MINUTES)
            .pollInterval(2, TimeUnit.MINUTES)
            .until {
                val resp = RestAssured.given()
                    .header("authorization", "Bearer $token")
                    .header("origin", "http://localhost")
                    .get("/process-status")
                    .then()
                    .log().all()
                    .extract()

                val status = resp.jsonPath().getString("status") ?: ""
                val flow = resp.jsonPath().getString("current_step") ?: ""
                val message = resp.jsonPath().getString("message") ?: ""
                LogCollector.println("\n📌 Status atual → $status")
                LogCollector.println("🔄 Step atual → $flow")
                LogCollector.println("🔄 Message → $message")
                status.equals("running", true)
            }
        LogCollector.println("\n────────────────────────────────────────────")

    }




}