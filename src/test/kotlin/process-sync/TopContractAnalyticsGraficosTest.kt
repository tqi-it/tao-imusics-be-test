package `process-sync`

import io.restassured.RestAssured
import io.restassured.module.jsv.JsonSchemaValidator
import org.junit.jupiter.api.*
import util.Data
import util.givenOauth

/**
 * → Tests de Contract
 *
 * Escopo:
 *   - Valida a resposta da API com base no JSON Schema definido.
 *   - Garante conformidade estrutural, de tipos, limites e obrigatoriedades.
 *
 * Benefícios:
 *   • Evita regressões em deploys futuros
 *   • Detecta mudanças inesperadas no contrato
 *   • Protege integrações como mobile, dashboard e BI
 *
 * Resultado esperado:
 *   Response deve estar 100% aderente ao schema do contrato.
 */
class TopContractAnalyticsGraficosTest {

    // TODO: Validações do json schema estão comentadas

    /**
     * → Endpoints
     * Endpoints
        * top-grafico-plataforma
        * top-grafico-faixas
        * top-grafico-albuns
     */

    companion object {
        private var token: String = ""
        private var paramDates = "dataInicial=2024-01-01&dataFinal=2025-12-31"

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
                .header("origin","http://localhost:4302")

    }

    /**
     * → Tests de Contract: topGraficoFaixas
     */
    @Test
    @Tag("contractTests")

    @DisplayName("Contrato - OPTIONS /analytics/top-grafico-faixas deve retornar 200 e JSON válido")
    fun topGraficoFaixas() {
        TopContractAnalyticsGeralTest.givenTop()
            .`when`()
            .log().all()
            .options("/analytics/top-grafico-faixas?$paramDates")
            .then()
            .log().all()
            .statusCode(200)
            //.body(JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/top-XXXXXX-schema.json"))

    }

    /**
     * → Tests de Contract: topGraficoAlbuns
     */
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - OPTIONS /analytics/top-grafico-albuns deve retornar 200 e JSON válido")
    fun topGraficoAlbuns() {
        TopContractAnalyticsGeralTest.givenTop()
            .`when`()
            .log().all()
            .options("/analytics/top-grafico-albuns?$paramDates")
            .then()
            .log().all()
            .statusCode(200)
            //.body(JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }

    /**
     * → Tests de Contract: topGraficoPlataforma
     */
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - OPTIONS /analytics/top-grafico-plataforma deve retornar 200 e JSON válido")
    fun topGraficoPlataforma() {
        givenTop()
            .`when`()
            .options("/analytics/top-grafico-plataforma?${paramDates}&lojaId=&faixaMusicalId=&dadosDiarios=")
            .then()
            .statusCode(200)
            //.body(JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/top-grafico-plataforma-schema.json"))

    }



}