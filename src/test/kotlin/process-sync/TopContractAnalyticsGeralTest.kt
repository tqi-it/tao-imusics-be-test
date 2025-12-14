package `process-sync`

import io.restassured.RestAssured
import io.restassured.module.jsv.JsonSchemaValidator.matchesJsonSchemaInClasspath
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import util.Data
import util.Data.Companion.ORIGIN
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
 *
 * Endpoint
    * top-plays-wl OK
    * top-plataforma,top-plataformas OK
    * top-playlist,top-playlists OK
    * top-albuns,top-album,top-album-musica,top-album-plataformas OK
    * top-plays-semana,total-plays-periodo OK
    * top-musicas,top-musica OK
    * top-regioes OK
 *
 */
class TopContractAnalyticsGeralTest {

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
                .header("origin",ORIGIN)

    }


    /**
     * → Tests de Contract: TopPlayList
     */
    @Test
    @Tag("contractTests")  // TODO: OK so não esta validando obrigatoriedade (mensagem)
    @DisplayName("Contrato - GET /analytics/top-playlists deve retornar 200 e JSON válido")
    fun topPlaylists() {
        givenTop()
            .`when`()
            .log().all()
            .get("/analytics/top-playlists?pagina=0&qde_por_pagina=5&$paramDates&faixaMusicalId=")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-playlists-schema.json"))

    }
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-playlist deve retornar 200 e JSON válido")
    fun topPlaylist() {
        givenTop()
            .`when`()
            .get("/analytics/top-playlist?dataInicial=2025-01-01&dataFinal=2025-11-30&faixaMusicalId=")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-playlist-schema.json"))

    }
    @Test
    @Tag("contractTests")
    @Disabled("Sem Dados no Ambiente")
    @DisplayName("Contrato - GET /analytics/top-plays-semana deve retornar 200 e JSON válido")
    fun topPlaysSemana() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-plays-semana")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plays-semana-schema.json"))

    }


    /**
     * → Tests de Contract: TopRegiões
     */
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-regioes deve retornar 200 e JSON válido")
    fun topRegioes() {
        givenTop()
            .`when`()
            .log().all()
            .get("/analytics/top-regioes?pagina=0&qde_por_pagina=5&$paramDates")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-regioes-schema.json"))

    }


    /**
     * → Tests de Contract: TopPlataformas
     */
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-plataformas deve retornar 200 e JSON válido")
    fun topPlataformas() {
        givenTop()
            .`when`()
            .log().all()
            .get("/analytics/top-plataformas?pagina=0&qde_por_pagina=5&order_field=id&order_direction=ASC&$paramDates&faixaMusicalId=")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataformas-schema.json"))

    }
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-plataforma deve retornar 200 e JSON válido")
    fun topPlataforma() {
        givenTop()
            .`when`()
            .log().all()
            .get("/analytics/top-plataforma?pagina=0&qde_por_pagina=5&order_field=id&order_direction=ASC&$paramDates&faixaMusicalId=")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataforma-schema.json"))

    }


    /**
     * → Tests de Contract: TopMusicas
     */
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-musicas deve retornar 200 e JSON válido")
    fun topMusicas() {
        givenTop()
            .`when`()
            .log().all()
            .get("/analytics/top-musicas?pagina=0&qde_por_pagina=5&$paramDates&faixaMusicalId=")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-musicas-schema.json"))

    }
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-musica deve retornar 200 e JSON válido")
    fun topMusica() {
        givenTop()
            .`when`()
            .log().all()
            .get("/analytics/top-musica?$paramDates")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-musica-schema.json"))

    }


    /**
     * → Tests de Contract: TopAlbuns
     */
    @Test
    @Tag("contractTests")
    @Disabled("Sem Dados no Ambiente")
    @DisplayName("Contrato - GET /analytics/top-albuns deve retornar 200 e JSON válido")
    fun topAlbuns() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-albuns?dataInicial=2024-01-01&dataFinal=2025-11-30&pagina=0&qde_por_pagina=5")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }
    @Test
    @Tag("contractTests")
    @Disabled("Sem Dados no Ambiente")
    @DisplayName("Contrato - GET /analytics/top-album deve retornar 200 e JSON válido")
    fun topAlbum() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-album?dataInicial=2024-01-01&dataFinal=2025-11-30&pagina=0&qde_por_pagina=5")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-album-schema.json"))

    }
    @Test
    @Tag("contractTests")
    @Disabled("Sem Dados no Ambiente")
    @DisplayName("Contrato - GET /analytics/top-album-musica deve retornar 200 e JSON válido")
    fun topAlbumMusica() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-album-musica?dataInicial=2024-01-01&dataFinal=2025-11-30&pagina=0&qde_por_pagina=5")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-album-schema.json"))

    }
    @Test
    @Tag("contractTests")
    @Disabled("Sem Dados no Ambiente")
    @DisplayName("Contrato - GET /analytics/top-album-plataformas deve retornar 200 e JSON válido")
    fun topAlbumPLataformas() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-album-plataformas?dataInicial=2024-01-01&dataFinal=2025-11-30&pagina=0&qde_por_pagina=5")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataformas-schema.json"))

    }


    /**
     * → Tests de Contract: Whitelist
     */
    @Test
    @Tag("contractTests")
    @Disabled("Sem dados no Ambiente")
    @DisplayName("Contrato - GET /analytics/top-plays-wl deve retornar 200 e JSON com dados")
    fun topPlaysWhitelabel() {
        givenTop()
            .`when`()
            .log().all()
            .get("/analytics/top-plays-wl?mesInicial=11&anoInicial=2025&mesFinal=11&anoFinal=2025&page=1&perpage=10")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-playlistsWL-schema.json"))

    }

    /**
     * → Tests de Contract: Total Players por Período
     */
    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/total-plays-periodo deve retornar 200 e JSON válido")
    fun totalPlaysPeriodo() {
        givenTop()
            .`when`()
            .get("/analytics/total-plays-periodo?dataInicial=2025-01-01&dataFinal=2025-11-30")
            .then()
            .log().all()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/total-plays-periodo-shema.json"))

    }

}