package `process-sync`

import io.restassured.RestAssured
import io.restassured.module.jsv.JsonSchemaValidator.matchesJsonSchemaInClasspath
import org.hamcrest.Matchers.*
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
                .header("origin","http://localhost:4302")

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
    @Tag("")
    @DisplayName("Contrato - GET /analytics/top-playlist deve retornar 200 e JSON válido")
    fun topPlaylist() {
        givenTop()
            .`when`()
            .get("/analytics/top-playlist?dataInicial=2025-01-01&dataFinal=2025-11-30&faixaMusicalId=")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-playlist-schema.json"))

    }
    @Test
    @Tag("")
    @DisplayName("Contrato - GET /analytics/top-plays-semana deve retornar 200 e JSON válido")
    fun topPlaysSemana() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-plays-semana")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataformas-schema.json"))

    }


    /**
     * → Tests de Contract: TopRegiões
     */
    @Test
    @Tag("contractTests")  // TODO: OK so não esta validando obrigatoriedade (mensagem, nomePais e imagemPais)
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
    @Tag("contractTests") // TODO: OK
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
    @Tag("contractTests") // TODO: OK so não esta validando obrigatoriedade (mensagem )
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
    @Tag("contractTests") // TODO: OK so não esta validando obrigatoriedade (mensagem e plataforma)
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
    @Tag("")
    @DisplayName("Contrato - GET /analytics/top-albuns deve retornar 200 e JSON válido")
    fun topAlbuns() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-albuns")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }
    @Test
    @Tag("")
    @DisplayName("Contrato - GET /analytics/top-album deve retornar 200 e JSON válido")
    fun topAlbum() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-album")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-album-schema.json"))

    }
    @Test
    @Tag("")
    @DisplayName("Contrato - GET /analytics/top-album-musica deve retornar 200 e JSON válido")
    fun topAlbumMusica() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-album-musica")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-album-schema.json"))

    }
    @Test
    @Tag("")
    @DisplayName("Contrato - GET /analytics/top-album-plataformas deve retornar 200 e JSON válido")
    fun topAlbumPLataformas() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-album-plataformas")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataformas-schema.json"))

    }

}