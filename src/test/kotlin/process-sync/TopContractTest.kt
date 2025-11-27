package `process-sync`

import io.restassured.RestAssured
import io.restassured.module.jsv.JsonSchemaValidator.matchesJsonSchemaInClasspath
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import util.Data
import util.givenOauth

class TopContractTest {

    companion object {
        private var token: String = ""

        @JvmStatic
        @BeforeAll
        fun setup() {
            RestAssured.baseURI = Data.BASE_URL_BACKEND
            val response = givenOauth()
            token = response.jsonPath().getString("token")
            Assertions.assertNotNull(token, "Token não deve ser nulo")
        }
    }

    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-plataformas deve retornar 200 e JSON válido")
    fun topPlataformas() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-plataformas")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataformas-schema.json"))

    }

    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-playlists deve retornar 200 e JSON válido")
    fun topPlaylists() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-playlists")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataformas-schema.json"))

    }

    @Test
    @Tag("contractTests")
    @Disabled("Contrato instável — aguardando backend ajustar campos")
    @DisplayName("Contrato - GET /analytics/top-playlist deve retornar 200 e JSON válido")
    fun topPlaylist() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-playlist")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-plataformas-schema.json"))

    }

    @Test
    @Tag("contractTests")
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
    @Tag("contractTests")
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
    @Tag("contractTests")
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
    @Tag("contractTests")
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

    @Test
    @Tag("contractTests")
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

    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-grafico-plataforma deve retornar 200 e JSON válido")
    fun topGraficoPlataforma() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-grafico-plataforma")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }

    @Test
    @Tag("contractTests")
    @DisplayName("Contrato - GET /analytics/top-grafico-faixas deve retornar 200 e JSON válido")
    fun topGraficoFaixas() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-grafico-faixas")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }

    @Test
    @Tag("contractTests")
    @Disabled("Contrato instável — aguardando backend ajustar campos")
    @DisplayName("Contrato - GET /analytics/top-grafico-albuns deve retornar 200 e JSON válido")
    fun topGraficoAlbuns() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-grafico-albuns")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }

    @Test
    @Tag("contractTests")
    @Disabled("Contrato instável — aguardando backend ajustar campos")
    @DisplayName("Contrato - GET /analytics/top-musicas deve retornar 200 e JSON válido")
    fun topMusicas() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-musicas")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }

    @Test
    @Tag("contractTests")
    @Disabled("Contrato instável — aguardando backend ajustar campos")
    @DisplayName("Contrato - GET /analytics/top-musica deve retornar 200 e JSON válido")
    fun topMusica() {
        RestAssured.given()
            .header("Authorization", "Bearer $token")
            .`when`()
            .get("/analytics/top-musica")
            .then()
            .statusCode(200)
            .body(matchesJsonSchemaInClasspath("schemas/top-albuns-schema.json"))

    }


}