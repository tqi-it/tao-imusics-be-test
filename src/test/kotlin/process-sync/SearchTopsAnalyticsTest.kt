package `process-sync`

import io.restassured.RestAssured
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import io.restassured.module.jsv.JsonSchemaValidator
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import util.*
import util.Data.Companion.ORIGIN
import util.Data.Companion.PATH_ANALYTICS_TOP_PLAYS_WL
import java.time.LocalDate

class SearchTopsAnalyticsTest {

    /**
     * → Objetivo: Testes voltados para validações dos cenários 200,500,400,401
     *
     * Endpoint
         * top-plays-wl OK
         * top-plataforma,top-plataformas OK
         * top-playlist,top-playlists OK
         * top-albuns,top-album,top-album-musica,top-album-plataformas OK
         * top-plays-semana,total-plays-periodo OK
         * top-musicas,top-musica OK
         * top-regioes OK
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
            assertNotNull(token, "Token não deve ser nulo")
        }

        fun givenTop()=
            given()
                .header("Authorization", "Bearer $token")
                .header("origin",ORIGIN)

    }

    /**
     * Endpoint
        → top-plays-wl
     */
    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-plays-wl – validar todas as páginas e contrato JSON")
    fun getTopPLaysWhitelabel200() {

        val mes = 11
        val ano = 2025
        val perPage = 10
        var page = 0

        val dataReferencia = LocalDate.of(2025, 11, 1)
        val wlId = 1L

        // 🔥 1. Garantir que Redis tem dados (inseridos pelo pipeline ou por você)
        val dadosRedis = RedisHelper.getTopPlays(wlId, dataReferencia)
            ?: error("Redis está vazio para $dataReferencia — não é possível testar cache HIT")
        println(dadosRedis)

        // ================================
        // 1️⃣ Primeiro request (descobrir total de páginas)
        // ================================
        val firstResponse = givenTop()
            .contentType(ContentType.JSON)
            .queryParam("mesInicial", mes)
            .queryParam("anoInicial", ano)
            .queryParam("mesFinal", mes)
            .queryParam("anoFinal", ano)
            .queryParam("page", page)
            .queryParam("perpage", perPage)
            .log().all()
            .`when`()
            .get(PATH_ANALYTICS_TOP_PLAYS_WL)
            .then()
            .log().all()
            .statusCode(200)
            .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/top-playlistsWL-schema.json"))
            .extract()

        val totalRegistros =
            firstResponse.jsonPath().getInt("qde_registros")
        val somaTotalRegistros =
            firstResponse.jsonPath().getInt("somaTotalRegistros")

        assertTrue(totalRegistros >= 0, "Total de registros não pode ser negativo")
        assertTrue(somaTotalRegistros >= 0, "Soma total inválida")

        val totalPaginas =
            if (totalRegistros == 0) 1 else ((totalRegistros + perPage - 1) / perPage)

        println("🔎 Total registros: $totalRegistros")
        println("📄 Total páginas: $totalPaginas")

        // ================================
        // 2️⃣ Loop percorrendo todas as páginas
        // ================================
        for (paginaAtual in 0 until totalPaginas) {

            println("\n\n==============================")
            println("📄 Validando página $paginaAtual")
            println("==============================")

            val resp = givenTop()
                .contentType(ContentType.JSON)
                .queryParam("mesInicial", mes)
                .queryParam("anoInicial", ano)
                .queryParam("mesFinal", mes)
                .queryParam("anoFinal", ano)
                .queryParam("page", paginaAtual)
                .queryParam("perpage", perPage)
                .log().all()
                .`when`()
                .get(PATH_ANALYTICS_TOP_PLAYS_WL)
                .then()
                .log().all()
                .statusCode(200)
                .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/top-playlistsWL-schema.json"))
                .extract()

            val dados = resp.jsonPath().getList<Map<String, Any>>("dados")

            // ================================
            // 3️⃣ Validações
            // ================================
            assertNotNull(dados)
            assertTrue(
                dados.size <= perPage,
                "Página $paginaAtual excedeu o limite de $perPage registros"
            )

            // Se não for a última página -> deve ter perPage registros
            if (paginaAtual < totalPaginas - 1 && totalRegistros > perPage) {
                assertEquals(
                    perPage,
                    dados.size,
                    "Página $paginaAtual deveria vir com $perPage registros"
                )
            }

            // Validar campos essenciais
            dados.forEach { item ->
                assertTrue(item["id"] != null)
                assertTrue(item["titulo"] is String)
                assertTrue(item["plays"] is Int || item["plays"] is Long)
                assertTrue(item["referencia"] is String)
            }
        }

        println("\n✔ Todas as páginas foram validadas com sucesso!")
    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 401 GET /analytics/top-plays-wl – validar token JWT inválido")
    fun getTopPLaysWhitelabel401() {
        given()
            .header("Authorization", "Bearer xxxx")
            .log().all()
            .get(PATH_ANALYTICS_TOP_PLAYS_WL)
            .then()
            .log().all()
            .statusCode(401)
    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 400 GET /analytics/top-plays-wl – validar parâmetros inválidos")
    fun getTopPLaysWhitelabelParamInvalid400() {

        data class Caso(
            val descricao: String,
            val mesInicial: Any?,
            val anoInicial: Any?,
            val mesFinal: Any? = 11,
            val anoFinal: Any? = 2025,
            val page: Any? = 0,
            val perPage: Any? = 10
        )

        val casosInvalidos = listOf(

            // 🔥 MESES INVÁLIDOS
            Caso("mesInicial = 0", 0, 2025),
            Caso("mesInicial = 13", 13, 2025),
            Caso("mesInicial negativo", -5, 2025),
            Caso("mesInicial string", "abc", 2025),

            Caso("mesFinal = 0", 11, 2025, mesFinal = 0),
            Caso("mesFinal = 13", 11, 2025, mesFinal = 13),
            Caso("mesFinal string", 11, 2025, mesFinal = "xyz"),

            // 🔥 ANOS INVÁLIDOS
            //Caso("anoInicial = 0", 11, 0),
            Caso("anoInicial negativo", 11, -2025),
            Caso("anoInicial string", 11, "AAAA"),

            Caso("anoFinal = 0", 11, 2025, anoFinal = 0),
            Caso("anoFinal negativo", 11, 2025, anoFinal = -2026),
            Caso("anoFinal string", 11, 2025, anoFinal = "202X"),

            // 🔥 PAGINAÇÃO INVÁLIDA
            Caso("page negativo", 11, 2025, page = -1),
            Caso("page string", 11, 2025, page = "xpto"),

            Caso("perpage negativo", 11, 2025, perPage = -10),
            Caso("perpage 0", 11, 2025, perPage = 0),
            Caso("perpage string", 11, 2025, perPage = "dez"),

            // 🔥 CAMPOS AUSENTES / vazios
            Caso("mesInicial vazio", "", 2025),
            Caso("anoInicial vazio", 11, ""),
            Caso("mesInicial null", null, 2025),
            Caso("anoInicial null", 11, null)
        )

        casosInvalidos.forEach { caso ->
            LogCollector.println("\n🚨 Testando caso inválido: ${caso.descricao}")

            val request = givenTop()
                .queryParam("mesInicial", caso.mesInicial)
                .queryParam("anoInicial", caso.anoInicial)
                .queryParam("mesFinal", caso.mesFinal)
                .queryParam("anoFinal", caso.anoFinal)
                .queryParam("page", caso.page)
                .queryParam("perpage", caso.perPage)
                .log().all()
                .`when`()
                .get(PATH_ANALYTICS_TOP_PLAYS_WL)
                .then()
                .log().all()
            assertAll(
                { assertEquals(400, request.extract().statusCode(), "✔ Cenário validado com sucesso → HTTP 400 retornado corretamente.") },
                //{ assertTrue(request.extract().body().asString().contains("mesInicial"), "Mensagem de erro inesperada") }
            )
        }
    }


    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 400 ET /analytics/top-plays-wl – validar ranges inválidos de período (ano/mes)")
    fun getTopPLaysWhitelabelDatesInvalid400() {

        data class Cenario(
            val mesInicial: Int,
            val anoInicial: Int,
            val mesFinal: Int,
            val anoFinal: Int,
            val descricao: String
        )

        val cenariosInvalidos = listOf(

            // ❌ Cenário 1: ano final < ano inicial
            Cenario(11, 2025, 10, 2024, "Ano final menor que o ano inicial"),

            // ❌ Cenário 2: mesmo ano, mas mesFinal < mesInicial
            Cenario(12, 2025, 11, 2025, "Mês final menor que o mês inicial no mesmo ano"),

            // ❌ Outro exemplo
            Cenario(5, 2026, 3, 2026, "Mês final menor dentro do mesmo ano"),

            // ❌ Ano final igual, mas mês inicial > final
            Cenario(8, 2027, 2, 2027, "Período inicial posterior ao final"),

            // ❌ Ano final maior, mas combinação inicial > final quando convertida para YYYYMM
            Cenario(12, 2027, 1, 2027, "Ano igual, mas mês final menor")
        )

        cenariosInvalidos.forEach { c ->
            LogCollector.println("\n🔎 Testando cenário inválido: ${c.descricao}")
            LogCollector.println("→ ${c.mesInicial}/${c.anoInicial}  até  ${c.mesFinal}/${c.anoFinal}")

            val request = givenTop()
                .queryParam("mesInicial", c.mesInicial)
                .queryParam("anoInicial", c.anoInicial)
                .queryParam("mesFinal", c.mesFinal)
                .queryParam("anoFinal", c.anoFinal)
                .queryParam("page", 0)
                .queryParam("perpage", 10)
                .log().all()
                .get(PATH_ANALYTICS_TOP_PLAYS_WL)
                .then()
                .log().all()
            assertAll(
                { assertEquals(400, request.extract().statusCode(), "✔ Cenário validado com sucesso → HTTP 400 retornado corretamente.") },
                //{ assertTrue(request.extract().body().asString().contains("mesInicial"), "Mensagem de erro inesperada") }
            )
        }
    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 404 GET /analytics/top-plays-wl – validar retorno quando não possui dados")
    fun getTopPLaysWhitelabel404() {

        val mes = 11
        val ano = 2027
        val perPage = 10
        var page = 0

        givenTop()
            .contentType(ContentType.JSON)
            .queryParam("mesInicial", mes)
            .queryParam("anoInicial", ano)
            .queryParam("mesFinal", mes)
            .queryParam("anoFinal", ano)
            .queryParam("page", page)
            .queryParam("perpage", perPage)
            .log().all()
            .`when`()
            .get(PATH_ANALYTICS_TOP_PLAYS_WL)
            .then()
            .log().all()
            .statusCode(404)
            .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/top-playlistsWL-schema.json"))
        println("\n✔ Validação com retorn 404 Not Founs realizada com sucesso!")
    }

    /**
     * Endpoint
        → top-plataforma
        → top-plataformas
     */


    // TODO: 🧵 FLUXO 1 — JWT inválido
    // TODO: 🧵 FLUXO 2 — Cache HIT no Redis
    // TODO: 🧵 FLUXO 3 — Cache MISS → Banco PostgreSQL
    // TODO: 🧵 FLUXO 4 — Parâmetros opcionais
    // TODO: 🧵 FLUXO 5 — Paginação
    // TODO:  @DisplayName("1. Popular Redis com dados para Super Admin visualizar (Novembro/2025)")
    // TODO:  @DisplayName("2. Validar que dados estão paginados corretamente")
    // TODO:  @DisplayName("3. Validar ordenação por plays (DESC)")
    // TODO:  @DisplayName("4. Validar cálculo de total de plays")
    // TODO:  @DisplayName("5. Popular dados para período de 3 meses")
    // TODO:  @DisplayName("6. Validar estrutura dos dados armazenados")

}