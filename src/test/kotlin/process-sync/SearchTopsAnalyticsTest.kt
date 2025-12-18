package `process-sync`

import io.restassured.RestAssured
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import io.restassured.module.jsv.JsonSchemaValidator
import io.restassured.response.ValidatableResponse
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import util.*
import util.Data.Companion.ORIGIN
import util.Data.Companion.PATH_ANALYTICS_TOP_ALBUM
import util.Data.Companion.PATH_ANALYTICS_TOP_ALBUM_MUSICA
import util.Data.Companion.PATH_ANALYTICS_TOP_ALBUM_PLATAFORMAS
import util.Data.Companion.PATH_ANALYTICS_TOP_ALBUNS
import util.Data.Companion.PATH_ANALYTICS_TOP_MUSICA
import util.Data.Companion.PATH_ANALYTICS_TOP_MUSICAS
import util.Data.Companion.PATH_ANALYTICS_TOP_PLATAFORMA
import util.Data.Companion.PATH_ANALYTICS_TOP_PLATAFORMAS
import util.Data.Companion.PATH_ANALYTICS_TOP_PLAYLIST
import util.Data.Companion.PATH_ANALYTICS_TOP_PLAYLISTS
import util.Data.Companion.PATH_ANALYTICS_TOP_PLAYS_REMUNERADOS
import util.Data.Companion.PATH_ANALYTICS_TOP_PLAYS_SEMANA
import util.Data.Companion.PATH_ANALYTICS_TOP_PLAYS_WL
import util.Data.Companion.PATH_ANALYTICS_TOP_REGIOES
import util.Data.Companion.PATH_ANALYTICS_TOTAL_PLAYS_PERIODO
import util.RedisUtils.garantirRedisComDadosParaAlgumPlayer
import util.RedisUtils.somarTotalItemsRedisPorMes

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
         * top-plataforma
         * top-grafico-plataforma
         * top-grafico-faixas
         * top-grafico-albuns

     * Cenários
         * CN 1 Cache Hit – Dados já no Redis
            * Dado que os dados do período solicitado estão no Redis
            * Quando o cliente chama qualquer endpoint analytics
            * Então os dados devem ser filtrados, sumarizados e retornados sem consultas ao Postgres.

         * CN 2 Cache Miss – Consulta ao banco
            * Dado que não há dados no Redis
            * Quando a API é chamada
            * Então o sistema deve consultar o Postgres, gerar os joins necessários e armazenar o resultado em cache.
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
     * Endpoint cenários e sucesso
     * Cache Hit – Dados já no Redis
     * Cache Miss – Consulta ao banco
    → top-plays-wl
     */
    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-plays-wl – validar dados retornados compativeis com dados do Redis")
    fun `CN13 - Validar consulta 'top-plays-wl' Cache Hit – Dados já no Redis`() {

        val mes = 11
        val ano = 2025
        val perPage = 50
        val sampleDay = "2025-11-02" //"2025-11-11"

        LogCollector.println("🧪 CN13 - Validar consulta 'top-plays-wl' Cache Hit – Dados já no Redis")

        // TODO: Aqui seta outros arquivos gerado no redis
        // 🔹 Pré-condição
        assertTrue(
            garantirRedisComDadosParaAlgumPlayer(sampleDay),
            "❌ Redis não contém dados para nenhum player no dia $sampleDay"
        )

        // 🔹 Redis → soma todos os dias + plataformas
        val totalRedis = somarTotalItemsRedisPorMes(
            prefix = "imusic:topplays",
            ano = ano,
            mes = mes
        )
        LogCollector.println("📊 Total Redis (mês): $totalRedis")


        // 🔹 Paginação API
        val totalApi = paginarApiTopPlaysWL(
            mes = mes,
            ano = ano,
            perPage = perPage,
            path = PATH_ANALYTICS_TOP_PLAYS_WL
        )
        LogCollector.println("📊 Total API (mês): $totalApi")

        // 🔹 Comparação agregada
        assertEquals(
            totalRedis,
            totalApi,
            "❌ Total API diferente do Redis"
        )

        // 🔹 TODO: Pegar umas listas por amostragem para validar redis X API

        LogCollector.println("🎉 CN13 validado com sucesso (paginação + cache hit)")
    }
    fun paginarApiTopPlaysWL(
        mes: Int,
        ano: Int,
        perPage: Int = 50,
        maxPages: Int = 20,
        path:String
    ): Long {

        var page = 0
        var total = 0L

        while (page < maxPages) {

            val response = givenTop()
                .queryParam("mesInicial", mes)
                .queryParam("anoInicial", ano)
                .queryParam("mesFinal", mes)
                .queryParam("anoFinal", ano)
                .queryParam("page", page)
                .queryParam("perpage", perPage)
                .get(path)
                .then()
                .statusCode(200)
                .extract()

            val items = response.jsonPath().getList<Any>("data")

            if (items.isEmpty()) break

            total += items.size
            page++
        }

        return total
    }


    /**
     * Endpoint cenários e sucesso
     * Filtros combinados
     * Respeito às permissões do usuário
     * Paginação
    → top-plays-wl
     */
    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-plays-wl – validar todas as páginas e contrato JSON")
    fun `CN15 - GET Validar contrato e paginação 'top-plays-wl' 200`() {

        val mes = 9
        val ano = 2025
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN15 - GET Validar contrato e paginação 'top-plays-wl' 200`")
        LogCollector.println("📅 Período: $mes/$ano")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTops(
            mesInicial = mes,
            anoInicial=ano,
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_PLAYS_WL,
            contract = "schemas/top-playlistsWL-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos id, playr, titulo, referencia
            // ================================
            dados.forEach { validarItemTopPlays(it) }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")
    }
    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-plataformas – validar todas as páginas e contrato JSON")
    fun `CN16 - GET Validar contrato e paginação 'top-plataformas' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN16 - GET Validar contrato e paginação 'top-plataformas' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsPlataformas(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            Id =1,
            paramId = "faixaMusicalId",
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_PLATAFORMAS,
            contract = "schemas/top-plataformas-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: plataforma,porcentagem,qdePlays,logoLoja
            // ================================
            dados.forEach { item ->
                assertTrue(item["plataforma"] is String || item["plataforma"]!= null)
                assertTrue(item["porcentagem"] is Number || item["porcentagem"]!= null)
                assertTrue(item["qdePlays"] is Int || item["qdePlays"] is Long)
                assertTrue(item["logoLoja"] is String || item["logoLoja"].toString().contains("https://"))
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-plataforma – validar todas as páginas e contrato JSON")
    fun `CN17 - GET Validar contrato e paginação 'top-plataforma' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN17 - GET Validar contrato e paginação 'top-plataforma' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        // TODO: Quais parametros
        val responses = endpointTopsPlataformas(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            Id =1,
            paramId = "faixaMusicalId",
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_PLATAFORMA,
            contract = "schemas/top-plataforma-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: plataforma,porcentagem,qdePlays,logoLoja
            // ================================
            dados.forEach { item ->
                assertTrue(item["plataforma"] is String || item["plataforma"]!= null)
                assertTrue(item["porcentagem"] is Number || item["porcentagem"]!= null)
                assertTrue(item["qdePlays"] is Int || item["qdePlays"] is Long)
                assertTrue(item["logoLoja"] is String || item["logoLoja"].toString().contains("https://"))
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-playlists – validar todas as páginas e contrato JSON")
    fun `CN18 - GET Validar contrato e paginação 'top-playlists' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN18 - GET Validar contrato e paginação 'top-playlists' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsPlataformas(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            Id =1,
            paramId = "faixaMusicalId",
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_PLAYLISTS,
            contract = "schemas/top-playlists-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": ["plataforma", "tituloMusica", "capaAlbum", "qdePlays", "percentual"],
            // ================================
            dados.forEach { item ->
                assertTrue(item["plataforma"] is String || item["plataforma"]!= null)
                assertTrue(item["tituloMusica"] is String || item["tituloMusica"]!= null)
                assertTrue(item["capaAlbum"] is String )
                assertTrue(item["qdePlays"] is Int || item["qdePlays"] is Long)
                val percentual = (item["percentual"] as Number).toLong()
                assertTrue(percentual in 0..100, "❌ Campo percentual não pode ser negativo: $percentual")
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-playlist – validar todas as páginas e contrato JSON")
    fun `CN19 - GET Validar contrato e paginação 'top-playlist' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN19 - GET Validar contrato e paginação 'top-playlist' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsPlayList(
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_PLAYLIST,
            contract = "schemas/top-playlist-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": ["id", "qdePlaysAtual", "titulo", "artistas", "dataLancamento", "capa", "qdePlaysAnterior", "plataforma"],
            // ================================
            dados.forEach { item ->
                assertTrue(item["qdePlaysAtual"] is Int || item["qdePlaysAtual"] is Long)
                assertTrue(item["titulo"] is String || item["titulo"]!= null)
                assertTrue(item["artistas"] is String || item["artistas"]!= null)
                assertTrue(item["dataLancamento"]!= null)
                assertTrue(item["capa"] is String )
                assertTrue(item["qdePlaysAnterior"] is Int || item["qdePlaysAnterior"] is Long)
                assertTrue(item["plataforma"] is String || item["plataforma"]!= null)
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-albuns – validar todas as páginas e contrato JSON")
    fun `CN20 - GET Validar contrato e paginação 'top-albuns' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN20 - GET Validar contrato e paginação 'top-albuns' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsAlbuns(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_ALBUNS,
            contract = "schemas/top-albuns-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-album – validar todas as páginas e contrato JSON")
    fun `CN21 - GET Validar contrato e paginação 'top-album' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN21 - GET Validar contrato e paginação 'top-album' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsPlataformas(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            Id =1,
            paramId = "distribuicaoId",
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_ALBUM,
            contract = "schemas/top-album-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-musicas – validar todas as páginas e contrato JSON")
    fun `CN22 - GET Validar contrato e paginação 'top-musicas' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN22 - GET Validar contrato e paginação 'top-musicas' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsPlataformas(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            Id =1,
            paramId = "faixaMusicalId",
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_MUSICAS,
            contract = "schemas/top-musicas-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-musica – validar todas as páginas e contrato JSON")
    fun `CN23 - GET Validar contrato e paginação 'top-musica' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN23 - GET Validar contrato e paginação 'top-musica' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsAlbuns(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_MUSICA,
            contract = "schemas/top-musica-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-album-musica – validar todas as páginas e contrato JSON")
    fun `CN24 - GET Validar contrato e paginação 'top-album-musica' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN24 - GET Validar contrato e paginação 'top-album-musica' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsPlataformas(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            Id=1,
            paramId="distribuicaoId",
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_ALBUM_MUSICA,
            contract = "schemas/top-musica-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-album-plataformas – validar todas as páginas e contrato JSON")
    fun `CN25 - GET Validar contrato e paginação 'top-album-plataformas' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN25 - GET Validar contrato e paginação 'top-album-plataformas' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsPlataformas(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            Id=1,
            paramId="distribuicaoId",
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_ALBUM_PLATAFORMAS,
            contract = "schemas/top-musica-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-plays-semana – validar todas as páginas e contrato JSON")
    fun `CN26 - GET Validar contrato e paginação 'top-plays-semana' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN26 - GET Validar contrato e paginação 'top-plays-semana' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsAlbuns(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOP_PLAYS_SEMANA,
            contract = "schemas/top-plays-semana-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/top-regioes – validar todas as páginas e contrato JSON")
    fun `CN27 - GET Validar contrato e paginação 'top-regioes' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN27 - GET Validar contrato e paginação 'top-regioes' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsRegioes(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            qde_por_pagina=perPage,
            pagina =page,
            path = PATH_ANALYTICS_TOP_REGIOES,
            contract = "schemas/top-regioes-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/plays-remunerados – validar todas as páginas e contrato JSON")
    fun `CN28 - GET Validar contrato e paginação 'plays-remunerados' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN28 - GET Validar contrato e paginação 'plays-remunerados' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsRegioes(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            qde_por_pagina=perPage,
            pagina =page,
            path = PATH_ANALYTICS_TOP_PLAYS_REMUNERADOS,
            contract = "schemas/top-regioes-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }

    @Test
    @Tag("smokeTests")
    @DisplayName("HTTPS 200 GET /analytics/total-plays-periodo – validar todas as páginas e contrato JSON")
    fun `CN29 - GET Validar contrato e paginação 'total-plays-periodo' 200`() {

        val dataInicial = "2025-06-01"
        val dataFinal = "2025-06-29"
        val perPage = 5
        var page = 0

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("🧪 CN29 - GET Validar contrato e paginação 'total-plays-periodo' 200")
        LogCollector.println("📅 Período: $dataInicial /$dataFinal")
        LogCollector.println("📄 Registros por página: $perPage")
        LogCollector.println("════════════════════════════════════════════════════")

        val responses = endpointTopsAlbuns(
            dataInicial = dataInicial,
            dataFinal=dataFinal,
            perPage=perPage,
            page =page,
            path = PATH_ANALYTICS_TOTAL_PLAYS_PERIODO,
            contract = "schemas/total-plays-periodo-schema.json")

        LogCollector.println("\n════════════════════════════════════════════════════")
        LogCollector.println("✅ Validações por página e por item")

        // ================================
        // 1️⃣ Validações por página + item
        // ================================
        responses.forEachIndexed { pagina, resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")

            validarPagina(dados, pagina, perPage)

            // ================================
            // 2️⃣ Validações especificas para os campos: "required": XXXXXXXXXX
            // ================================
            dados.forEach { item ->
                // TODO: Avaliar quais campos são
                LogCollector.println("✔ Item validados com sucesso: $item")
            }
        }

        // ================================
        // 2️⃣ Validações globais
        // ================================
        validarIdsUnicos(responses)
        validarSomaPlays(responses)

        LogCollector.println("🏁 TESTE FINALIZADO COM SUCESSO")

    }



    /**
     * Endpoints reutilizáveis
     */
    private fun endpointTops(mesInicial: Int, anoInicial: Int, perPage: Int, page: Int, path: String, contract: String): List<ValidatableResponse> {
        val responses = mutableListOf<ValidatableResponse>()
        // ================================
        // 1️⃣ Primeira request
        // ================================
        val firstResponse = givenTop()
            .contentType(ContentType.JSON)
            .queryParam("mesInicial", mesInicial)
            .queryParam("anoInicial", anoInicial)
            .queryParam("mesFinal", mesInicial)
            .queryParam("anoFinal", anoInicial)
            .queryParam("page", page)
            .queryParam("perpage", perPage)
            .get(path)
            .then()
            .log().all()
            .statusCode(200)
            .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))
            .extract()

        val totalRegistros = firstResponse.jsonPath().getInt("qde_registros")
        val totalPaginas =
            if (totalRegistros == 0) 1 else ((totalRegistros + perPage - 1) / perPage)

        LogCollector.println("🔎 Total registros: $totalRegistros")
        LogCollector.println("📄 Total páginas: $totalPaginas\n")

        // ================================
        // 2️⃣ Loop
        // ================================
        for (paginaAtual in 0 until totalPaginas) {

            LogCollector.println("🔍 Consultando dados da página $paginaAtual")

            val response = givenTop()
                .contentType(ContentType.JSON)
                .queryParam("mesInicial", mesInicial)
                .queryParam("anoInicial", anoInicial)
                .queryParam("mesFinal", mesInicial)
                .queryParam("anoFinal", anoInicial)
                .queryParam("page", paginaAtual)
                .queryParam("perpage", perPage)
                .get(path)
                .then()
                //.log().all()
                .statusCode(200)
                .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))

            var linhas = response.extract().jsonPath().getList<Map<String, Any>>("dados")

            assertTrue(
                linhas.size <= perPage,
                "Página $paginaAtual excedeu o limite de $perPage registros"
            )

            // Se não for a última página -> deve ter perPage registros
            if (paginaAtual < totalPaginas - 1 && totalRegistros > perPage) {
                assertEquals(
                    perPage,
                    linhas.size,
                    "Página $paginaAtual deveria vir com $perPage registros"
                )
            }

            responses.add(response)
        }

        return responses
    }
    private fun endpointTopsPlataformas(dataInicial: String, dataFinal: String, Id: Int, paramId: String, perPage: Int, page: Int, path: String, contract: String): List<ValidatableResponse> {
        val responses = mutableListOf<ValidatableResponse>()
        // ================================
        // 1️⃣ Primeira request
        // ================================
        val firstResponse = givenTop()
            .contentType(ContentType.JSON)
            .queryParam("dataInicial", dataInicial)
            .queryParam("dataFinal", dataFinal)
            .queryParam("$paramId", Id)
            .queryParam("page", page)
            .queryParam("perpage", perPage)
            .get(path)
            .then()
            .log().all()
            .statusCode(200)
            .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))
            .extract()

        val totalRegistros = firstResponse.jsonPath().getInt("qde_registros")
        val totalPaginas =
            if (totalRegistros == 0) 1 else ((totalRegistros + perPage - 1) / perPage)

        LogCollector.println("🔎 Total registros: $totalRegistros")
        LogCollector.println("📄 Total páginas: $totalPaginas\n")

        // ================================
        // 2️⃣ Loop
        // ================================
        for (paginaAtual in 0 until totalPaginas) {

            LogCollector.println("🔍 Consultando dados da página $paginaAtual")

            val response = givenTop()
                .contentType(ContentType.JSON)
                .queryParam("dataInicial", dataInicial)
                .queryParam("dataFinal", dataFinal)
                .queryParam("$paramId", Id)
                .queryParam("page", page)
                .queryParam("perpage", perPage)
                .get(path)
                .then()
                //.log().all()
                .statusCode(200)
                .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))

            var linhas = response.extract().jsonPath().getList<Map<String, Any>>("dados")

            assertTrue(
                linhas.size <= perPage,
                "Página $paginaAtual excedeu o limite de $perPage registros"
            )

            // Se não for a última página -> deve ter perPage registros
            if (paginaAtual < totalPaginas - 1 && totalRegistros > perPage) {
                assertEquals(
                    perPage,
                    linhas.size,
                    "Página $paginaAtual deveria vir com $perPage registros"
                )
            }

            responses.add(response)
        }

        return responses
    }
    private fun endpointTopsAlbuns(dataInicial: String, dataFinal: String, perPage: Int, page: Int, path: String, contract: String): List<ValidatableResponse> {
        val responses = mutableListOf<ValidatableResponse>()
        // ================================
        // 1️⃣ Primeira request
        // ================================
        val firstResponse = givenTop()
            .contentType(ContentType.JSON)
            .queryParam("dataInicial", dataInicial)
            .queryParam("dataFinal", dataFinal)
            .queryParam("page", page)
            .queryParam("perpage", perPage)
            .get(path)
            .then()
            .log().all()
            .statusCode(200)
            .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))
            .extract()

        val totalRegistros = firstResponse.jsonPath().getInt("qde_registros")
        val totalPaginas =
            if (totalRegistros == 0) 1 else ((totalRegistros + perPage - 1) / perPage)

        LogCollector.println("🔎 Total registros: $totalRegistros")
        LogCollector.println("📄 Total páginas: $totalPaginas\n")

        // ================================
        // 2️⃣ Loop
        // ================================
        for (paginaAtual in 0 until totalPaginas) {

            LogCollector.println("🔍 Consultando dados da página $paginaAtual")

            val response = givenTop()
                .contentType(ContentType.JSON)
                .queryParam("dataInicial", dataInicial)
                .queryParam("dataFinal", dataFinal)
                .queryParam("page", page)
                .queryParam("perpage", perPage)
                .get(path)
                .then()
                //.log().all()
                .statusCode(200)
                .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))

            var linhas = response.extract().jsonPath().getList<Map<String, Any>>("dados")

            assertTrue(
                linhas.size <= perPage,
                "Página $paginaAtual excedeu o limite de $perPage registros"
            )

            // Se não for a última página -> deve ter perPage registros
            if (paginaAtual < totalPaginas - 1 && totalRegistros > perPage) {
                assertEquals(
                    perPage,
                    linhas.size,
                    "Página $paginaAtual deveria vir com $perPage registros"
                )
            }

            responses.add(response)
        }

        return responses
    }
    private fun endpointTopsPlayList(perPage: Int, page: Int, path: String,contract: String): List<ValidatableResponse> {
        val responses = mutableListOf<ValidatableResponse>()
        // ================================
        // 1️⃣ Primeira request
        // ================================
        val firstResponse = givenTop()
            .contentType(ContentType.JSON)
            .queryParam("page", page)
            .queryParam("perpage", perPage)
            .get(path)
            .then()
            .log().all()
            .statusCode(200)
            .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))
            .extract()

        val totalRegistros = firstResponse.jsonPath().getInt("qde_registros")
        val totalPaginas =
            if (totalRegistros == 0) 1 else ((totalRegistros + perPage - 1) / perPage)

        LogCollector.println("🔎 Total registros: $totalRegistros")
        LogCollector.println("📄 Total páginas: $totalPaginas\n")

        // ================================
        // 2️⃣ Loop
        // ================================
        for (paginaAtual in 0 until totalPaginas) {

            LogCollector.println("🔍 Consultando dados da página $paginaAtual")

            val response = givenTop()
                .contentType(ContentType.JSON)
                .queryParam("page", paginaAtual)
                .queryParam("perpage", perPage)
                .get(path)
                .then()
                //.log().all()
                .statusCode(200)
                .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))

            var linhas = response.extract().jsonPath().getList<Map<String, Any>>("dados")

            assertTrue(
                linhas.size <= perPage,
                "Página $paginaAtual excedeu o limite de $perPage registros"
            )

            // Se não for a última página -> deve ter perPage registros
            if (paginaAtual < totalPaginas - 1 && totalRegistros > perPage) {
                assertEquals(
                    perPage,
                    linhas.size,
                    "Página $paginaAtual deveria vir com $perPage registros"
                )
            }

            responses.add(response)
        }

        return responses
    }
    private fun endpointTopsRegioes(dataInicial: String, dataFinal: String, qde_por_pagina: Int, pagina: Int, path: String, contract: String): List<ValidatableResponse> {
        val responses = mutableListOf<ValidatableResponse>()
        // ================================
        // 1️⃣ Primeira request
        // ================================
        val firstResponse = givenTop()
            .contentType(ContentType.JSON)
            .queryParam("dataInicial", dataInicial)
            .queryParam("dataFinal", dataFinal)
            .queryParam("pagina", pagina)
            .queryParam("qde_por_pagina", qde_por_pagina)
            .get(path)
            .then()
            .log().all()
            .statusCode(200)
            .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))
            .extract()

        val totalRegistros = firstResponse.jsonPath().getInt("qde_registros")
        val totalPaginas =
            if (totalRegistros == 0) 1 else ((totalRegistros + qde_por_pagina - 1) / qde_por_pagina)

        LogCollector.println("🔎 Total registros: $totalRegistros")
        LogCollector.println("📄 Total páginas: $totalPaginas\n")

        // ================================
        // 2️⃣ Loop
        // ================================
        for (paginaAtual in 0 until totalPaginas) {

            LogCollector.println("🔍 Consultando dados da página $paginaAtual")

            val response = givenTop()
                .contentType(ContentType.JSON)
                .queryParam("dataInicial", dataInicial)
                .queryParam("dataFinal", dataFinal)
                .queryParam("pagina", pagina)
                .queryParam("qde_por_pagina", qde_por_pagina)
                .get(path)
                .then()
                //.log().all()
                .statusCode(200)
                .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("$contract"))

            var linhas = response.extract().jsonPath().getList<Map<String, Any>>("dados")

            assertTrue(
                linhas.size <= qde_por_pagina,
                "Página $paginaAtual excedeu o limite de $qde_por_pagina registros"
            )

            // Se não for a última página -> deve ter perPage registros
            if (paginaAtual < totalPaginas - 1 && totalRegistros > qde_por_pagina) {
                assertEquals(
                    qde_por_pagina,
                    linhas.size,
                    "Página $paginaAtual deveria vir com $qde_por_pagina registros"
                )
            }

            responses.add(response)
        }

        return responses
    }

    /**
     * Funções reutilizáveis
     */
    fun validarPagina(
        dados: List<Map<String, Any>>,
        pagina: Int,
        perPage: Int
    ) {
        LogCollector.println("📄 Validando página $pagina")

        assertTrue(
            dados.size <= perPage,
            "❌ Página $pagina excedeu o limite de $perPage registros"
        )

        // Ordenação por plays DESC
        val playsList = dados.map { (it["plays"] as Number).toLong() }
        assertTrue(
            playsList == playsList.sortedDescending(),
            "❌ Página $pagina não está ordenada por plays DESC"
        )

        LogCollector.println("✔ Página $pagina validada com sucesso")
    }
    fun validarIdsUnicos(
        responses: List<ValidatableResponse>,
        idField: String = "id"
    ) {
        LogCollector.println("🔎 Validando IDs únicos entre páginas")

        val ids = mutableSetOf<Long>()

        responses.forEach { resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")
            dados.forEach { item ->
                val id = (item[idField] as Number).toLong()
                assertTrue(
                    ids.add(id),
                    "❌ ID duplicado encontrado entre páginas: $id"
                )
            }
        }

        LogCollector.println("✔ Nenhum ID duplicado encontrado")
    }
    fun validarSomaPlays(
        responses: List<ValidatableResponse>,
        campoSomaApi: String = "somaTotalRegistros",
        campoPlays: String = "plays"
    ) {
        LogCollector.println("📊 Validando soma total de plays")

        val somaTotalApi =
            responses.first().extract().jsonPath().getLong(campoSomaApi)

        val somaCalculada = responses.sumOf { resp ->
            val dados = resp.extract().jsonPath().getList<Map<String, Any>>("dados")
            dados.sumOf { (it[campoPlays] as Number).toLong() }
        }

        LogCollector.println("➕ Soma calculada: $somaCalculada")
        LogCollector.println("📊 Soma informada pela API: $somaTotalApi")

        assertEquals(
            somaTotalApi,
            somaCalculada,
            "❌ Soma dos plays difere do valor informado pela API"
        )

        LogCollector.println("✔ Soma total validada com sucesso")
    }
    fun validarItemTopPlays(item: Map<String, Any>) {

        assertNotNull(item["id"], "❌ Campo id não pode ser null")

        val plays = (item["plays"] as Number).toLong()
        assertTrue(
            plays >= 0,
            "❌ Campo plays não pode ser negativo: $plays"
        )

        assertTrue(
            (item["titulo"] as String).isNotBlank(),
            "❌ Campo titulo não pode ser vazio"
        )

        assertTrue(
            (item["referencia"] as String).matches(Regex("\\d{2}/\\d{4}")),
            "❌ Campo referencia deve estar no formato MM/yyyy"
        )
    }




    /**
     * Endpoint cenários de exceções
        *
            → top-plays-wl
     */
    @Test
    @Tag("smokeTests") // OK
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
    @Tag("smokeTests") // TODO: retornando 500 ao invez de 400 com motivo da falha
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
    @Tag("smokeTests") // TODO: retornando 200 ao invez de 400 com motivo da falha
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
    @Tag("smokeTests") // TODO: retornando 200 ao invez de 404
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