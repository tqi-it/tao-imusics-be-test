package util

class Data {
    companion object {

        const val APPLICATION = "application"
        const val DATA = "data"

        const val BASE_URL_ANALYTICS = "http://localhost:3015"
        const val BASE_URL_BACKEND = "http://localhost:8080" // http://localhost:8080 | https://analytics-backend.dev.sintonize.me
        const val ORIGIN = "http://localhost" // http://localhost" | http://localhost:4302
        const val DIR_SUMMARY_DUMP = "temp/summary-dump"
        const val DIR_TEST_RESULT = "temp/test-results"
        const val DIR_TEMP="/tmp"
        const val DIR_REPORT_DOCS = "docs"
        const val PATH_PROCESS = "/start-process"
        const val PATH_PROCESS_ANALYTICS = "/start-analytics"

        // Endpoints
        const val PATH_ANALYTICS_TOP_PLAYS_WL = "/analytics/top-plays-wl"
        const val PATH_ANALYTICS_TOP_PLATAFORMAS = "/analytics/plataformas"
        const val PATH_ANALYTICS_TOP_PLATAFORMA = "/analytics/plataforma"
        const val PATH_ANALYTICS_TOP_PLAYLISTS = "/analytics/top-playlists"
        const val PATH_ANALYTICS_TOP_PLAYLIST = "/analytics/top-playlist"
        const val PATH_ANALYTICS_TOP_ALBUNS = "/analytics/top-albuns"
        const val PATH_ANALYTICS_TOP_ALBUM = "/analytics/top-album"
        const val PATH_ANALYTICS_TOP_MUSICAS= "/analytics/top-musicas"
        const val PATH_ANALYTICS_TOP_MUSICA= "/analytics/top-musica"
        const val PATH_ANALYTICS_TOP_ALBUM_MUSICA= "/analytics/top-album-musica"
        const val PATH_ANALYTICS_TOP_ALBUM_PLATAFORMAS= "/analytics/top-album-plataformas"
        const val PATH_ANALYTICS_TOP_PLAYS_SEMANA= "/analytics/top-plays-semana"
        const val PATH_ANALYTICS_TOP_REGIOES= "/analytics/top-regioes"
        const val PATH_ANALYTICS_TOP_PLAYS_REMUNERADOS= "/analytics/plays-remunerados"
        const val PATH_ANALYTICS_TOTAL_PLAYS_PERIODO= "/analytics/total-plays-periodo"

        // Campos Redis
        const val ASSERT_ID = "asset_id"
        const val PLATAFORM = "plataform"
        const val UCP = "upc"
        const val STATUS = "status"
        const val TERRITORY = "territory"
        const val NUMBER_OF_STREAMS = "number_of_streams"
        const val DATE = "date"
        const val STREAM_SOURCE = "stream_source"
        const val STREAM_SOURCE_URI = "stream_source_uri"

        val pgUrl =  "jdbc:postgresql://localhost:5436/imusics" // localhost:5436/imusics | dev-rds-tao-instance-1.c9aqeoq0mtx8.us-east-1.rds.amazonaws.com:5432/imusics-dev-temp
        val pgUser =  "postgres" // postgres | postgres
        val pgPass =  "postgres" //postgres | boHtmiiViSFMYSM221UJ

        // Users
        const val USER_LOGIN = "superadmin@taomusic.com.br" // superadmin@taomusic.com.br | equipetropado7music2023@gmail.com | brena@taomusic.com.br"
        const val PASS_LOGIN = "tao001" // tao001 | uJb7RgenrKrtuocqL2dqXoHTR24hGfaizzzTjHVvK5


    }
}