package util

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class Data {
    companion object {

        const val APPLICATION = "application"
        const val DATA = "data"

        const val BASE_URL = "http://localhost:3015"
        const val DIR_SUMMARY_DUMP = "temp/summary-dump"
        const val DIR_TEST_RESULT = "temp/test-results"
        const val DIR_TEMP="/tmp"
        const val DIR_REPORT_DOCS = "docs"
        const val PATH_PROCESS = "/start-process"

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


    }
}