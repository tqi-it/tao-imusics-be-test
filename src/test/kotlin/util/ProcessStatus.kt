package util

import io.restassured.RestAssured
import io.restassured.http.ContentType
import io.restassured.listener.ResponseValidationFailureListener
import io.restassured.matcher.ResponseAwareMatcher
import io.restassured.response.Response
import io.restassured.response.ValidatableResponse
import org.apache.http.HttpStatus
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

object ProcessStatus {

    fun aguardarProcessoCompleto(token: String) {
        Awaitility.await()
            .atMost(90, TimeUnit.MINUTES)
            .pollInterval(15, TimeUnit.SECONDS)
            .ignoreExceptions()
            .until {
                val resp = RestAssured.given()
                    .contentType(ContentType.JSON)
                    .header("origin", "http://localhost")
                    .header("authorization", "Bearer $token")
                    .get("/process-status")
                    .then()
                    .log().all()
                    .statusCode(200)

                val status = resp.extract().jsonPath().getString("status")
                val message = resp.extract().jsonPath().getString("message")
                val progress = resp.extract().jsonPath().getString("progress_percent")
                LogCollector.println("⏳ Status atual → $status | ⏳ Mensagem → $message | ⏳ Progresso → $progress% ")

                TimestampValidation.validarTimestampPresente(
                    campo = "started_at",
                    valor = resp?.extract()?.jsonPath()?.getString("started_at")
                )

                historicoProgresso.add(
                    ProgressoEtapa(
                        message = "$message | $progress%"
                    )
                )

                status.equals("completed", ignoreCase = true)
            }
    }

    fun processStatus(token: String)  : ValidatableResponse? {
        return RestAssured.given()
                    .contentType(ContentType.JSON)
                    .header("origin", "http://localhost")
                    .header("authorization", "Bearer $token")
                    .get("/process-status")
                    .then()
                    .log().all()
                    .statusCode(200)
    }


    data class ProgressoEtapa(
        val message: String
    )
    val historicoProgresso = mutableListOf<ProgressoEtapa>()

    fun imprimirHistorico() {
        LogCollector.println("\n📘 HISTÓRICO DO PROCESSO")
        historicoProgresso
            .distinct()
            .forEach { item ->
            LogCollector.println(" → ${item.message}")
        }
    }










}