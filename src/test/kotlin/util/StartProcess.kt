package util

import io.restassured.RestAssured
import io.restassured.http.ContentType
import io.restassured.response.ValidatableResponse


object StartProcess {

    fun PostStartProcess(startDate: String,endDate: String, token:String) : ValidatableResponse? {
        val requestBody = """
            {
              "start-date": "$startDate",
              "end-date": "$endDate"
            }
        """.trimIndent()

        return RestAssured.given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .log().all()
            .body(requestBody)
            .post(Data.PATH_PROCESS)
            .then()
            .log().all()
    }

    fun PostStartProcessNotDate(token:String) : ValidatableResponse? {
        return RestAssured.given()
            .contentType(ContentType.JSON)
            .header("origin", "http://localhost")
            .header("authorization", "Bearer $token")
            .log().all()
            .post(Data.PATH_PROCESS)
            .then()
            .log().all()
    }


}