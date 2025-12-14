package util

import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import util.Data.Companion.PASS_LOGIN
import util.Data.Companion.USER_LOGIN


/**
 *  Given em comum
 */

// Body
private var startDate ="2025-09-28"
private var endDate ="2025-09-28"
val requestBodyStartProcess = """
            {
              "start-date": "$startDate",
              "end-date": "$endDate"
            }
        """.trimIndent()


// Login Oauth
val loginBody = """
                {
                  "grant_type": "client_credentials",
                  "email": "$USER_LOGIN",
                  "senha": "$PASS_LOGIN"
                }
            """.trimIndent()
fun givenOauth() =
    given()
        .contentType(ContentType.JSON)
        .header("origin", "http://localhost")
        .body(loginBody)
        .log().all()
        .post("/auth/login")
        .then()
        .log().all()
        .statusCode(200)
        .extract()

