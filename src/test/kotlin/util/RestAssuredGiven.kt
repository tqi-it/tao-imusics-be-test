package util

import io.restassured.RestAssured.given
import io.restassured.http.ContentType



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
                  "email": "superadmin@taomusic.com.br",
                  "senha": "tao001"
                }
            """.trimIndent()
fun givenOauth() =
    given()
        .contentType(ContentType.JSON)
        .header("origin", "http://localhost")
        .body(loginBody)
        .post("/auth/login")
        .then()
        .log().all()
        .statusCode(200)
        .extract()
