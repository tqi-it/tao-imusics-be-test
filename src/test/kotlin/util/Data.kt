package util.configs

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class Data {
    companion object {

        const val APPLICATION = "application"
        const val DATA = "data"

        /**
         * URLs Services
         */
        val URL_: String = getProperties(APPLICATION, "url.responsys")

        // DADOS
        val USER_PASS: String = getProperties(DATA, "user.pass")


        val TIME_SLEEP: Long = getProperties(APPLICATION, "time.sleep").toLong()

        /**
         * Utilizado para obter parametros de acordo com cada ambiente
         */
        fun getProperties(parameterType: String, parameterName: String): String {
            val profile: String = Env.get() ?: throw Exception("Faltou passar o profile (-Dprofile=ambiente)")
            val propertyInputStream = this::class.java.classLoader.getResourceAsStream("$parameterType-$profile.properties")
            val property = Properties()
            property.load(propertyInputStream)
            println(parameterName)
            return property.getProperty(parameterName)
        }

        /**
         * Realizar replace em variáveis
         */
        fun replaceValue(parametroReplace: String, valueReplace: String? = " "):String{
            return parametroReplace.replace("$valueReplace","")
                .replace("{","")
                .replace("}","")
                .replace("[","")
                .replace("]","")
                .replace(" ","")
        }

    }
}