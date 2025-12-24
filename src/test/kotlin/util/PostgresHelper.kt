package util

import util.Data.Companion.pgPass
import util.Data.Companion.pgUrl
import util.Data.Companion.pgUser
import java.sql.Connection
import java.sql.DriverManager

object PostgresHelper {

    fun getConnection(): Connection {
        return DriverManager.getConnection(pgUrl, pgUser, pgPass)
    }

    fun querySingleLong(sql: String): Long {
        getConnection().use { conn ->
            conn.createStatement().use { st ->
                val rs = st.executeQuery(sql)
                if (rs.next()) {
                    return rs.getLong(1)
                }
            }
        }
        return 0L
    }

    fun contarTopPlaysNoPostgresPorMes(
        ano: Int,
        mes: Int
    ): Long {

        val sql = """
        SELECT COUNT(1)
        FROM top_plays
        WHERE EXTRACT(YEAR FROM reference_date) = $ano
          AND EXTRACT(MONTH FROM reference_date) = $mes
    """.trimIndent()
        val total = querySingleLong(sql)
        LogCollector.println("🐘 Postgres → Total registros $mes/$ano: $total")
        return total
    }

    fun somarPlaysNoPostgresPorMes(
        ano: Int,
        mes: Int
    ): Long {

        val sql = """
        SELECT COALESCE(SUM(plays), 0)
        FROM top_plays
        WHERE EXTRACT(YEAR FROM reference_date) = $ano
          AND EXTRACT(MONTH FROM reference_date) = $mes
    """.trimIndent()

        val total = PostgresHelper.querySingleLong(sql)

        LogCollector.println("🐘 Postgres → Soma plays $mes/$ano: $total")

        return total
    }





    /*
 private val ds = PGSimpleDataSource().apply {
     setURL("jdbc:postgresql://localhost:5432/imusics")
     user = "postgres"
     password = "postgres"
 }
 fun queryTopPlays(
     mesInicial: Int,
     anoInicial: Int,
     mesFinal: Int,
     anoFinal: Int
 ): List<TopPlaysRow> {

     val sql = """
         SELECT faixa_musical_id, soma_plays, titulo, capa, usuario, artistas, data_referencia, usuario_id
         FROM TOP_PLAYS
         WHERE mes BETWEEN ? AND ?
           AND ano BETWEEN ? AND ?
         ORDER BY soma_plays DESC
     """.trimIndent()

     return ds.connection.use { conn ->
         conn.prepareStatement(sql).use { st ->

             st.setInt(1, mesInicial)
             st.setInt(2, mesFinal)
             st.setInt(3, anoInicial)
             st.setInt(4, anoFinal)

             val rs = st.executeQuery()
             val list = mutableListOf<TopPlaysRow>()

             while (rs.next()) {
                 list.add(
                     TopPlaysRow(
                         rs.getLong("faixa_musical_id"),
                         rs.getLong("soma_plays"),
                         rs.getString("titulo"),
                         rs.getString("capa"),
                         rs.getString("usuario"),
                         rs.getString("artistas"),
                         rs.getTimestamp("data_referencia").toLocalDateTime(),
                         rs.getLong("usuario_id")
                     )
                 )
             }

             list
         }
     }
 }

  */
}
