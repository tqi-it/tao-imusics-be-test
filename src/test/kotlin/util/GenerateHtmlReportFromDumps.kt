package util

import `analytics-process`.UploadRedisOpenDataTest
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import util.Data.Companion.DIR_REPORT_DOCS
import util.Data.Companion.DIR_SUMMARY_DUMP
import util.Data.Companion.DIR_TEST_RESULT
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.math.abs

class GenerateHtmlReportFromDumps() {

    /**
     * Classe destinada para montar um report em HTML com o resultado das validações
        * expected.json e _from_redis.json
     */

    fun prettyNumber(n: Number): String {
        val value = n.toLong()
        return when {
            value >= 1_000_000 -> String.format("%.1fM", value / 1_000_000.0)
            value >= 1_000 -> String.format("%.1fK", value / 1_000.0)
            else -> value.toString()
        }
    }

    fun escapeHtml(s: String?): String {
        if (s == null) return ""
        return s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&#39;")
    }

   fun report(summaryKey: String, campo: String){
       val dumpDir = File(DIR_SUMMARY_DUMP)
       require(dumpDir.exists() && dumpDir.isDirectory) { "Dump dir inexistente: $DIR_TEST_RESULT" }

       val expectedFile = File(dumpDir, "${summaryKey}_expected.json")
       val redisFile = File(dumpDir, "${summaryKey}_from_redis.json")

       require(expectedFile.exists()) { "Arquivo esperado não encontrado: ${expectedFile.absolutePath}" }
       require(redisFile.exists()) { "Arquivo from_redis não encontrado: ${redisFile.absolutePath}" }

       val mapper = jacksonObjectMapper()

       // Map<String, Int>
       val expected: Map<String, Int> = mapper.readValue(expectedFile)
       val redisMap: Map<String, Int?> = mapper.readValue(redisFile) // valores podem ser null

       // diffs: key -> Pair(expected, redis)
       val diffs = expected.mapKeys { it.key }.mapValues { (k, v) ->
           val r = redisMap[k]
           Pair(v, r)
       }

       // listar tambem chaves que estão no redis e nao no expected
       val extraRedisKeys = redisMap.keys.filter { it !in expected.keys }

       // ordenar diffs por magnitude da diferença (abs), desc
       val diffsOrdered = diffs.entries.sortedByDescending { (_, pv) ->
           val (exp, red) = pv
           abs(exp - (red ?: 0))
       }

       // obter config dos campos (para sumarizar por campo)
       val config = UploadRedisOpenDataTest().getSumarizacaoConfig(summaryKey, campo)
       val groupFields = config.groupFields

       // inicializar summary por campo
       data class FieldStats(var count: Int = 0, var totalAbsDiff: Long = 0)
       val fieldSummary = groupFields.associateWith { FieldStats() }.toMutableMap()

       // analisar diferenças por campo
       diffsOrdered.forEach { (entryKey, pair) ->
           val (expectedStreams, redisStreamsNullable) = pair
           val redisStreams = redisStreamsNullable ?: 0
           if (expectedStreams != redisStreams) {
               val parts = entryKey.split("|")
               groupFields.forEachIndexed { idx, field ->
                   val valAt = parts.getOrNull(idx) ?: ""
                   // se diferiu no total, conta pro campo (mesmo se campo igual) — alternativa: só contar se campo estiver vazio/diferente
                   fieldSummary[field]!!.count += 1
                   fieldSummary[field]!!.totalAbsDiff += abs(expectedStreams - redisStreams).toLong()
               }
           }
       }

       // montar tabela HTML (limitar por exemplo top 5k linhas)
       val topList = diffsOrdered.take(5000)

       val now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

       val html = StringBuilder()
       html.append("""
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8"/>
          <title>Relatório Sumarização — ${summaryKey}</title>
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial; margin: 20px; color:#222; }
            h1,h2 { color: #003366; }
            .metrics { display:flex; gap:20px; flex-wrap:wrap; margin-bottom:20px; }
            .card { padding:12px 16px; border-radius:8px; background:#f7f9fb; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
            table { width:100%; border-collapse:collapse; margin-top:12px; }
            th, td { padding:8px 10px; border-bottom:1px solid #e7eef6; text-align:left; font-size:13px; }
            th { background:#f0f6fb; color:#004a7f; }
            tr.diff { background:#fff7f6; }
            .bad { color:#c00; font-weight:600; }
            .ok { color:#0a8a0a; font-weight:600; }
            .mono { font-family: "Courier New", monospace; font-size:12px; color:#333; }
            .small { font-size:12px; color:#666; }
            .summary-table td { width: 200px; }
            a.link { color:#0b66a3; text-decoration:none; }
          </style>
        </head>
        <body>
          <h1>Relatório de Sumarização — ${summaryKey}</h1>
          <p class="small">Gerado em: $now</p>

          <div class="metrics">
            <div class="card"><strong>Expected keys</strong><br/>${prettyNumber(expected.size)}</div>
            <div class="card"><strong>Redis keys</strong><br/>${prettyNumber(redisMap.size)}</div>
            <div class="card"><strong>Chaves divergentes</strong><br/>${prettyNumber(diffsOrdered.count { (_, p) -> p.first != (p.second ?: 0) })}</div>
            <div class="card"><strong>Chaves extras no Redis</strong><br/>${prettyNumber(extraRedisKeys.size)}</div>
            <div class="card"><strong>Arquivo expected</strong><br/><a class="link" href="${expectedFile.name}">${expectedFile.name}</a></div>
            <div class="card"><strong>Arquivo from_redis</strong><br/><a class="link" href="${redisFile.name}">${redisFile.name}</a></div>
          </div>

          <h2>Resumo por campo (somatório absoluto das diferenças)</h2>
          <table class="summary-table">
            <thead><tr><th>Campo</th><th>Qtde de chaves com divergência</th><th>Soma absoluta das diferenças</th></tr></thead>
            <tbody>
    """.trimIndent())

       fieldSummary.forEach { (field, stats) ->
           html.append("<tr><td>${field}</td><td>${prettyNumber(stats.count)}</td><td>${prettyNumber(stats.totalAbsDiff)}</td></tr>")
       }

       html.append("""
            </tbody>
          </table>

          <h2>Top divergências (ordenado por |expected - redis| descendente)</h2>
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>key</th>
                <th class="mono">expected</th>
                <th class="mono">redis</th>
                <th class="mono">abs(diff)</th>
              </tr>
            </thead>
            <tbody>
    """.trimIndent())

       topList.forEachIndexed { idx, entry ->
           val key = entry.key
           val expectedStreams = entry.value.first
           val redisStreams = entry.value.second ?: 0
           val absDiff = abs(expectedStreams - redisStreams)
           val rowCss = if (absDiff != 0) "class=\"diff\"" else ""
           html.append("""
            <tr $rowCss>
              <td>${idx + 1}</td>
              <td class="mono">${escapeHtml(key)}</td>
              <td class="mono">${expectedStreams}</td>
              <td class="mono">${redisStreams}</td>
              <td class="mono ${if (absDiff != 0) "bad" else "ok"}">${absDiff}</td>
            </tr>
        """.trimIndent())
       }

       html.append("""
            </tbody>
          </table>

          <h2>Observações</h2>
          <ul>
            <li>O key é composto pela concatenação dos campos de agrupamento, na ordem: ${groupFields.joinToString(", ")}.</li>
            <li>Se o valor do Redis aparecer como <code>null</code>, significa que a chave esperada não foi encontrada no Redis.</li>
          </ul>

        </body>
        </html>
    """.trimIndent())

       // salvar HTML
       val outFile = File(DIR_REPORT_DOCS, "${summaryKey}_report.html")
       outFile.writeText(html.toString(), Charsets.UTF_8)

       LogCollector.println("✅ Relatório HTML salvo em: ${outFile.absolutePath}")

   }

}


