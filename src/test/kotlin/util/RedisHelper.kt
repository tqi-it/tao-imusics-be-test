package util

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object RedisHelper {

    private val jedis = RedisClient.jedis // já configurado no seu projeto

    fun getTopPlays(whitelabelId: Long, data: LocalDate): MutableList<String>? {
        val key = "imusic:topplays:$whitelabelId:${data.format(DateTimeFormatter.ISO_DATE)}:rows" //imusic:topplays:<plataforma>:<date>:rows
        return jedis.lrange(key, 0, -1)
    }

    fun pushTopPlaysForWhitelabel(whitelabelId: Long, date: LocalDate, records: List<Map<String, Any?>>) {
        val key = getKey(whitelabelId, date)
        // reset
        jedis.del(key)
        val mapper = jacksonObjectMapper()
        records.forEach {
            jedis.rpush(key, mapper.writeValueAsString(it))
        }
    }

    fun clearTopPlays(whitelabelId: Long, date: LocalDate) {
        jedis.del(getKey(whitelabelId, date))
    }

    fun getKey(whitelabelId: Long, date: LocalDate): String {
        val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        return "imusic:topplays:wl:$whitelabelId:${date.format(fmt)}:rows" // ajuste se seu prefix for outro
    }

    fun count(key: String): Long = jedis.llen(key)

    fun close() { jedis.close() }
}