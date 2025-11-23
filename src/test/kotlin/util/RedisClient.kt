package util

import redis.clients.jedis.JedisPooled

object RedisClient {
    private val host = EnvLoader.get("REDIS_HOST")
    private val port = EnvLoader.get("REDIS_PORT")
    private val password = EnvLoader.get("REDIS_PASSWORD")

    val jedis: JedisPooled by lazy {
        if (password.isNullOrBlank()) {
            JedisPooled(host, port.toInt())
        } else {
            val uri = "redis://:$password@$host:$port"
            JedisPooled(uri)
        }
    }
}