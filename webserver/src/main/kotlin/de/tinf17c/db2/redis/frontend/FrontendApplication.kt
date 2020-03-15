package de.tinf17c.db2.redis.frontend

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

@SpringBootApplication
@EnableScheduling
class FrontendApplication {
    private val logger = LoggerFactory.getLogger(FrontendApplication::class.java)

    private val client = RedisClient.create(RedisURI.Builder.redis("localhost").build())
    private val connection = client.connect()
    private val pubSub = client.connectPubSub()
    private val pubSubCommandsAsync = pubSub.async()
    private val commands = connection.sync()
    private val commandsAsync = connection.async()

    @Autowired
    private lateinit var messaging: SimpMessagingTemplate

    @PostConstruct
    fun postConstruct() {
        pubSub.addListener(Listener(messaging, commands))
        pubSubCommandsAsync.subscribe("new-lap")
    }
}

fun main(args: Array<String>) {
    runApplication<FrontendApplication>(*args)
}

fun Long.toTimeString(): String {
    return String.format(
            "%02d:%02d:%02d.%03d", TimeUnit.MILLISECONDS.toHours(this),
            TimeUnit.MILLISECONDS.toMinutes(this) % TimeUnit.HOURS.toMinutes(1),
            TimeUnit.MILLISECONDS.toSeconds(this) % TimeUnit.MINUTES.toSeconds(1),
            this % TimeUnit.SECONDS.toMillis(1)
    )
}

fun Double.toTimeString(): String {
    return this.toLong().toTimeString()
}
