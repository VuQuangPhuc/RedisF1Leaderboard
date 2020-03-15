package de.tinf17c.db2.redis.frontend

import io.lettuce.core.Range
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.pubsub.RedisPubSubListener
import org.slf4j.LoggerFactory
import org.springframework.messaging.simp.SimpMessagingTemplate

class Listener(private val messaging: SimpMessagingTemplate,
               private val commands: RedisCommands<String, String>) : RedisPubSubListener<String, String> {
    private val logger = LoggerFactory.getLogger(Listener::class.java)

    override fun psubscribed(pattern: String, count: Long) {
        logger.info("psubscribed $pattern, $count")
    }

    override fun punsubscribed(pattern: String, count: Long) {
        logger.info("punsubscribed $pattern, $count")
    }

    override fun unsubscribed(channel: String, count: Long) {
        logger.info("unsubscribed $channel, $count")
    }

    override fun subscribed(channel: String, count: Long) {
        logger.info("subscribed $channel, $count")
    }

    override fun message(channel: String, message: String) {
        if (channel == "new-lap") {
            loadMessages(message)
        }
    }

    override fun message(pattern: String, channel: String, message: String) {
        if (channel == "new-lap") {
            loadMessages(message)
        }
    }

    private fun loadMessages(lap: String) {
        logger.info("got lap-no $lap")
        val lapTimes = commands.zrangebyscoreWithScores("race:1030:lap:$lap", Range.create(0, Double.MAX_VALUE))
        val fullTimes = commands.zrangebyscoreWithScores("race:1030:fulltime", Range.create(0, Double.MAX_VALUE))

        val standings = fullTimes.map { value ->
            val driverName = commands.get("driver:${value.value}")
            val lapTime = lapTimes.find { it.value == value.value }
            if (lapTime != null) {
                Times.Time(driverName, lapTime.score.toTimeString(), value.score.toTimeString(), value.value)
            } else {
                Times.Time(driverName, null, value.score.toTimeString(), value.value)
            }
        }.toMutableList().sorted()

        messaging.convertAndSend("/times/lap", Times(lap.toInt(), standings))
    }
}
