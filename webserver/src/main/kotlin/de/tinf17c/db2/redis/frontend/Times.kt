package de.tinf17c.db2.redis.frontend

import com.fasterxml.jackson.annotation.JsonIgnore
import org.slf4j.LoggerFactory

class Times (
        val lap: Int,
        val times: List<Time>
) {
    class Time (
            val driver: String,
            val lastLap: String?,
            val total: String,
            val driverId: String
    ): Comparable<Time> {
        override fun compareTo(other: Time): Int {
            return if (this.lastLap != null && other.lastLap == null) {
                -1
            } else if (this.lastLap == null && other.lastLap != null) {
                1
            } else if (this.lastLap == null && other.lastLap == null) {
                this.total.compareTo(other.total) * -1
            } else if (this.lastLap != null && other.lastLap != null) {
                this.total.compareTo(other.total)
            } else {
                0
            }
        }
    }
}
