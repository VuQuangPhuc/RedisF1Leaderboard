package de.tinf17c.db2.redis.example

import io.lettuce.core.*
import io.lettuce.core.api.sync.RedisCommands
import java.io.File
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis


class Main

fun main(args: Array<String>) {
    val filename = "output.csv"

    if (args.contains("--help")) {
        println("The application could be called with the following options:\n")
        println("""
            --help              Show this help
            
            --rebuild           Reset the database
            --extract           only used when --rebuild is set, extracts the race with id 1030
            --target FILENAME   only used when --extract is set, specifies file for extracted data, defaults to $filename
            
            --file   FILENAME   specify which file contains current race, if not set $filename is used
                                when --extract and --target are set, --target will be used
        """.trimIndent())
        exitProcess(0)
    }

    val redisUri = RedisURI.Builder.redis("localhost").build()
    val client = RedisClient.create(redisUri)
    val connection = client.connect()
    val commandsAsync = connection.async()
    val commandsSync = connection.sync()

    if (args.contains("--rebuild")) {
        val futures = mutableListOf<RedisFuture<out Any>>()
        var firstLine = true
        commandsSync.flushall()
        val time = measureTimeMillis {
            Main::class.java.getResourceAsStream("/drivers.csv").bufferedReader().lines().forEach { driver ->
                if (firstLine) {
                    firstLine = false
                    return@forEach
                }
                val spliced = driver.split(",")
                val id = spliced[0].toInt()
                val firstname = spliced[4].substring(1, (spliced[4].length - 1))
                val lastname = spliced[5].substring(1, (spliced[5].length - 1))

                futures.add(commandsAsync.set("driver:$id", "$firstname $lastname"))
            }

            firstLine = true
            Main::class.java.getResourceAsStream("/lap_times.csv").bufferedReader().lines().forEach { time ->
                if (firstLine) {
                    firstLine = false
                    return@forEach
                }
                val spliced = time.split(",")
                val raceId = spliced[0].toInt()
                val driverId = spliced[1].toInt()
                val lapInt = spliced[2].toInt()
                val lap = lapInt.toTwoDigitString()
                val milliseconds = spliced[5].toDouble()
                futures.add(commandsAsync.zincrby("race:$raceId:lap:$lap", milliseconds, "$driverId"))
                futures.add(commandsAsync.zincrby("race:$raceId:fulltime", milliseconds, "$driverId"))
            }

            firstLine = true
            Main::class.java.getResourceAsStream("/circuits.csv").bufferedReader().lines().forEach { circuit ->
                if (firstLine) {
                    firstLine = false
                    return@forEach
                }
                val spliced = circuit.split(",")
                val circuitId = spliced[0].toInt()
                val name = spliced[2].removeSurroundingQuotes()
                futures.add(commandsAsync.set("circuit:$circuitId", name))
            }

            firstLine = true
            Main::class.java.getResourceAsStream("/races.csv").bufferedReader().lines().forEach { races ->
                if (firstLine) {
                    firstLine = false
                    return@forEach
                }
                val spliced = races.split(",")
                val raceId = spliced[0].toInt()
                val name = spliced[4].removeSurroundingQuotes()
                val date = spliced[5].removeSurroundingQuotes()
                val circuitId = spliced[3].toInt()
                futures.add(commandsAsync.set("race:$raceId:name", name))
                futures.add(commandsAsync.set("race:$raceId:date", date))
                futures.add(commandsAsync.set("race:$raceId:circuitId", "$circuitId"))
            }

            println("Insert statements: ${futures.size}")
            LettuceFutures.awaitAll(Duration.ofMinutes(10), *futures.toTypedArray())
            println("Inserted to Redis")
        }

        println("Insert time: $time")

        val lapKeys = commandsSync.getMatchingKeys("race:1030:lap:*")
        lapKeys.sort()

        if (args.contains("--extract")) {
            val outputFileName = if (args.contains("--target")) {
                args[args.indexOf("--target") + 1]
            } else {
                filename
            }

            val output = File(outputFileName)
            output.writeText("race,lap,driver,time\n")
            lapKeys.forEach { lapKey ->
                val lapNo = lapKey.replace("race:1030:lap:", "").toInt()
                val lap = commandsSync.zrangebyscoreWithScores(lapKey, Range.create(0, Int.MAX_VALUE))
                lap.forEach { driver ->
                    output.appendText("1030,$lapNo,${driver.value},${driver.score}\n")
                }
            }
        }
    }
    val lapKeys = commandsSync.getMatchingKeys("race:1030:lap:*")

    commandsSync.unlink(*lapKeys.toTypedArray(), "race:1030:fulltime")

    val inputFileName = if (args.contains("--file")) {
        args[args.indexOf("--file") + 1]
    } else if (args.contains("--extract") && args.contains("--target")) {
        args[args.indexOf("--target") + 1]
    } else {
        filename
    }

    var firstLine = true
    var currLap = 1
    File(inputFileName).forEachLine { line ->
        if (firstLine) {
            firstLine = false
            return@forEachLine
        }
        val spliced = line.split(",")
        val raceId = spliced[0].toInt()
        val lapInt = spliced[1].toInt()
        val lap = lapInt.toTwoDigitString()
        val driver = spliced[2].toInt()
        val time = spliced[3].toDouble()

        if (lapInt != currLap) {
            val currLapString = currLap.toTwoDigitString()
            commandsSync.publish("new-lap", currLapString)
            println("pulished lap-no $currLapString")
            sleep(1000)

            currLap = lapInt
        }
        commandsSync.zincrby("race:$raceId:lap:$lap", time, "$driver")
        commandsSync.zincrby("race:$raceId:fulltime", time, "$driver")
    }

    val currLapString = currLap.toTwoDigitString()
    commandsSync.publish("new-lap", currLapString)
    println("pulished lap-no $currLapString")
//    val test2 = commands.zrangeWithScores("race:841:fulltime", 0L, -1L)
//    println(test2)
}

fun RedisCommands<out Any, out Any>.getMatchingKeys(key: String): MutableList<String> {
    val list = mutableListOf<String>()

    var cur: KeyScanCursor<out Any> = KeyScanCursor()
    cur.cursor = "0"
    cur.isFinished = false
    val args = ScanArgs.Builder.matches(key)
    while (!cur.isFinished) {
        cur = this.scan(cur, args)
        list.addAll(cur.keys.map { it.toString() })
    }

    return list
}

fun String.removeSurroundingQuotes(): String {
    return this.removeSurrounding("\"")
}

fun Int.toTwoDigitString(): String {
    return if (this > 9) {
        "$this"
    } else {
        "0$this"
    }
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
