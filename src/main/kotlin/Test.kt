@file:JvmName("Example")

package com.futureadvisor.algoio.kotlin.example

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import kotlin.reflect.KClass

//extension function to register Kotlin classes
fun SparkConf.registerKryoClasses(vararg args: KClass<*>) = registerKryoClasses(args.map { it.java }.toTypedArray())

data class MyItem(val id: Int, val value: String)

fun kingLearExample(sc: JavaSparkContext): Long {
    val home = System.getenv("HOME")
    val kingLearPath = "$home/Documents/Development/src/algoIO/kinglear.txt"

    val fileNameContentsRDD = sc.textFile(kingLearPath, 1).cache()
    val totalWords = fileNameContentsRDD.flatMap { it.split(Regex("[\n\r\t ]")).iterator() }

    return totalWords.count()
}

fun sumNumbers(sc: JavaSparkContext): Int {
    val items = listOf("123/643/7563/2134/ALPHA", "2343/6356/BETA/2342/12", "23423/656/343")

    val input = sc.parallelize(items)

    return input
        .flatMap { it.split("/").iterator() }
        .filter { it.matches(Regex("[0-9]+")) }
        .map { it.toInt() }
        .reduce { total, next -> total + next }
}

fun collectLetters(sc: JavaSparkContext): List<String> {
    val input = sc.parallelize(listOf(MyItem(1,"Alpha"), MyItem(2,"Beta")))

    val letters = input
            .flatMap { it.value.split(Regex("(?<=.)")).iterator() }
            .map(String::toUpperCase)
            .filter { it.matches(Regex("[A-Z]")) }

    return letters.collect()
}

fun main(args: Array<String>) {
    with(SparkConf()) {
        val master = if(args.isNotEmpty()) args[0] else "local"

        setMaster(master)
        setAppName("Kotlin Spark Test")

        registerKryoClasses(MyItem::class)

        JavaSparkContext(this).let {
            println("total: ${sumNumbers(it)}")
            println("words: ${kingLearExample(it)}")
            println("string: ${collectLetters(it)}")
        }
    }
}
