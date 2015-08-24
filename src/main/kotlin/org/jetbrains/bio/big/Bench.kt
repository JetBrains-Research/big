package org.jetbrains.bio.big

import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.PatternLayout
import java.nio.file.Paths
import kotlin.util.measureTimeMillis

fun main(args: Array<String>) {
    with(LogManager.getRootLogger()) {
        addAppender(ConsoleAppender(PatternLayout(), "System.out"))
        setLevel(Level.TRACE)
    }

    val wigs = WigParser(Paths.get("/home/user/Downloads/fix_me1_hg19_100_2_%2B.bw.wig.txt").bufferedReader())

    BigWigFile.write(wigs, Paths.get("/tmp/hg19.chrom.sizes").chromosomes(),
                     Paths.get("/tmp/foo.bb"))

    // chr13:[65862773, 69461833):
    // chr13:[64063243, 67662303)
//    BigWigFile.read(Paths.get("/tmp/foo.bb")).use { bwf ->
//        for (i in 0..9) {
//            bwf.summarize("chr13", 64063243, 67662303, numBins = 1592)
//        }
//
//        for (i in 0..9) {
//            bwf.summarize("chr13", 64063243, 67662303, numBins = 1592, index = false)
//        }
//
//        println(">>>")
//
//        LogManager.getRootLogger().time("index = true") {
//            bwf.summarize("chr13", 64063243, 67662303, numBins = 1592)
//        }
//
//        LogManager.getRootLogger().time("index = false") {
//            bwf.summarize("chr13", 64063243, 67662303, numBins = 1592, index = false)
//        }
//
//    }
}