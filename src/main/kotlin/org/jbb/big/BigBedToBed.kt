package org.jbb.big

import com.google.common.base.Joiner
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * A converter from BigBED to BED format.
 *
 * @link https://genome.ucsc.edu/FAQ/FAQformat.html#format1
 * @author Sergey Zherevchuk
 * @since 11/03/15
 */
public object BigBedToBed {
    /**
     * Main method to convert from BigBED to BED format
     *
     * @param inputPath Path to source *.bb file
     * @param queryChromName If set restrict output to given chromosome
     * @param queryStart If set, restrict output to only that over start. Should be zero by default.
     * @param queryEnd If set, restict output to only that under end. Should be zero to restrict by
     *                 chromosome size
     * @param maxItems If set, restrict output to first N items
     */
    public fun main(inputPath: Path, outputPath: Path, queryChromName: String,
                    queryStart: Int, queryEnd: Int, maxItems: Int) {
        BigBedFile.read(inputPath).use { bf ->
            Files.newBufferedWriter(outputPath).use { out ->
                var itemCount = 0
                for (chromName in bf.chromosomes) {
                    if (!queryChromName.isEmpty() && chromName != queryChromName) {
                        continue
                    }

                    var itemsLeft = 0  // zero - no limit
                    if (maxItems != 0) {
                        itemsLeft = maxItems - itemCount
                        if (itemsLeft <= 0) {
                            break
                        }
                    }

                    for (interval in bf.query(chromName, queryStart, queryEnd, itemsLeft)) {
                        out.write(Joiner.on('\t').join(chromName, interval.start, interval.end, interval.rest))
                        out.write("\n")
                        itemCount++
                    }
                }
            }
        }
    }
}
