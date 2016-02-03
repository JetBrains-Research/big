# big [![Build Status](https://travis-ci.org/JetBrains-Research/big.svg?branch=master)](https://travis-ci.org/JetBrains-Research/big) [![Build status](https://ci.appveyor.com/api/projects/status/e9q4o6rgdfhyy6ry?svg=true)](https://ci.appveyor.com/project/superbobry/big)

`big` implements high performance classes for reading and writing BigWIG,
BigBED and TDF. You can use `big` in any programming language running on the
JVM, but the API is in part Kotlin-specific.

Installation
------------

The latest version of `big` is available on [jCenter] [jcenter]. If you're using
Gradle just add the following to your `build.gradle`:

```gradle
repositories {
    jcenter()
}

dependencies {
    compile 'org.jetbrains.bio:big:0.2.5'
}

```

[jcenter]: https://bintray.com/bintray/jcenter

Example
-------

Download example [BED file] [bed-example] and hg19 [chromosome sizes] [chrom-sizes]
from the [BigBED] [bigbed] documention. Unfortunately the example file has a header
line which makes it improper BED. Remove the header either manually or via `tail`:

```bash
$ tail -n +2 ./bedExample.txt > example.bed
```

Showtime!

```kotlin
fun main(args: Array<String>) {
    val bedPath = Paths.get("./example.bed")
    val chromSizesPath = Paths.get("./hg19.chrom.sizes")
    val bigBedPath = Paths.get("./example.bb")

    // Convert a BED file to BigBED.
    BigBedFile.write(BedFile.read(bedPath), chromSizesPath.chromosomes(),
                     bigBedPath)

    // Iterate over entries in a BigBED file.
    BigBedFile.read(bigBedPath).use { bbf ->
        for (chromosome in bbf.chromosomes.valueCollection()) {
            for ((chrom, start, end) in bbf.query(chromosome)) {
                println("$chrom\t$start\t$end")
            }
        }
    }

    // Summarise the entries in a BigBED file.
    BigBedFile.read(bigBedPath).use { bbf ->
        println("Total: ${bbf.totalSummary}")

        for (chromosome in bbf.chromosomes.valueCollection()) {
            for ((i, summary) in bbf.summarize(chromosome, numBins = 10).withIndex()) {
                println("bin #${i + 1}: $summary")
            }
        }
    }
}
```

[bed-example]: http://genome.ucsc.edu/goldenpath/help/examples/bedExample.txt
[chrom-sizes]: http://genome.ucsc.edu/goldenpath/help/hg19.chrom.sizes

Useful links
------------

* Kent et al. [paper] [paper] in Bioinformatics
* UCSC documentation on [WIG] [wig], [BED] [bed], [BigWIG] [bigwig] and [BigBED] [bigbed]
* Reference C [implementation](http://hgdownload.cse.ucsc.edu/admin/exe) of both
  big formats by UCSC
* Sketch of [TDF spec.] [tdf] in IGV repository.

[paper]: http://bioinformatics.oxfordjournals.org/content/26/17/2204.abstract
[wig]: http://genome.ucsc.edu/goldenpath/help/wiggle.html
[bed]: https://genome.ucsc.edu/FAQ/FAQformat.html#format1
[bigwig]: http://genome.ucsc.edu/goldenpath/help/bigWig.html
[bigbed]: http://genome.ucsc.edu/goldenpath/help/bigBed.html
[tdf]: https://github.com/igvteam/igv/blob/master/src/org/broad/igv/tdf/notes.txt
