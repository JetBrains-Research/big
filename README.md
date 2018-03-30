# big [![Build Status](https://travis-ci.org/JetBrains-Research/big.svg?branch=master)](https://travis-ci.org/JetBrains-Research/big) [![Build status](https://ci.appveyor.com/api/projects/status/e9q4o6rgdfhyy6ry?svg=true)](https://ci.appveyor.com/project/superbobry/big) [![Build status](https://teamcity.jetbrains.com/app/rest/builds/buildType:(id:Epigenome_Tools_Big)/statusIcon.svg)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Epigenome_Tools_Big&guest=1)

`big` implements high performance classes for reading and writing BigWIG,
BigBED and TDF. You can use `big` in any programming language running on the
JVM, but the public API is in part Kotlin-specific.

Installation
------------

The latest version of `big` is available on [jCenter] [jcenter]. If you're using
Gradle just add the following to your `build.gradle`:

```gradle
repositories {
    jcenter()
}

dependencies {
    compile 'org.jetbrains.bio:big:0.3.4'
}

```

[jcenter]: https://bintray.com/bintray/jcenter

Examples
--------

The following examples assume that all required symbols are imported into the
current scope. They also rely on the helper function for reading TSV formated
[chromosome sizes] [chrom-sizes] from UCSC annotations.

```kotlin
/** Fetches chromosome sizes from a UCSC provided TSV file. */
internal fun Path.chromosomes(): List<Pair<String, Int>> {
    return Files.newBufferedReader(this).lineSequence().map { line ->
        val chunks = line.split('\t', limit = 3)
        chunks[0] to chunks[1].toInt()
    }.toList()
}
```

### wigToBigWig

```kotlin
fun wigToBigWig(inputPath: Path, outputPath: Path, chromSizesPath: Path) {
    BigWigFile.write(WigFile(inputPath), chromSizesPath.chromosomes(), outputPath)
}
```

### bigWigSummary

```kotlin
fun bigWigSummary(inputPath: Path, numBins: Int) {
    BigWigFile.read(inputPath).use { bwf ->
        println("Total: ${bwf.totalSummary}")

        for (chromosome in bwf.chromosomes.valueCollection()) {
            for ((i, summary) in bwf.summarize(chromosome, numBins = numBins).withIndex()) {
                println("bin #${i + 1}: $summary")
            }
        }
    }
}
```

### bedToBigBed

```kotlin
fun bedToBigBed(inputPath: Path, outputPath: Path, chromSizesPath: Path) {
    BigBedFile.write(BedFile(inputPath), chromSizesPath.chromosomes(), outputPath)
}
```

### bigBedToBed

```kotlin
fun bigBedToBed(inputPath: Path) {
    BigBedFile.read(inputPath).use { bbf ->
        for (chromosome in bbf.chromosomes.valueCollection()) {
            for ((chrom, start, end) in bbf.query(chromosome)) {
                // See 'BedEntry' for a complete list of available
                // attributes.
                println("$chrom\t$start\t$end")
            }
        }
    }
}
```

[chrom-sizes]: http://genome.ucsc.edu/goldenpath/help/hg19.chrom.sizes

Building from source
--------------------

The build process is as simple as

```bash
$ ./gradlew assemble
```

Testing
-------

No extra configuration is required for running the tests from Gradle

```bash
$ ./gradlew test
```

Publishing
----------

You can publish a new release with a one-liner

```bash
./gradlew clean assemble test generatePomFileForMavenJavaPublication bintrayUpload
```

Make sure to set Bintray credentials (see API key section
[here](https://bintray.com/profile/edit)) in `$HOME/.gradle/gradle.properties`.

```
$ cat $HOME/.gradle/gradle.properties
bintrayUser=CHANGEME
bintrayKey=CHANGEME
```

Useful links
------------

* Kent et al. [paper] [paper] in Bioinformatics
* UCSC documentation on [WIG] [wig], [BED] [bed], [BigWIG] [bigwig] and [BigBED] [bigbed]
* Reference C [implementation](http://hgdownload.cse.ucsc.edu/admin/exe) of both
  big formats by UCSC
* Sketch of [TDF spec.] [tdf] in IGV repository and another version [on Gist] [tdf-gist]

[paper]: http://bioinformatics.oxfordjournals.org/content/26/17/2204.abstract
[wig]: http://genome.ucsc.edu/goldenpath/help/wiggle.html
[bed]: https://genome.ucsc.edu/FAQ/FAQformat.html#format1
[bigwig]: http://genome.ucsc.edu/goldenpath/help/bigWig.html
[bigbed]: http://genome.ucsc.edu/goldenpath/help/bigBed.html
[tdf]: https://github.com/igvteam/igv/blob/master/src/org/broad/igv/tdf/notes.txt
[tdf-gist]: https://gist.github.com/superbobry/c67614cbfe2a15d35d5c
