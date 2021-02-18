[![JetBrains Research](https://jb.gg/badges/research.svg)](https://confluence.jetbrains.com/display/ALL/JetBrains+on+GitHub)
Linux/MacOS [![Build status](https://teamcity.jetbrains.com/app/rest/builds/buildType:(id:Epigenome_Tools_Big)/statusIcon.svg)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Epigenome_Tools_Big&guest=1)  Windows [![Build status](https://teamcity.jetbrains.com/app/rest/builds/buildType:(id:Epigenome_Tools_BigWindows)/statusIcon.svg)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=Epigenome_Tools_BigWindows&guest=1)

# big

`big` implements high performance classes for reading and writing BigWIG,
BigBED and TDF. You can use `big` in any programming language running on the
JVM, but the public API is in part Kotlin-specific.

Installation
------------

The latest version of `big` is available on [Maven Central] [maven-central]. If you're using
Gradle just add the following to your `build.gradle`:

```groovy
repositories {
    mavenCentral()
}

dependencies {
    compile 'org.jetbrains.bio:big:0.9.1'
}

```

With Maven, specify the following in your `pom.xml`:
```xml
<dependency>
  <groupId>org.jetbrains.bio</groupId>
  <artifactId>big</artifactId>
  <version>0.9.1</version>
</dependency>
```

The previous versions were published on Bintray. They can be downloaded
from [GitHub Releases](https://github.com/JetBrains-Research/big/releases).

[maven-central]: https://search.maven.org/artifact/org.jetbrains.bio/big/0.9.1/jar

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
$ ./gradlew jar
```

Note: don't use `./gradlew assemble`, since it includes the signing of the artifacts
and will fail if the correct credentials are not provided.

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
