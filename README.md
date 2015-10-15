# big [![Build Status](https://travis-ci.org/JetBrains-Research/big.svg?branch=master)](https://travis-ci.org/JetBrains-Research/big) [![Build status](https://ci.appveyor.com/api/projects/status/e9q4o6rgdfhyy6ry?svg=true)](https://ci.appveyor.com/project/superbobry/big)

`big` implements classes for reading and writing BigWIG and BigBED for the JVM.

Installation
------------

The latest version of `big` is available on [jCenter] [jcenter]. If you're using
Gradle just add the following to your `build.gradle`:

```gradle
repositories {
    jcenter()
}

dependencies {
    compile 'org.jetbrains.bio:big:0.1.9'
}

```

[jcenter]: https://bintray.com/bintray/jcenter

Useful links
------------

* Kent et al. [paper] [paper] in Bioinformatics
* UCSC documentation on [WIG] [wig], [BED] [bed], [BigWIG] [bigwig] and [BigBED] [bigbed]
* Reference C [implementation](http://hgdownload.cse.ucsc.edu/admin/exe) of both big formats by UCSC

[paper]: http://bioinformatics.oxfordjournals.org/content/26/17/2204.abstract
[wig]: http://genome.ucsc.edu/goldenpath/help/wiggle.html
[bed]: http://genome.ucsc.edu/goldenpath/help/bed.html
[bigwig]: http://genome.ucsc.edu/goldenpath/help/bigWig.html
[bigbed]: http://genome.ucsc.edu/goldenpath/help/bigBed.html
