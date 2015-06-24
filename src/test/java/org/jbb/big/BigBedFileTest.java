package org.jbb.big;

import junit.framework.TestCase;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class BigBedFileTest extends TestCase {
  private static final Random RANDOM = new Random();

  @Override
  protected void setUp() throws Exception {
    RANDOM.setSeed(42);
  }

  public void testQuerySmall() throws IOException {
    final List<RawBedData> items = getExampleItems();
    try (final BigBedFile bbf = getExampleFile()) {
      for (int i = 0; i < 100; i++) {
        testQuery(bbf, items.get(RANDOM.nextInt(items.size())));
      }
    }
  }

  public void testQueryLarge() throws IOException {
    final List<RawBedData> items = getExampleItems();
    try (final BigBedFile bbf = getExampleFile()) {
      for (int i = 0; i < 10; i++) {
        final RawBedData a = items.get(RANDOM.nextInt(items.size()));
        final RawBedData b = items.get(RANDOM.nextInt(items.size()));
        testQuery(bbf, new RawBedData(a.name, Math.min(a.start, b.start),
                                      Math.max(a.end, b.end)));
      }
    }
  }

  private void testQuery(final BigBedFile bbf, final RawBedData query)
      throws IOException {
    final List<BedData> actual = bbf.query(query.name, query.start, query.end);
    for (final BedData item : actual) {
      assertTrue(item.start >= query.start && item.end <= query.end);
    }

    final List<BedData> expected = getExampleItems().stream()
        .filter(item -> item.start >= query.start && item.end <= query.end)
        .map(item -> new BedData(0, item.start, item.end, ""))
        .collect(Collectors.toList());

    //assertEquals(expected.size(), actual.size());
    assertEquals(query.toString(), expected, actual);
  }

  @NotNull
  private BigBedFile getExampleFile() throws IOException {
    return BigBedFile.parse(Examples.get("example1.bb"));
  }

  private List<RawBedData> getExampleItems() throws IOException {
    return Files.lines(Examples.get("example1.bed")).map(line -> {
      final String[] chunks = line.split("\t", 3);
      return new RawBedData(chunks[0],
                            Integer.parseInt(chunks[1]),
                            Integer.parseInt(chunks[2]));
    }).collect(Collectors.toList());
  }

  private static class RawBedData {
    private final String name;
    private final int start;
    private final int end;

    private RawBedData(final String name, final int start, final int end) {
      this.name = name;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return String.format("%s@[%d, %d)", name, start, end);
    }
  }
}