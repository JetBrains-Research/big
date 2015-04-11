package org.jbb.big;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Just like BED only BIGGER.
 *
 * @author Sergei Lebedev
 * @date 11/04/15
 */
public class BigBedFile extends BigFile {
  public static BigBedFile parse(final Path path) throws IOException {
    return new BigBedFile(path);
  }

  protected BigBedFile(final Path path) throws IOException {
    super(path);
  }

  /**
   * Defaults {@code maxItems} to {@code 0}.
   */
  public List<BedData> query(final String chromName, final int startOffset, final int endOffset)
      throws IOException {
    return query(chromName, startOffset, endOffset, 0);
  }

  /**
   * Queries an R+-tree.
   *
   * @param chromName human-readable chromosome name, e.g. {@code "chr9"}.
   * @param startOffset 0-based start offset (inclusive).
   * @param endOffset 0-based end offset (exclusive), if 0 than all chromosome is used.
   * @param maxItems upper bound on the number of items to return.
   * @return a list of intervals overlapping the query.
   * @throws IOException if the underlying {@link SeekableDataInput} does so.
   */
  public List<BedData> query(final String chromName, final int startOffset, final int endOffset,
                             final int maxItems)
      throws IOException {
    final Optional<BPlusLeaf> bpl = header.bPlusTree.find(handle, chromName);
    if (bpl.isPresent()) {
      final int chromIx = bpl.get().id;
      final RTreeInterval target = RTreeInterval.of(
          chromIx, startOffset, endOffset == 0 ? bpl.get().size : endOffset);
      return header.rTree.findOverlaps(handle, target, maxItems);
    } else {
      return ImmutableList.of();
    }
  }
}
