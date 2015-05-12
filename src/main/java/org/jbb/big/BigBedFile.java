package org.jbb.big;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Just like BED only BIGGER.
 *
 * @author Sergei Lebedev
 * @since 11/04/15
 */
public class BigBedFile extends BigFile<BedData> {

  public static final int MAGIC = 0x8789f2eb;

  public static BigBedFile parse(final Path path) throws IOException {
    return new BigBedFile(path);
  }

  protected BigBedFile(final Path path) throws IOException {
    super(path);
  }

  @Override
  public int getHeaderMagic() {
    return MAGIC;
  }

  @Override
  protected List<BedData> queryInternal(final RTreeInterval query, final int maxItems)
      throws IOException {
    final int chromIx = query.left.chromIx;
    final List<BedData> res = Lists.newArrayList();
    header.rTree.findOverlappingBlocks(handle, query, block -> {
      handle.seek(block.dataOffset);

      do {
        assert handle.readInt() == chromIx : "interval contains wrong chromosome";
        if (maxItems > 0 && res.size() == maxItems) {
          // XXX think of a way of terminating the traversal?
          return;
        }

        final int startOffset = handle.readInt();
        final int endOffset = handle.readInt();
        byte ch;
        final StringBuilder sb = new StringBuilder();
        for (; ; ) {
          ch = handle.readByte();
          if (ch == 0) {
            break;
          }

          sb.append(ch);
        }

        if (startOffset < query.left.offset) {
          continue;
        } else if (endOffset > query.right.offset) {
          break;
        }

        res.add(new BedData(chromIx, startOffset, endOffset, sb.toString()));
      } while (handle.tell() - block.dataOffset < block.dataSize);
    });

    return res;
  }
}
