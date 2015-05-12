package org.jbb.big;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * @author Konstantin Kolosovsky.
 */
public class BigWigFile extends BigFile<WigData> {

  public static final int MAGIC = 0x888FFC26;

  @NotNull
  public static BigWigFile parse(@NotNull final Path path) throws IOException {
    return new BigWigFile(path);
  }

  protected BigWigFile(@NotNull final Path path) throws IOException {
    super(path);
  }

  @Override
  public int getHeaderMagic() {
    return MAGIC;
  }

  @Override
  protected List<WigData> queryInternal(final RTreeInterval query, final int maxItems)
      throws IOException {
    final List<WigData> result = Lists.newArrayList();

    header.rTree.findOverlappingBlocks(handle, query, block -> {
      handle.seek(block.dataOffset);

      // TODO: Do we need to merge WigData instances with subsequent headers?
      // TODO: Investigate bigWigToWig output and source code.
      WigSectionHeader header = WigSectionHeader.read(handle);

      switch (header.type) {
        case WigSectionHeader.FIXED_STEP_TYPE:
          result.add(FixedStepWigData.read(header, handle));
          break;
        case WigSectionHeader.VARIABLE_STEP_TYPE:
          result.add(VariableStepWigData.read(header, handle));
          break;
        case WigSectionHeader.BED_GRAPH_TYPE:
          throw new IllegalStateException("bedGraph sections are not supported in bigWig files");
        default:
          throw new IllegalStateException("unknown section type " + header.type);
      }

      Preconditions.checkState(handle.tell() - block.dataOffset == block.dataSize,
                               "wig section read incorrectly - %s, %s, %s", handle.tell(),
                               block.dataOffset, block.dataSize);
    });

    return result;
  }
}
