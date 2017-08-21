/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositionsArray;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class MaskedBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MaskedBlock.class).instanceSize();

    private final int[] positions;
    private final int offset;
    private final int length;

    private final Block baseBlock;

    private final long retainedSizeInBytes;

    private volatile long sizeInBytes = -1;

    public MaskedBlock(int[] positions, int offset, int length, Block baseBlock)
    {
        checkValidPositionsArray(positions, offset, length);
        this.positions = positions;
        this.offset = offset;
        this.length = length;
        this.baseBlock = requireNonNull(baseBlock, "baseBlock is null");
        this.retainedSizeInBytes = INSTANCE_SIZE + baseBlock.getRetainedSizeInBytes() + sizeOf(positions);
    }

    @Override
    public int getPositionCount()
    {
        return length;
    }

    @Override
    public int getSliceLength(int position)
    {
        return baseBlock.getSliceLength(getBaseBlockPosition(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return baseBlock.getByte(getBaseBlockPosition(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return baseBlock.getShort(getBaseBlockPosition(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return baseBlock.getInt(getBaseBlockPosition(position), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return baseBlock.getLong(getBaseBlockPosition(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return baseBlock.getSlice(getBaseBlockPosition(position), offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return baseBlock.getObject(getBaseBlockPosition(position), clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return baseBlock.bytesEqual(getBaseBlockPosition(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return baseBlock.bytesCompare(getBaseBlockPosition(position),
                offset,
                length,
                otherSlice,
                otherOffset,
                otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        baseBlock.writeBytesTo(getBaseBlockPosition(position), offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        baseBlock.writePositionTo(getBaseBlockPosition(position), blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return baseBlock.equals(getBaseBlockPosition(position),
                offset,
                otherBlock,
                otherPosition,
                otherOffset,
                length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return baseBlock.hash(getBaseBlockPosition(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return baseBlock.compareTo(getBaseBlockPosition(leftPosition),
                leftOffset,
                leftLength,
                rightBlock,
                rightPosition,
                rightOffset,
                rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return baseBlock.getSingleValueBlock(getBaseBlockPosition(position));
    }

    @Override
    public long getSizeInBytes()
    {
        // this is racy but is safe because sizeInBytes is a volatile and the calculation is stable
        if (sizeInBytes < 0) {
            int computedSizeInBytes = length * Integer.BYTES;
            for (int i = offset; i < offset + length; ++i) {
                computedSizeInBytes += baseBlock.getRegionSizeInBytes(positions[i], 1);
            }
            sizeInBytes = computedSizeInBytes;
        }
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkValidRegion(this.length, position, length);

        if (position == 0 && length == getPositionCount()) {
            // Calculation of getRegionSizeInBytes is expensive in this class.
            // On the other hand, getSizeInBytes result is cached.
            return getSizeInBytes();
        }

        int computedSizeInBytes = length * Integer.BYTES;
        for (int i = offset + position; i < offset + position + length; ++i) {
            computedSizeInBytes += baseBlock.getRegionSizeInBytes(positions[i], 1);
        }

        return computedSizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        baseBlock.retainedBytesForEachPart(consumer);
        consumer.accept(positions, sizeOf(positions));
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new MaskedBlockEncoding(baseBlock.getEncoding());
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkValidPositionsArray(positions, offset, length);
        int[] basePositions = new int[length];
        for (int i = 0; i < length; ++i) {
            basePositions[i] = getBaseBlockPosition(positions[offset + i]);
        }
        return baseBlock.copyPositions(basePositions, 0, length);
    }

    @Override
    public Block getRegion(int position, int length)
    {
        checkValidRegion(this.length, position, length);
        return new MaskedBlock(positions, offset + position, length, baseBlock);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        checkValidRegion(this.length, position, length);
        int[] basePositions = new int[length];
        for (int i = 0; i < length; ++i) {
            basePositions[i] = positions[offset + position + i];
        }
        return baseBlock.copyPositions(basePositions, 0, length);
    }

    @Override
    public boolean isNull(int position)
    {
        return baseBlock.isNull(getBaseBlockPosition(position));
    }

    private int getBaseBlockPosition(int position)
    {
        if (position < 0 || position > length) {
            throw new IllegalArgumentException("position is not valid");
        }

        return positions[offset + position];
    }
}
