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

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static java.util.Objects.requireNonNull;

public class MaskedBlockEncoding
        implements BlockEncoding
{
    private static final String NAME = "MASKED_BLOCK";
    private final BlockEncoding baseBlockEncoding;

    public MaskedBlockEncoding(BlockEncoding baseBlockEncoding)
    {
        this.baseBlockEncoding = requireNonNull(baseBlockEncoding, "baseBlockEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Block readBlock(SliceInput input)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        baseBlockEncoding.writeBlock(sliceOutput, block.copyRegion(0, block.getPositionCount()));
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return new MaskedBlockEncodingFactory(baseBlockEncoding.getFactory());
    }

    public static class MaskedBlockEncodingFactory
            implements BlockEncodingFactory<MaskedBlockEncoding>
    {
        private final BlockEncodingFactory baseEncodingFactory;

        public MaskedBlockEncodingFactory(BlockEncodingFactory baseEncodingFactory)
        {
            this.baseEncodingFactory = baseEncodingFactory;
        }

        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public MaskedBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, MaskedBlockEncoding blockEncoding)
        {
            baseEncodingFactory.writeEncoding(serde, output, blockEncoding);
        }
    }
}
