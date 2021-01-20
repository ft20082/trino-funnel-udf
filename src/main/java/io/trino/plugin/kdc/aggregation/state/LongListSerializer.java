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
package io.trino.plugin.kdc.aggregation.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

import static io.trino.spi.type.VarbinaryType.VARBINARY;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 17:24
 */
public class LongListSerializer
        implements AccumulatorStateSerializer<LongListState>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(LongListState state, BlockBuilder out)
    {
        if (state.getList() == null) {
            out.appendNull();
        }
        else {
            try {
                VARBINARY.writeSlice(out, Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(state.getList())));
            }
            catch (JsonProcessingException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public void deserialize(Block block, int index, LongListState state)
    {
        if (!block.isNull(index)) {
            SliceInput slice = VARBINARY.getSlice(block, index).getInput();
            List<Long> listState;
            try {
                listState = OBJECT_MAPPER.readValue(slice.readSlice(slice.available()).getBytes(), new TypeReference<List<Long>>() {});
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            state.getList().clear();
            state.getList().addAll((List<Long>) listState);
        }
    }
}
