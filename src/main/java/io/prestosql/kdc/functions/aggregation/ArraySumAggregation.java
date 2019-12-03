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
package io.prestosql.kdc.functions.aggregation;

import io.airlift.log.Logger;
import io.prestosql.kdc.functions.aggregation.state.LongListState;
import io.prestosql.kdc.functions.util.ArrayUtil;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 17:35
 */
@AggregationFunction("array_sum")
public class ArraySumAggregation
{
    private ArraySumAggregation() {}

    private static Logger log = Logger.get(ArraySumAggregation.class);

    @InputFunction
    public static void input(LongListState state,
                             @SqlNullable @SqlType("array(" + StandardTypes.BIGINT + ")") Block block)
    {
        if (block != null) {
            List<Long> list = ArrayUtil.blockListLongOf(block);
            int listSize = list.size();
            if (state.getList().isEmpty()) {
                state.getList().addAll(list);
            }
            else {
                int statListSize = state.getList().size();
                if (listSize == statListSize) {
                    for (int i = 0; i < listSize; i++) {
                        state.getList().set(i, state.getList().get(i) + list.get(i));
                    }
                }
                else {
                    throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "array input num must be same.");
                }
            }
        }
    }

    @CombineFunction
    public static void combine(LongListState state, LongListState otherState)
    {
        int listSize = state.getList().size();
        int otherListSize = otherState.getList().size();

        if (listSize > 0 || otherListSize > 0) {
            if (listSize == 0 && otherListSize > 0) {
                state.getList().addAll(otherState.getList());
            }
            else if (listSize > 0 && otherListSize > 0 && otherListSize == listSize) {
                for (int i = 0; i < listSize; i++) {
                    Long l1 = state.getList().get(i);
                    Long l2 = otherState.getList().get(i);
                    state.getList().set(i, l1 + l2);
                }
            }
        }
    }

    /**
     * export Bigint
     */
    @OutputFunction("array(" + StandardTypes.BIGINT + ")")
    public static void output(LongListState state, BlockBuilder out)
    {
        if (state.getList().isEmpty()) {
            out.appendNull();
        }
        else {
            log.debug("state now " + ArrayUtil.ListLongStringOf(state.getList()));
            BlockBuilder blockBuilder = out.beginBlockEntry();
            state.getList().forEach(item -> {
                BIGINT.writeLong(blockBuilder, item);
            });
            out.closeEntry();
        }
    }
}
