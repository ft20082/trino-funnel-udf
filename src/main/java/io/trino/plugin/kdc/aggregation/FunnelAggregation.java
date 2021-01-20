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
package io.trino.plugin.kdc.aggregation;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.kdc.aggregation.state.FunnelState;
import io.trino.plugin.kdc.util.FunnelUtil;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 16:55
 */
@AggregationFunction("funnel")
public final class FunnelAggregation
{
    private FunnelAggregation() {}

    private static Logger log = Logger.get(FunnelAggregation.class);

    public static void inputBase(FunnelState state, Slice action, long timestamp, Block... steps)
    {
        if (state.getFunnelSteps().isEmpty()) {
            addFunnelSteps(state, steps);
        }
        String actionString = action.toStringUtf8();
        if (action != null && timestamp > 0 && state.getFunnelSets().contains(actionString)) {
            Map<String, HashSet<Integer>> actionList = state.getActionLists();
            if (!actionList.containsKey(actionString)) {
                actionList.put(actionString, new HashSet<>());
            }
            actionList.get(actionString).add((int) timestamp);
        }
    }

    /**
     * add funnel steps
     * @param state
     * @param steps
     */
    private static void addFunnelSteps(FunnelState state, Block... steps)
    {
        List<ArrayList<String>> stepAll = new ArrayList<>();
        Set<String> stepSet = new HashSet<>();
        for (int i = 0; i < steps.length; i++) {
            ArrayList<String> stepItems = new ArrayList<>();
            Block itemBlock = steps[i];
            int positionCount = itemBlock.getPositionCount();
            for (int x = 0; x < positionCount; x++) {
                Slice stepItem = (Slice) readNativeValue(VARCHAR, itemBlock, x);
                String stepItemString = stepItem.toStringUtf8();
                stepItems.add(stepItemString);
                stepSet.add(stepItemString);
            }
            stepAll.add(stepItems);
        }
        state.getFunnelSteps().addAll(stepAll);
        state.getFunnelSets().addAll(stepSet);
    }

    @CombineFunction
    public static void combine(FunnelState state, FunnelState otherState)
    {
        log.debug("combine state funnel action list size " + state.getActionLists().size());
        log.debug("combine otherState funnel action list size " + otherState.getActionLists().size());
        // merge data
        Map<String, HashSet<Integer>> source = state.getActionLists();
        Map<String, HashSet<Integer>> target = otherState.getActionLists();

        for (String itemStep : target.keySet()) {
            if (source.containsKey(itemStep)) {
                source.get(itemStep).addAll(target.get(itemStep));
            }
            else {
                HashSet<Integer> timeList = new HashSet<>();
                timeList.addAll(target.get(itemStep));
                source.put(itemStep, timeList);
            }
        }

        if (state.getFunnelSteps().isEmpty()) {
            state.getFunnelSteps().addAll(otherState.getFunnelSteps());
        }
        if (state.getFunnelSets().isEmpty()) {
            state.getFunnelSets().addAll(otherState.getFunnelSets());
        }
    }

    @OutputFunction("array(" + StandardTypes.BIGINT + ")")
    public static void output(FunnelState state, BlockBuilder out)
    {
        if (state.getFunnelSteps().isEmpty()) {
            out.appendNull();
        }
        else {
            FunnelUtil funnelUtil = new FunnelUtil(state.getActionLists(), state.getFunnelSteps());
            BlockBuilder blockBuilder = out.beginBlockEntry();
            funnelUtil.computer().forEach(item -> {
                BIGINT.writeLong(blockBuilder, item);
            });
            out.closeEntry();
        }
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2)
    {
        inputBase(state, action, timestamp, s1, s2);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3)
    {
        inputBase(state, action, timestamp, s1, s2, s3);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4)
    {
        inputBase(state, action, timestamp, s1, s2, s3, s4);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5)
    {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6)
    {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7)
    {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8)
    {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7, s8);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s9)
    {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7, s8, s9);
    }

    @InputFunction
    public static void input(FunnelState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice action,
                             @SqlNullable @SqlType(StandardTypes.BIGINT) long timestamp,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s9,
                             @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s10)
    {
        inputBase(state, action, timestamp, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
    }
}
