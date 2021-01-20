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

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 16:57
 */
@AccumulatorStateMetadata(stateSerializerClass = FunnelStateSerializer.class, stateFactoryClass = FunnelStateFactory.class)
public interface FunnelState
        extends AccumulatorState
{
    /**
     * memory
     * @param memory
     */
    void addMemoryUsage(int memory);

    /**
     * get funnel action list, action is key, timestamp is array list.
     * @return
     */
    Map<String, HashSet<Integer>> getActionLists();

    /**
     * Funnel steps
     * @return
     */
    List<ArrayList<String>> getFunnelSteps();

    /**
     * funnel set
     * @return
     */
    Set<String> getFunnelSets();
}
