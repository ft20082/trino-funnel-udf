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

import java.util.List;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 17:23
 */
@AccumulatorStateMetadata(stateSerializerClass = LongListSerializer.class, stateFactoryClass = LongListFactory.class)
public interface LongListState
        extends AccumulatorState
{
    /**
     * memory
     * @param memory
     */
    void addMemoryUsage(int memory);

    /**
     * 获取 list
     * @return
     */
    List<Long> getList();
}
