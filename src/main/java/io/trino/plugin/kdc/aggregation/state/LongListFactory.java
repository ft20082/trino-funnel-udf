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

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 17:22
 */
public class LongListFactory
        implements AccumulatorStateFactory
{
    private static final long ARRAY_LIST_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();

    @Override
    public Object createSingleState()
    {
        return new SingleLongListFactory();
    }

    @Override
    public Class getSingleStateClass()
    {
        return SingleLongListFactory.class;
    }

    @Override
    public Object createGroupedState()
    {
        return new GroupedLongListFactory();
    }

    @Override
    public Class getGroupedStateClass()
    {
        return GroupedLongListFactory.class;
    }

    public static class GroupedLongListFactory
            implements GroupedAccumulatorState, LongListState
    {
        private final ObjectBigArray<List<Long>> lists = new ObjectBigArray<>();

        private long memoryUsage;
        private long groupId;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            lists.ensureCapacity(size);
        }

        @Override
        public void addMemoryUsage(int memory)
        {
            memoryUsage += memory;
        }

        @Override
        public List<Long> getList()
        {
            if (lists.get(groupId) == null) {
                lists.set(groupId, new ArrayList<Long>());
                memoryUsage += ARRAY_LIST_SIZE;
            }
            return lists.get(groupId);
        }

        @Override
        public long getEstimatedSize()
        {
            return memoryUsage;
        }
    }

    public static class SingleLongListFactory
            implements LongListState
    {
        private final List<Long> list = new ArrayList<>();

        private int memoryUsage;

        @Override
        public void addMemoryUsage(int memory)
        {
            memoryUsage += memory;
        }

        @Override
        public List<Long> getList()
        {
            return list;
        }

        @Override
        public long getEstimatedSize()
        {
            return memoryUsage + ARRAY_LIST_SIZE;
        }
    }
}
