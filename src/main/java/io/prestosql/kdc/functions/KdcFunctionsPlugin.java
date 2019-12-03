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
package io.prestosql.kdc.functions;

import com.google.common.collect.ImmutableSet;
import io.prestosql.kdc.functions.aggregation.ArraySumAggregation;
import io.prestosql.kdc.functions.aggregation.FunnelAggregation;
import io.prestosql.kdc.functions.scalar.HiveNvl;
import io.prestosql.spi.Plugin;

import java.util.Set;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 14:18
 */
public class KdcFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                       .add(HiveNvl.class)
                       .add(FunnelAggregation.class)
                       .add(ArraySumAggregation.class)
                       .build();
    }
}
