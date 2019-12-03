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
package io.prestosql.kdc.functions.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 14:23
 */
@Description("Return default value if the value is NULL else return value")
@ScalarFunction("nvl")
public final class HiveNvl
{
    private HiveNvl() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Slice nvlSlice(@SqlNullable @SqlType("T") Slice value, @SqlNullable @SqlType("T") Slice defaultValue)
    {
        return (value == null) ? defaultValue : value;
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Long nvlSlice(@SqlNullable @SqlType("T") Long value, @SqlNullable @SqlType("T") Long defaultValue)
    {
        return (value == null) ? defaultValue : value;
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Double nvlSlice(@SqlNullable @SqlType("T") Double value, @SqlNullable @SqlType("T") Double defaultValue)
    {
        return (value == null) ? defaultValue : value;
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Boolean nvlSlice(@SqlNullable @SqlType("T") Boolean value, @SqlNullable @SqlType("T") Boolean defaultValue)
    {
        return (value == null) ? defaultValue : value;
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Object nvlSlice(@SqlNullable @SqlType("T") Object value, @SqlNullable @SqlType("T") Object defaultValue)
    {
        return (value == null) ? defaultValue : value;
    }
}
