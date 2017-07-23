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

package com.facebook.presto.matching;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public abstract class Pattern
{
    private static final Pattern ANY = new TypeOf(Object.class);

    private Pattern() {}

    public abstract boolean matches(Object object);

    public static Pattern any()
    {
        return ANY;
    }

    public static <T> Pattern typeOf(Class<T> objectClass)
    {
        return new TypeOf(objectClass);
    }

    //TODO remove this Pattern class along with this method once the migration is complete
    public static Pattern v2Adapter(com.facebook.presto.matching.v2.Pattern<?> pattern)
    {
        return new V2Adapter(pattern);
    }

    static class TypeOf<T>
            extends Pattern
    {
        private final Class<T> type;

        TypeOf(Class<T> type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        Class<T> getType()
        {
            return type;
        }

        @Override
        public boolean matches(Object object)
        {
            return type.isInstance(object);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .toString();
        }
    }

    static class V2Adapter
            extends Pattern
    {
        V2Adapter(com.facebook.presto.matching.v2.Pattern<?> pattern)
        {
            this.pattern = pattern;
        }

        private com.facebook.presto.matching.v2.Pattern<?> pattern;

        @Override
        public boolean matches(Object object)
        {
            throw new UnsupportedOperationException("a matcher should be used for matching the v2 pattern");
        }

        public com.facebook.presto.matching.v2.Pattern<?> getPattern()
        {
            return pattern;
        }
    }
}
