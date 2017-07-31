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

import com.facebook.presto.matching.pattern.*;

import java.util.Optional;

public class DefaultMatcher
        implements Matcher
{
    public static final Matcher DEFAULT_MATCHER = new DefaultMatcher();

    @Override
    public <T> Match<T> match(Pattern<T> pattern, Object object, Captures captures)
    {
        if (pattern.previous() != null) {
            Match<?> match = match(pattern.previous(), object, captures);
            return match.flatMap((value) -> pattern.accept(this, value, match.captures()));
        }
        else {
            return pattern.accept(this, object, captures);
        }
    }

    @Override
    public <T> Match<T> matchTypeOf(TypeOfPattern<T> typeOfPattern, Object object, Captures captures)
    {
        Class<T> expectedClass = typeOfPattern.expectedClass();
        if (expectedClass.isInstance(object)) {
            return Match.of(expectedClass.cast(object), captures);
        }
        else {
            return Match.empty();
        }
    }

    @Override
    public <T, R> Match<R> matchHasProperty(HasPropertyPattern<T, R> hasPropertyPattern, Object object, Captures captures) {
        Property<? super T, R> property = hasPropertyPattern.getProperty();
        Optional<R> propertyValue = property.apply((T) object, captures);
        Match<R> propertyMatch = propertyValue
                .map(value -> Match.of(value, captures))
                .orElse(Match.empty());
        return propertyMatch;
    }

    @Override
    public <T> Match<T> matchScoped(ScopedPattern<T> scopedPattern, Object object, Captures captures) {
        Pattern<?> pattern = scopedPattern.getPattern();
        Match<?> match = match(pattern, object, captures);
        return match.map(value -> (T) object);
    }

    @Override
    public <T> Match<T> matchCombine(CombinePattern<T> combinePattern, Object object, Captures captures) {
        return match(combinePattern.getPattern(), object, captures);
    }

    @Override
    public <T> Match<T> matchCapture(CapturePattern<T> capturePattern, Object object, Captures captures)
    {
        return Match.of((T) object, captures.addAll(Captures.ofNullable(capturePattern.capture(), (T) object)));
    }

    @Override
    public <T> Match<T> matchEquals(EqualsPattern<T> equalsPattern, Object object, Captures captures)
    {
        return Match.of((T) object, captures).filter(equalsPattern.expectedValue()::equals);
    }

    @Override
    public <T> Match<T> matchFilter(FilterPattern<T> filterPattern, Object object, Captures captures)
    {
        return Match.of((T) object, captures).filter(filterPattern.predicate());
    }
}
