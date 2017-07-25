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

import com.facebook.presto.matching.pattern.CapturePattern;
import com.facebook.presto.matching.pattern.ExtractPattern;
import com.facebook.presto.matching.pattern.FilterPattern;
import com.facebook.presto.matching.pattern.TypeOfPattern;
import com.facebook.presto.matching.pattern.WithPattern;
import com.google.common.collect.Iterables;

import java.util.function.Predicate;

import static com.google.common.base.Predicates.not;

public abstract class Pattern<T>
{
    private final Pattern<?> previous;

    public static Pattern<Object> any()
    {
        return typeOf(Object.class);
    }

    public static <T> Pattern<T> typeOf(Class<T> expectedClass)
    {
        return new TypeOfPattern<>(expectedClass);
    }

    protected Pattern()
    {
        this(null);
    }

    protected Pattern(Pattern<?> previous)
    {
        this.previous = previous;
    }

    public static <F, T extends Iterable<S>, S> PropertyPattern<F, T> empty(Property<F, T> property)
    {
        return PropertyPattern.upcast(property.matching("empty", Iterables::isEmpty));
    }

    public static <F, T extends Iterable<S>, S> PropertyPattern<F, T> nonEmpty(Property<F, T> property)
    {
        return PropertyPattern.upcast(property.matching("nonEmpty", not(Iterables::isEmpty)));
    }

    public Pattern<T> capturedAs(Capture<T> capture)
    {
        return new CapturePattern<>(capture, this);
    }

    public Pattern<T> matching(Predicate<? super T> predicate)
    {
        return matching("", predicate);
    }

    public Pattern<T> matching(String description, Predicate<? super T> predicate)
    {
        return new FilterPattern<>(description, UsageCallSite.get(), predicate, this);
    }

    public <R> Pattern<R> matching(Extractor<? super T, R> extractor)
    {
        return matching("", extractor);
    }

    /**
     * For cases when evaluating a property is needed to check
     * if it's possible to construct an object and that object's
     * construction largely repeats checking the property.
     * <p>
     * E.g. let's say we have a set and we'd like to match
     * other sets having a non-empty intersection with it.
     * If the intersection is not empty, we'd like to use it
     * in further computations. Extractors allow for exactly that.
     * <p>
     * An adequate extractor for the example above would compute
     * the intersection and return it wrapped in a Match
     * (think: null-capable Optional with a field for storing captures).
     * If the intersection would be empty, the extractor should
     * would signal a non-match by returning Match.empty().
     *
     * @param <R> type of the extracted value
     * @param description
     * @param extractor @return
     */
    public <R> Pattern<R> matching(String description, Extractor<? super T, R> extractor)
    {
        return new ExtractPattern<>(description, UsageCallSite.get(), extractor, this);
    }

    public Pattern<T> with(PropertyPattern<? super T, ?> pattern)
    {
        return new WithPattern<>(pattern, this);
    }

    public Pattern<?> previous()
    {
        return previous;
    }

    public abstract Match<T> accept(Matcher matcher, Object object, Captures captures);

    public abstract void accept(PatternVisitor patternVisitor);

    @Override
    public String toString()
    {
        DefaultPrinter printer = new DefaultPrinter();
        accept(printer);
        return printer.result();
    }
}
