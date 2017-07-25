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
package com.facebook.presto.matching.pattern;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Extractor;
import com.facebook.presto.matching.Match;
import com.facebook.presto.matching.Matcher;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.PatternVisitor;
import com.facebook.presto.matching.UsageCallSite;

public class ExtractPattern<T, R>
        extends Pattern<R>
{
    private final String description;
    private final UsageCallSite usageCallSite;
    private final Extractor<? super T, R> extractor;

    public ExtractPattern(String description, UsageCallSite usageCallSite, Extractor<? super T, R> extractor, Pattern<T> pattern)
    {
        super(pattern);
        this.description = description;
        this.usageCallSite = usageCallSite;
        this.extractor = extractor;
    }

    public String description()
    {
        return description;
    }

    public UsageCallSite usageCallSite()
    {
        return usageCallSite;
    }

    public Extractor<? super T, R> extractor()
    {
        return extractor;
    }

    @Override
    public Match<R> accept(Matcher matcher, Object object, Captures captures)
    {
        return matcher.matchExtract(this, object, captures);
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitExtract(this);
    }
}
