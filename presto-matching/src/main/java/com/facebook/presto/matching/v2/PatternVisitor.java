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
package com.facebook.presto.matching.v2;

import com.facebook.presto.matching.v2.pattern.CapturePattern;
import com.facebook.presto.matching.v2.pattern.FilterPattern;
import com.facebook.presto.matching.v2.pattern.TypeOfPattern;
import com.facebook.presto.matching.v2.pattern.WithPattern;

public interface PatternVisitor
{
    void visitCapture(CapturePattern<?> pattern);

    void visitFilter(FilterPattern<?> pattern);

    void visitTypeOf(TypeOfPattern<?> pattern);

    void visitWith(WithPattern<?> pattern);

    default void visitPrevious(Pattern pattern)
    {
        Pattern previous = pattern.previous();
        if (previous != null) {
            previous.accept(this);
        }
    }
}
