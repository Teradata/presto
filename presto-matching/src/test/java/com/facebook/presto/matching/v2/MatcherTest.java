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

import example.ast.FilterNode;
import example.ast.JoinNode;
import example.ast.PlanNode;
import example.ast.ProjectNode;
import example.ast.ScanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.matching.v2.Capture.newCapture;
import static com.facebook.presto.matching.v2.DefaultMatcher.DEFAULT_MATCHER;
import static com.facebook.presto.matching.v2.Pattern.any;
import static com.facebook.presto.matching.v2.Pattern.typeOf;
import static com.facebook.presto.matching.v2.Property.optionalProperty;
import static com.facebook.presto.matching.v2.Property.property;
import static example.ast.Patterns.filter;
import static example.ast.Patterns.plan;
import static example.ast.Patterns.project;
import static example.ast.Patterns.scan;
import static example.ast.Patterns.source;
import static example.ast.Patterns.tableName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
public class MatcherTest
{
    @Test
    void trivial_matchers()
    {
        //any
        assertMatch(any(), 42);
        assertMatch(any(), "John Doe");

        //class based
        assertMatch(typeOf(Integer.class), 42);
        assertMatch(typeOf(Number.class), 42);
        assertNoMatch(typeOf(Integer.class), "John Doe");

        //predicate-based
        assertMatch(typeOf(Integer.class).matching(x -> x > 0), 42);
        assertNoMatch(typeOf(Integer.class).matching(x -> x > 0), -1);
    }

    @Test
    void match_object()
    {
        assertMatch(project(), new ProjectNode(null));
        assertNoMatch(project(), new ScanNode("t"));
    }

    @Test
    void property_matchers()
    {
        Pattern<String> aString = typeOf(String.class);
        Property<String, Integer> length = property(String::length);
        String string = "a";

        assertMatch(project().with(source().matching(scan())), new ProjectNode(new ScanNode("T")));
        assertMatch(aString.with(length.matching(any())), string);
        assertMatch(aString.with(length.matching(x -> x > 0)), string);
        assertMatch(aString.with(length.matching((Number x) -> x.intValue() > 0)), string);

        assertNoMatch(project().with(source().matching(scan())), new ProjectNode(new ProjectNode(new ScanNode("T"))));
        assertNoMatch(aString.with(length.matching(typeOf(Void.class))), string);
        assertNoMatch(aString.with(length.matching(x -> x < 1)), string);
        assertNoMatch(aString.with(length.matching((Number x) -> x.intValue() < 1)), string);
    }

    @Test
    void match_nested_properties()
    {
        Pattern<ProjectNode> pattern = project().with(source().matching(scan()));

        assertMatch(pattern, new ProjectNode(new ScanNode("t")));
        assertNoMatch(pattern, new ScanNode("t"));
        //TODO this needs a custom Option type to work , or NPEs will happen.
        //Optional does not allow null values.
        //assertNoMatch(pattern, new ProjectNode(null));
        assertNoMatch(pattern, new ProjectNode(new ProjectNode(null)));
    }

    @Test
    void match_additional_properties()
    {
        String matchedValue = "A little string.";

        Pattern<String> pattern = typeOf(String.class)
                .matching(s -> s.startsWith("A"))
                .matching((CharSequence s) -> s.length() > 7);

        assertMatch(pattern, matchedValue);
    }

    @Test
    void optional_properties()
    {
        Property<PlanNode, PlanNode> onlySource = optionalProperty(node ->
                Optional.of(node.getSources())
                        .filter(sources -> sources.size() == 1)
                        .map((List<PlanNode> sources) -> sources.get(0)));

        Pattern<PlanNode> planNodeWithExactlyOneSource = plan()
                .with(onlySource.matching(any()));

        assertMatch(planNodeWithExactlyOneSource, new ProjectNode(new ScanNode("t")));
        assertNoMatch(planNodeWithExactlyOneSource, new ScanNode("t"));
        assertNoMatch(planNodeWithExactlyOneSource, new JoinNode(new ScanNode("t"), new ScanNode("t")));
    }

    @Test
    void capturing_matches_in_a_typesafe_manner()
    {
        Capture<FilterNode> filter = newCapture();
        Capture<ScanNode> scan = newCapture();
        Capture<String> name = newCapture();

        Pattern<ProjectNode> pattern = project()
                .with(source().matching(filter().capturedAs(filter)
                        .with(source().matching(scan().capturedAs(scan)
                                .with(tableName().capturedAs(name))))));

        ProjectNode tree = new ProjectNode(new FilterNode(new ScanNode("orders"), null));

        Match<ProjectNode> match = assertMatch(pattern, tree);
        //notice the concrete type despite no casts:
        FilterNode capturedFilter = match.capture(filter);
        assertEquals(tree.getSource(), capturedFilter);
        assertEquals(((FilterNode) tree.getSource()).getSource(), match.capture(scan));
        assertEquals("orders", match.capture(name));
    }

    @Test
    void no_match_means_no_captures()
    {
        Capture<Void> impossible = newCapture();
        Pattern<Void> pattern = typeOf(Void.class).capturedAs(impossible);

        Match<Void> match = DEFAULT_MATCHER.match(pattern, 42);

        assertTrue(match.isEmpty());
        //TODO reimplement using TestNG
        //Throwable throwable = assertThrows(NoSuchElementException.class, () -> match.capture(impossible));
        //assertTrue(() -> throwable.getMessage().contains("Empty match contains no value"));
    }

    @Test
    void unknown_capture_is_an_error()
    {
        Pattern<?> pattern = any();
        Capture<?> unknownCapture = newCapture();

        Match<?> match = DEFAULT_MATCHER.match(pattern, 42);

        //TODO reimplement using TestNG
        //Throwable throwable = assertThrows(NoSuchElementException.class, () -> match.capture(unknownCapture));
        //assertTrue(() -> throwable.getMessage().contains("unknown Capture"));
        //TODO make the error message somewhat help which capture was used, when the captures are human-discernable.
    }

    @Test
    void null_not_matched_by_default()
    {
        assertNoMatch(any(), null);
        assertNoMatch(typeOf(Integer.class), null);
    }

    private <T> Match<T> assertMatch(Pattern<T> pattern, T expectedMatch)
    {
        return assertMatch(pattern, expectedMatch, expectedMatch);
    }

    private <T, R> Match<R> assertMatch(Pattern<R> pattern, T matchedAgainst, R expectedMatch)
    {
        Match<R> match = DEFAULT_MATCHER.match(pattern, matchedAgainst);
        assertEquals(expectedMatch, match.value());
        return match;
    }

    private <T> void assertNoMatch(Pattern<T> pattern, Object expectedNoMatch)
    {
        Match<T> match = DEFAULT_MATCHER.match(pattern, expectedNoMatch);
        assertEquals(Match.empty(), match);
    }
}
