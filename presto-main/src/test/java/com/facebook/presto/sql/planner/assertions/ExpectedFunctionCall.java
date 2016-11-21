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

package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.WindowFrame;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class ExpectedFunctionCall
{
    private final QualifiedName name;
    private final List<String> argumentAliases;
    private final Optional<WindowFrame> frame;

    public ExpectedFunctionCall(QualifiedName name, List<String> argumentAliases, Optional<WindowFrame> frame)
    {
        this.name = name;
        this.argumentAliases = argumentAliases;
        this.frame = frame;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<String> getArgumentAliases()
    {
        return argumentAliases;
    }

    public Optional<WindowFrame> getFrame()
    {
        return frame;
    }

    @Override
    public String toString()
    {
        return name
                + "("
                + argumentAliases.stream().collect(joining(", "))
                + ")";
    }
}
