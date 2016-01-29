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
package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Parameter;

import java.util.List;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INCORRECT_NUMBER_OF_PARAMETERS;

public class ParameterValidator
        extends DefaultTraversalVisitor<Void, Void>
{
    private int parameterCount = 0;

    private ParameterValidator()
    {
    }

    public int getParameterCount()
    {
        return parameterCount;
    }

    @Override
    public Void visitParameter(Parameter node, Void context)
    {
        parameterCount += 1;
        return null;
    }

    public static void validateParameters(Node node, List<Literal> parameterValues)
    {
        ParameterValidator validator = new ParameterValidator();
        validator.process(node, null);
        if (parameterValues.size() != validator.getParameterCount()) {
            throw new SemanticException(INCORRECT_NUMBER_OF_PARAMETERS, node, "Incorrect number of parameters: expected " + validator.getParameterCount() + " but found " + parameterValues.size());
        }
    }
}
