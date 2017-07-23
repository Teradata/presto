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
package example.ast;

import java.util.List;

import static java.util.Arrays.asList;

public class JoinNode
        implements PlanNode
{
    private PlanNode probe;
    private PlanNode build;

    public JoinNode(PlanNode probe, PlanNode build)
    {
        this.probe = probe;
        this.build = build;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return asList(probe, build);
    }

    public PlanNode getProbe()
    {
        return probe;
    }

    public PlanNode getBuild()
    {
        return build;
    }
}
