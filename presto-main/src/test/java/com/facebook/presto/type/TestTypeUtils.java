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
package com.facebook.presto.type;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.OptionalLong;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.extractCalculationInputs;
import static org.testng.Assert.assertEquals;

public class TestTypeUtils
{
    @Test
    public void testExtractCalculationInputs()
    {
        assertEquals(extractCalculationInputs(parseTypeSignature("varchar(x + y as result)"), parseTypeSignature("varchar(10)")),
                ImmutableMap.of("RESULT", OptionalLong.of(10)));
    }
}
