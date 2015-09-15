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
package com.facebook.presto.benchmark;

import com.facebook.presto.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.tpch.TpchMetadata.TINY_LONG_DECIMAL_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SHORT_DECIMAL_SCHEMA_NAME;

public final class NumericBenchmarks
{
    private static final int WARMUP_ITERATIONS = 40;
    private static final int MEASURED_ITERATIONS = 200;

    private NumericBenchmarks() {}

    public static void main(String... args)
    {
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunnerWithSessionSchema(TINY_SCHEMA_NAME)) {
            runNumericBenchmarks(localQueryRunner);
        }

        try (LocalQueryRunner localQueryRunner = createLocalQueryRunnerWithSessionSchema(TINY_SHORT_DECIMAL_SCHEMA_NAME)) {
            runNumericBenchmarks(localQueryRunner);
        }

        try (LocalQueryRunner localQueryRunner = createLocalQueryRunnerWithSessionSchema(TINY_LONG_DECIMAL_SCHEMA_NAME)) {
            runNumericBenchmarks(localQueryRunner);
        }

        try (LocalQueryRunner localQueryRunner = createLocalQueryRunnerWithSessionDecimalNumericType(16, 10)) {
            runNumericBenchmarks(localQueryRunner);
        }

        try (LocalQueryRunner localQueryRunner = createLocalQueryRunnerWithSessionDecimalNumericType(38, 12)) {
            runNumericBenchmarks(localQueryRunner);
        }
    }

    private static void runNumericBenchmarks(LocalQueryRunner localQueryRunner)
    {
        SimpleLineBenchmarkResultWriter writer = new SimpleLineBenchmarkResultWriter(System.out);
        numericBenchmarkAdd(localQueryRunner).runBenchmark(writer);
        numericBenchmarkAdd(localQueryRunner).runBenchmark(writer);
        numericBenchmarkSubtract(localQueryRunner).runBenchmark(writer);
        numericBenchmarkMultiply(localQueryRunner).runBenchmark(writer);
        numericBenchmarkDivide(localQueryRunner).runBenchmark(writer);
        numericBenchmarkSingleColumnCount(localQueryRunner).runBenchmark(writer);
        numericBenchmarkTwoColumnsCount(localQueryRunner).runBenchmark(writer);
    }

    public static SqlBenchmark numericBenchmarkAdd(LocalQueryRunner localQueryRunner)
    {
        return createSqlBenchmark(localQueryRunner, "numericBenchmarkAdd_" + queryNameSuffix(localQueryRunner),
                "SELECT count(extendedprice + tax) from lineitem");
    }

    public static SqlBenchmark numericBenchmarkSubtract(LocalQueryRunner localQueryRunner)
    {
        return createSqlBenchmark(localQueryRunner, "numericBenchmarkSubtract_" + queryNameSuffix(localQueryRunner),
                "SELECT count(extendedprice - tax) from lineitem");
    }

    public static SqlBenchmark numericBenchmarkMultiply(LocalQueryRunner localQueryRunner)
    {
        return createSqlBenchmark(localQueryRunner, "numericBenchmarkMultiply_" + queryNameSuffix(localQueryRunner),
                "SELECT count(extendedprice * tax) from lineitem");
    }

    public static SqlBenchmark numericBenchmarkDivide(LocalQueryRunner localQueryRunner)
    {
        return createSqlBenchmark(localQueryRunner, "numericBenchmarkDivide_" + queryNameSuffix(localQueryRunner),
                "SELECT count(tax / extendedprice) from lineitem");
    }

    public static SqlBenchmark numericBenchmarkSingleColumnCount(LocalQueryRunner localQueryRunner)
    {
        return createSqlBenchmark(localQueryRunner, "numericBenchmarkSingleColumnCount_" + queryNameSuffix(localQueryRunner),
                "SELECT count(extendedprice) from lineitem");
    }

    public static SqlBenchmark numericBenchmarkTwoColumnsCount(LocalQueryRunner localQueryRunner)
    {
        return createSqlBenchmark(localQueryRunner, "numericBenchmarkTwoColumnsCount_" + queryNameSuffix(localQueryRunner),
                "SELECT count(extendedprice), count(tax) from lineitem");
    }

    private static String queryNameSuffix(LocalQueryRunner localQueryRunner)
    {
        return localQueryRunner.getDefaultSession().getSchema().get();
    }

    private static LocalQueryRunner createLocalQueryRunnerWithSessionSchema(String schema)
    {
        return createLocalQueryRunner(false, schema);
    }

    private static LocalQueryRunner createLocalQueryRunnerWithSessionDecimalNumericType(int precision, int scale)
    {
        return createLocalQueryRunner(false, "sf0.01_decimal_" + precision + "_" + scale);
    }

    private static SqlBenchmark createSqlBenchmark(LocalQueryRunner localQueryRunner, String benchmarkName, @Language("SQL") String query)
    {
        return new SqlBenchmark(localQueryRunner, benchmarkName, WARMUP_ITERATIONS, MEASURED_ITERATIONS, query);
    }
}
