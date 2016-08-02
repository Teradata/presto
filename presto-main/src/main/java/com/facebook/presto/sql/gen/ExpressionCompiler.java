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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilationException;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.operator.JoinFilterFunction;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.google.common.base.MoreObjects.toStringHelper;

public class ExpressionCompiler
{
    private final Metadata metadata;

    private final LoadingCache<ProcessorCacheKey, Class<? extends PageProcessor>> pageProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<ProcessorCacheKey, Class<? extends PageProcessor>>()
            {
                @Override
                public Class<? extends PageProcessor> load(ProcessorCacheKey key)
                        throws Exception
                {
                    return compile(key.getFilter(), key.getProjections(), new PageProcessorCompiler(metadata), PageProcessor.class);
                }
            });

    private final LoadingCache<ProcessorCacheKey, Class<? extends CursorProcessor>> cursorProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<ProcessorCacheKey, Class<? extends CursorProcessor>>()
            {
                @Override
                public Class<? extends CursorProcessor> load(ProcessorCacheKey key)
                        throws Exception
                {
                    return compile(key.getFilter(), key.getProjections(), new CursorProcessorCompiler(metadata), CursorProcessor.class);
                }
            });

    private final LoadingCache<JoinFilterCacheKey, Class<? extends JoinFilterFunction>> joinFilterFunctions = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<JoinFilterCacheKey, Class<? extends JoinFilterFunction>>()
            {
                @Override
                public Class<? extends JoinFilterFunction> load(JoinFilterCacheKey key)
                        throws Exception
                {
                    ClassDefinition classDefinition = new ClassDefinition(
                            a(PUBLIC, FINAL),
                            makeClassName("JoinFilterFunction"),
                            type(Object.class),
                            type(JoinFilterFunction.class));

                    CallSiteBinder callSiteBinder = new CallSiteBinder();

                    new JoinFilterFunctionCompiler(metadata).generateMethods(classDefinition, callSiteBinder, key.getFilter(), key.getLeftBlocksSize());

                    //
                    // toString method
                    //
                    generateToString(
                            classDefinition,
                            callSiteBinder,
                            toStringHelper(classDefinition.getType().getJavaClassName())
                                    .add("filter", key.getFilter())
                                    .add("leftBlocksSize", key.getLeftBlocksSize())
                                    .toString());

                    return defineClass(classDefinition, JoinFilterFunction.class, callSiteBinder.getBindings(), getClass().getClassLoader());
                }
            });

    @Inject
    public ExpressionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Managed
    public long getCacheSize()
    {
        return pageProcessors.size();
    }

    public Supplier<CursorProcessor> compileCursorProcessor(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
    {
        Class<? extends CursorProcessor> cursorProcessor = cursorProcessors.getUnchecked(new ProcessorCacheKey(filter, projections, uniqueKey));
        return () -> {
            try {
                return cursorProcessor.newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    public Supplier<PageProcessor> compilePageProcessor(RowExpression filter, List<RowExpression> projections)
    {
        Class<? extends PageProcessor> pageProcessor = pageProcessors.getUnchecked(new ProcessorCacheKey(filter, projections, null));
        return () -> {
            try {
                return pageProcessor.newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    public Supplier<JoinFilterFunction> compileJoinFilterFunction(RowExpression filter, int leftBlocksSize)
    {
        Class<? extends JoinFilterFunction> joinFilterFunction = joinFilterFunctions.getUnchecked(new JoinFilterCacheKey(filter, leftBlocksSize));
        return () -> {
            try {
                return joinFilterFunction.newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private <T> Class<? extends T> compile(RowExpression filter, List<RowExpression> projections, BodyCompiler<T> bodyCompiler, Class<? extends T> superType)
    {
        // create filter and project page iterator class
        try {
            return compileProcessor(filter, projections, bodyCompiler, superType);
        }
        catch (CompilationException e) {
            throw new PrestoException(COMPILER_ERROR, e.getCause());
        }
    }

    private <T> Class<? extends T> compileProcessor(
            RowExpression filter,
            List<RowExpression> projections,
            BodyCompiler<T> bodyCompiler,
            Class<? extends T> superType)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(superType.getSimpleName()),
                type(Object.class),
                type(superType));

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        bodyCompiler.generateMethods(classDefinition, callSiteBinder, filter, projections);

        //
        // toString method
        //
        generateToString(
                classDefinition,
                callSiteBinder,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filter)
                        .add("projections", projections)
                        .toString());

        return defineClass(classDefinition, superType, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateToString(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String string)
    {
        // bind constant via invokedynamic to avoid constant pool issues due to large strings
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    private static final class ProcessorCacheKey
    {
        private final RowExpression filter;
        private final List<RowExpression> projections;
        private final Object uniqueKey;

        private ProcessorCacheKey(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
        {
            this.filter = filter;
            this.uniqueKey = uniqueKey;
            this.projections = ImmutableList.copyOf(projections);
        }

        private RowExpression getFilter()
        {
            return filter;
        }

        private List<RowExpression> getProjections()
        {
            return projections;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filter, projections, uniqueKey);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ProcessorCacheKey other = (ProcessorCacheKey) obj;
            return Objects.equals(this.filter, other.filter) &&
                    Objects.equals(this.projections, other.projections) &&
                    Objects.equals(this.uniqueKey, other.uniqueKey);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("uniqueKey", uniqueKey)
                    .toString();
        }
    }

    private static final class JoinFilterCacheKey
    {
        private final RowExpression filter;
        private final int leftBlocksSize;

        public JoinFilterCacheKey(RowExpression filter, int leftBlocksSize)
        {
            this.filter = filter;
            this.leftBlocksSize = leftBlocksSize;
        }

        public RowExpression getFilter()
        {
            return filter;
        }

        public int getLeftBlocksSize()
        {
            return leftBlocksSize;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JoinFilterCacheKey that = (JoinFilterCacheKey) o;
            return leftBlocksSize == that.leftBlocksSize &&
                    Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filter, leftBlocksSize);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("leftBlocksSize", leftBlocksSize)
                    .toString();
        }
    }
}
