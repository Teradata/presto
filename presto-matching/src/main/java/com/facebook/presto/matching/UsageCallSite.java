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

import com.google.common.base.Splitter;

import static com.google.common.collect.Iterables.getLast;
import static java.lang.String.format;
import static java.util.Arrays.stream;

public class UsageCallSite
{
    private final String className;
    private final String methodName;
    private final String fileName;
    private final int lineNumber;

    public static UsageCallSite get()
    {
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        StackTraceElement callSite = stream(stackTrace)
                .filter(element -> !packageName(element).equals("com.facebook.presto.matching"))
                .findFirst()
                .get();
        return new UsageCallSite(
                callSite.getClassName(),
                callSite.getMethodName(),
                callSite.getFileName(),
                callSite.getLineNumber());
    }

    private static String packageName(StackTraceElement stackTraceElement)
    {
        return stackTraceElement.getClassName().replaceAll("\\.[_\\w\\d]*$", "");
    }

    private UsageCallSite(String className, String methodName, String fileName, int lineNumber)
    {
        this.className = className;
        this.methodName = methodName;
        this.fileName = fileName;
        this.lineNumber = lineNumber;
    }

    @Override
    public String toString()
    {
        return format("%s.%s(%s:%d)", abbreviate(className), methodName, fileName, lineNumber);
    }

    private String abbreviate(String className)
    {
        Iterable<String> parts = Splitter.on(".").split(className);
        String simpleName = getLast(parts);
        return simpleName.replaceAll("[^A-Z]", "");
    }

    public String getClassName()
    {
        return className;
    }

    public String getMethodName()
    {
        return methodName;
    }

    public String getFileName()
    {
        return fileName;
    }

    public int getLineNumber()
    {
        return lineNumber;
    }
}
