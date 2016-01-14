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
package com.facebook.presto.hive;

import com.facebook.presto.hadoop.HadoopFileSystemCache;
import com.facebook.presto.hadoop.HadoopNative;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import javax.inject.Inject;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopFileSystemCache.initialize();
    }

    private final HdfsConfiguration hdfsConfiguration;
    private final boolean verifyChecksum;
    private final Optional<UserGroupInformation> superUserUgi;

    @Inject
    public HdfsEnvironment(HdfsConfiguration hdfsConfiguration, HiveClientConfig config, Optional<UserGroupInformation> superUserUgi)
    {
        this.superUserUgi = superUserUgi;
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.verifyChecksum = requireNonNull(config, "config is null").isVerifyChecksum();
    }

    public Configuration getConfiguration(Path path)
    {
        return hdfsConfiguration.getConfiguration(path.toUri());
    }

    public FileSystem getFileSystem(Path path)
            throws IOException
    {
        FileSystem fileSystem = path.getFileSystem(getConfiguration(path));
        fileSystem.setVerifyChecksum(verifyChecksum);

        return fileSystem;
    }

    public <T> T doAs(Callable<T> action, String user)
            throws Exception
    {
        Optional<UserGroupInformation> effectiveUserGroupInformation = buildUserGroupInformation(user);

        if (!effectiveUserGroupInformation.isPresent()) {
            return action.call();
        }
        else {
            return effectiveUserGroupInformation.get().doAs(
                    (PrivilegedExceptionAction<T>) () -> action.call());
        }
    }

    private Optional<UserGroupInformation> buildUserGroupInformation(String userName)
    {
        return superUserUgi.map(it -> UserGroupInformation.createProxyUser(userName, it));
    }
}
