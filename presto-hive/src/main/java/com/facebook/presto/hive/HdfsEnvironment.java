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
import com.facebook.presto.hadoop.shaded.com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import javax.inject.Inject;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;

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
    public HdfsEnvironment(HdfsConfiguration hdfsConfiguration, HiveClientConfig config)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.verifyChecksum = requireNonNull(config, "config is null").isVerifyChecksum();
        try {
            this.superUserUgi = Optional.of(UserGroupInformation.loginUserFromKeytabAndReturnUGI("presto@HADOOP.TERADATA.COM", "/Users/losipiuk/tmp/kerberos/presto_superuser.keytab"));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
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

    public FileSystem getFileSystem(Path path, String userName)
            throws IOException
    {
        Optional<UserGroupInformation> userGroupInformation = buildUserGroupInformation(userName);
        if (userGroupInformation.isPresent()) {
            try {
                return userGroupInformation.get().doAs((PrivilegedExceptionAction<FileSystem>) () -> getFileSystem(path));
            }
            catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            return getFileSystem(path);
        }
    }

    private Optional<UserGroupInformation> buildUserGroupInformation(String userName)
    {
        return superUserUgi.map(it -> UserGroupInformation.createProxyUser(userName, it));
    }
}
