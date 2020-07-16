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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HdfsConfig.HdfsDataTranserProtection;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HdfsConfig.class)
                .setResourceConfigFiles("")
                .setNewDirectoryPermissions("0777")
                .setVerifyChecksum(true)
                .setIpcPingInterval(new Duration(10, TimeUnit.SECONDS))
                .setDfsTimeout(new Duration(60, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setDfsKeyProviderCacheTtl(new Duration(30, TimeUnit.MINUTES))
                .setDomainSocketPath(null)
                .setSocksProxy(null)
                .setWireEncryptionEnabled(false)
                .setHdfsDataTransferProtection(HdfsDataTranserProtection.NONE)
                .setFileSystemMaxCacheSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path resource1 = Files.createTempFile(null, null);
        Path resource2 = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.config.resources", resource1.toString() + "," + resource2.toString())
                .put("hive.fs.new-directory-permissions", "0700")
                .put("hive.dfs.verify-checksum", "false")
                .put("hive.dfs.ipc-ping-interval", "34s")
                .put("hive.dfs-timeout", "33s")
                .put("hive.dfs.connect.timeout", "20s")
                .put("hive.dfs.connect.max-retries", "10")
                .put("hive.dfs.key-provider.cache-ttl", "42s")
                .put("hive.dfs.domain-socket-path", "/foo")
                .put("hive.hdfs.socks-proxy", "localhost:4567")
                .put("hive.hdfs.wire-encryption.enabled", "true")
                .put("hive.hdfs.data-transfer-protection", "privacy")
                .put("hive.fs.cache.max-size", "1010")
                .build();

        HdfsConfig expected = new HdfsConfig()
                .setResourceConfigFiles(ImmutableList.of(resource1.toFile(), resource2.toFile()))
                .setNewDirectoryPermissions("0700")
                .setVerifyChecksum(false)
                .setIpcPingInterval(new Duration(34, TimeUnit.SECONDS))
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setDfsKeyProviderCacheTtl(new Duration(42, TimeUnit.SECONDS))
                .setDomainSocketPath("/foo")
                .setSocksProxy(HostAndPort.fromParts("localhost", 4567))
                .setWireEncryptionEnabled(true)
                .setHdfsDataTransferProtection(HdfsDataTranserProtection.PRIVACY)
                .setFileSystemMaxCacheSize(1010);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testHdfsDataTranserProtectionFromString()
    {
        assertThat(HdfsDataTranserProtection.fromString("none")).isEqualTo(HdfsDataTranserProtection.NONE);
        assertThat(HdfsDataTranserProtection.fromString("authentication")).isEqualTo(HdfsDataTranserProtection.AUTHENTICATION);
        assertThat(HdfsDataTranserProtection.fromString("integrity")).isEqualTo(HdfsDataTranserProtection.INTEGRITY);
        assertThat(HdfsDataTranserProtection.fromString("privacy")).isEqualTo(HdfsDataTranserProtection.PRIVACY);
    }
}
