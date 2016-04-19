/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cloudstack.storage.resource;

import com.cloud.utils.PropertiesUtil;
import com.cloud.utils.exception.CloudRuntimeException;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.cloudstack.storage.command.DeleteCommand;
import org.apache.cloudstack.storage.to.TemplateObjectTO;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import javax.naming.ConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

public class NfsSecondaryStorageResourceTest extends TestCase {
    private static Map<String, Object> testParams;

    private static final Logger s_logger = Logger.getLogger(NfsSecondaryStorageResourceTest.class.getName());

    NfsSecondaryStorageResource resource;

    @Before
    @Override
    public void setUp() throws ConfigurationException {
        s_logger.setLevel(Level.ALL);
        resource = new NfsSecondaryStorageResource();
        resource.setInSystemVM(true);
        testParams = PropertiesUtil.toMap(loadProperties());
        resource.configureStorageLayerClass(testParams);
        Object testLocalRoot = testParams.get("testLocalRoot");
        if (testLocalRoot != null) {
            resource.setParentPath((String) testLocalRoot);
        }
    }

    @Test
    public void testMount() throws Exception {
        String sampleUriStr = "cifs://192.168.1.128/CSHV3?user=administrator&password=1pass%40word1&foo=bar";
        URI sampleUri = new URI(sampleUriStr);

        s_logger.info("Check HostIp parsing");
        String hostIpStr = resource.getUriHostIp(sampleUri);
        Assert.assertEquals("Expected host IP " + sampleUri.getHost() + " and actual host IP " + hostIpStr + " differ.", sampleUri.getHost(), hostIpStr);

        s_logger.info("Check option parsing");
        String expected = "user=administrator,password=1pass@word1,foo=bar,";
        String actualOpts = resource.parseCifsMountOptions(sampleUri);
        Assert.assertEquals("Options should be " + expected + " and not " + actualOpts, expected, actualOpts);

        // attempt a configured mount
        final Map<String, Object> params = PropertiesUtil.toMap(loadProperties());
        String sampleMount = (String)params.get("testCifsMount");
        if (!sampleMount.isEmpty()) {
            s_logger.info("functional test, mount " + sampleMount);
            URI realMntUri = new URI(sampleMount);
            String mntSubDir = resource.mountUri(realMntUri);
            s_logger.info("functional test, umount " + mntSubDir);
            resource.umount(resource.getMountingRoot() + mntSubDir, realMntUri);
        } else {
            s_logger.info("no entry for testCifsMount in " + "./conf/agent.properties - skip functional test");
        }
    }

    @Test
    public void testSwiftWriteMetadataFile() throws Exception {

        String expected = "uniquename=test\nfilename=testfile\nsize=100\nvirtualsize=1000";
        final List<String> resultArray = new ArrayList<String>();

        File mockedFile = Mockito.mock(File.class);
        FileWriter mockedFileWriter = Mockito.mock(FileWriter.class);
        BufferedWriter mockedBufferedWriter = Mockito.mock(BufferedWriter.class);

        PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(mockedFile);
        PowerMockito.whenNew(FileWriter.class).withAnyArguments().thenReturn(mockedFileWriter);
        PowerMockito.whenNew(BufferedWriter.class).withAnyArguments().thenReturn(mockedBufferedWriter);

        Mockito.doNothing().when(mockedFileWriter).close();
        Mockito.doNothing().when(mockedBufferedWriter).close();
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                resultArray.add((String) invocation.getArguments()[0]);
                return null;
            }
        }).when(mockedBufferedWriter).write(anyString());

        resource.swiftWriteMetadataFile("testfile", "test", "testfile", 100, 1000);

        String output = "";
        for (String o : resultArray) {
            output = output.concat(o);
        }

        assertEquals(expected, output);
    }

    @Test
    public void testCleanupNfsStaging(){
        TemplateObjectTO templateMock = Mockito.mock(TemplateObjectTO.class);
        Exception exception = new Exception();
        resource.logger = Mockito.mock(Logger.class);

        Mockito.doNothing().when(resource.logger).debug("Failed to clean up staging area:", exception);
        Mockito.when(resource.execute(any(DeleteCommand.class))).thenThrow(exception);

        resource.cleanupStagingNfs(templateMock);
        Mockito.verify(resource.logger).debug("Failed to clean up staging area:", exception);
    }

    public static Properties loadProperties() throws ConfigurationException {
        Properties properties = new Properties();
        final File file = PropertiesUtil.findConfigFile("agent.properties");
        if (file == null) {
            throw new ConfigurationException("Unable to find agent.properties.");
        }
        s_logger.info("agent.properties found at " + file.getAbsolutePath());
        try(FileInputStream fs = new FileInputStream(file);) {
            properties.load(fs);
        } catch (final FileNotFoundException ex) {
            throw new CloudRuntimeException("Cannot find the file: " + file.getAbsolutePath(), ex);
        } catch (final IOException ex) {
            throw new CloudRuntimeException("IOException in reading " + file.getAbsolutePath(), ex);
        }
        return properties;
    }

}
