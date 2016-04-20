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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.StringWriter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class NfsSecondaryStorageResourceTest {

    private NfsSecondaryStorageResource resource;

    @Before
    public void setUp() {
        resource = new NfsSecondaryStorageResource();
    }

    @Test
    @PrepareForTest(NfsSecondaryStorageResource.class)
    public void testSwiftWriteMetadataFile() throws Exception {
        String expected = "uniquename=test\nfilename=testfile\nsize=100\nvirtualsize=1000";

        StringWriter stringWriter = new StringWriter();
        BufferedWriter bufferWriter = new BufferedWriter(stringWriter);
        PowerMockito.whenNew(BufferedWriter.class).withArguments(Mockito.any(FileWriter.class)).thenReturn(bufferWriter);

        resource.swiftWriteMetadataFile("testfile", "test", "testfile", 100, 1000);

        Assert.assertEquals(expected, stringWriter.toString());
    }


}
