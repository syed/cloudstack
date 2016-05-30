//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package com.cloud.hypervisor.xenserver.resource.wrapper.xen610;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.storage.MigrateVolumeAnswer;
import com.cloud.agent.api.storage.MigrateVolumeCommand;
import com.cloud.agent.api.to.StorageFilerTO;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHelper;
import com.cloud.hypervisor.xenserver.resource.release.XenServer610Resource;
import com.cloud.hypervisor.xenserver.resource.storage.XenServerStorageResource;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.SR;
import com.xensource.xenapi.Task;
import com.xensource.xenapi.Types;
import com.xensource.xenapi.VDI;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

@ResourceWrapper(handles =  MigrateVolumeCommand.class)
public final class XenServer610MigrateVolumeCommandWrapper extends CommandWrapper<MigrateVolumeCommand, Answer, XenServer610Resource> {

    private static final Logger s_logger = Logger.getLogger(XenServer610MigrateVolumeCommandWrapper.class);

    @Override
    public Answer execute(final MigrateVolumeCommand command, final XenServer610Resource xenServer610Resource) {
        final Connection connection = xenServer610Resource.getConnection();
        final XenServerStorageResource storageResource = xenServer610Resource.getStorageResource();
        final String volumeUUID = command.getVolumePath();
        final StorageFilerTO poolTO = command.getPool();

        try {
            final String uuid = poolTO.getUuid();
            final SR destinationPool = storageResource.getStorageRepository(connection, uuid);
            final VDI srcVolume = storageResource.getVDIbyUuid(connection, volumeUUID);
            final Map<String, String> other = new HashMap<String, String>();
            other.put("live", "true");

            // Live migrate the vdi across pool.
            final Task task = srcVolume.poolMigrateAsync(connection, destinationPool, other);
            final long timeout = xenServer610Resource.getMigrateWait() * 1000L;
            XenServerHelper.waitForTask(connection, task, 1000, timeout);
            XenServerHelper.checkForSuccess(connection, task);

            final VDI dvdi = Types.toVDI(task, connection);

            return new MigrateVolumeAnswer(command, true, null, dvdi.getUuid(connection));
        } catch (final Exception e) {
            final String msg = "Catch Exception " + e.getClass().getName() + " due to " + e.toString();
            s_logger.error(msg, e);
            return new MigrateVolumeAnswer(command, false, msg, null);
        }
    }
}