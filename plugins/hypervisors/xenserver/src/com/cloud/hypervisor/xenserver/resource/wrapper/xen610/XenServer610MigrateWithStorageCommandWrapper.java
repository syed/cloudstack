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
import com.cloud.agent.api.MigrateWithStorageAnswer;
import com.cloud.agent.api.MigrateWithStorageCommand;
import com.cloud.agent.api.to.NicTO;
import com.cloud.agent.api.to.StorageFilerTO;
import com.cloud.agent.api.to.VirtualMachineTO;
import com.cloud.agent.api.to.VolumeTO;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHelper;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHost;
import com.cloud.hypervisor.xenserver.resource.network.XenServerLocalNetwork;
import com.cloud.hypervisor.xenserver.resource.release.XenServer610Resource;
import com.cloud.hypervisor.xenserver.resource.storage.XenServerStorageResource;
import com.cloud.network.Networks.TrafficType;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.utils.exception.CloudRuntimeException;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.Network;
import com.xensource.xenapi.SR;
import com.xensource.xenapi.Task;
import com.xensource.xenapi.Types;
import com.xensource.xenapi.VDI;
import com.xensource.xenapi.VIF;
import com.xensource.xenapi.VM;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ResourceWrapper(handles =  MigrateWithStorageCommand.class)
public final class XenServer610MigrateWithStorageCommandWrapper extends CommandWrapper<MigrateWithStorageCommand, Answer, XenServer610Resource> {

    private static final Logger s_logger = Logger.getLogger(XenServer610MigrateWithStorageCommandWrapper.class);

    @Override
    public Answer execute(final MigrateWithStorageCommand command, final XenServer610Resource xenServer610Resource) {
        final Connection connection = xenServer610Resource.getConnection();
        final VirtualMachineTO vmSpec = command.getVirtualMachine();
        final XenServerStorageResource storageResource = xenServer610Resource.getStorageResource();
        final Map<VolumeTO, StorageFilerTO> volumeToFiler = command.getVolumeToFiler();
        final String vmName = vmSpec.getName();
        Task task = null;

        final XenServerHost xenServerHost = xenServer610Resource.getHost();
        final String uuid = xenServerHost.getUuid();
        try {
            storageResource.prepareISO(connection, vmName, null, null);

            // Get the list of networks and recreate VLAN, if required.
            for (final NicTO nicTo : vmSpec.getNics()) {
                xenServer610Resource.getNetwork(connection, nicTo);
            }

            final Map<String, String> other = new HashMap<String, String>();
            other.put("live", "true");

            final XenServerLocalNetwork nativeNetworkForTraffic = xenServer610Resource.getNativeNetworkForTraffic(connection, TrafficType.Storage, null);
            final Network networkForSm = nativeNetworkForTraffic.getNetwork();

            // Create the vif map. The vm stays in the same cluster so we have to pass an empty vif map.
            final Map<VIF, Network> vifMap = new HashMap<VIF, Network>();
            final Map<VDI, SR> vdiMap = new HashMap<VDI, SR>();
            for (final Map.Entry<VolumeTO, StorageFilerTO> entry : volumeToFiler.entrySet()) {
                final VolumeTO volume = entry.getKey();
                final StorageFilerTO sotrageFiler = entry.getValue();
                vdiMap.put(storageResource.getVDIbyUuid(connection, volume.getPath()), storageResource.getStorageRepository(connection, sotrageFiler.getUuid()));
            }

            // Get the vm to migrate.
            final Set<VM> vms = VM.getByNameLabel(connection, vmSpec.getName());
            final VM vmToMigrate = vms.iterator().next();

            // Check migration with storage is possible.
            final Host host = Host.getByUuid(connection, uuid);
            final Map<String, String> token = host.migrateReceive(connection, networkForSm, other);
            task = vmToMigrate.assertCanMigrateAsync(connection, token, true, vdiMap, vifMap, other);
            try {
                // poll every 1 seconds
                final long timeout = xenServer610Resource.getMigrateWait() * 1000L;
                XenServerHelper.waitForTask(connection, task, 1000, timeout);
                XenServerHelper.checkForSuccess(connection, task);
            } catch (final Types.HandleInvalid e) {
                s_logger.error("Error while checking if vm " + vmName + " can be migrated to the destination host " + host, e);
                throw new CloudRuntimeException("Error while checking if vm " + vmName + " can be migrated to the " + "destination host " + host, e);
            }

            // Migrate now.
            task = vmToMigrate.migrateSendAsync(connection, token, true, vdiMap, vifMap, other);
            try {
                // poll every 1 seconds.
                final long timeout = xenServer610Resource.getMigrateWait() * 1000L;
                XenServerHelper.waitForTask(connection, task, 1000, timeout);
                XenServerHelper.checkForSuccess(connection, task);
            } catch (final Types.HandleInvalid e) {
                s_logger.error("Error while migrating vm " + vmName + " to the destination host " + host, e);
                throw new CloudRuntimeException("Error while migrating vm " + vmName + " to the destination host " + host, e);
            }

            // Volume paths would have changed. Return that information.
            final List<VolumeObjectTO> volumeToList = xenServer610Resource.getUpdatedVolumePathsOfMigratedVm(connection, vmToMigrate, vmSpec.getDisks());
            vmToMigrate.setAffinity(connection, host);
            return new MigrateWithStorageAnswer(command, volumeToList);
        } catch (final Exception e) {
            s_logger.warn("Catch Exception " + e.getClass().getName() + ". Storage motion failed due to " + e.toString(), e);
            return new MigrateWithStorageAnswer(command, e);
        } finally {
            if (task != null) {
                try {
                    task.destroy(connection);
                } catch (final Exception e) {
                    s_logger.debug("Unable to destroy task " + task.toString() + " on host " + uuid + " due to " + e.toString());
                }
            }
        }
    }
}