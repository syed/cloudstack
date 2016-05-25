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

package com.cloud.hypervisor.xenserver.resource.wrapper.xenbase;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.CreateStoragePoolCommand;
import com.cloud.agent.api.to.StorageFilerTO;
import com.cloud.hypervisor.xenserver.resource.XenServerResourceBase;
import com.cloud.hypervisor.xenserver.resource.storage.XenServerStorageResource;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.storage.Storage.StoragePoolType;
import com.xensource.xenapi.Connection;
import org.apache.log4j.Logger;

@ResourceWrapper(handles =  CreateStoragePoolCommand.class)
public final class CitrixCreateStoragePoolCommandWrapper extends CommandWrapper<CreateStoragePoolCommand, Answer, XenServerResourceBase> {

    private static final Logger s_logger = Logger.getLogger(CitrixCreateStoragePoolCommandWrapper.class);

    @Override
    public Answer execute(final CreateStoragePoolCommand command, final XenServerResourceBase xenServerResourceBase) {
        final XenServerStorageResource storageResource = xenServerResourceBase.getStorageResource();
        final Connection conn = xenServerResourceBase.getConnection();
        final StorageFilerTO pool = command.getPool();
        try {
            if (pool.getType() == StoragePoolType.NetworkFilesystem) {
                storageResource.getNfsSR(conn, Long.toString(pool.getId()), pool.getUuid(), pool.getHost(), pool.getPath(), pool.toString());
            } else if (pool.getType() == StoragePoolType.IscsiLUN) {
                storageResource.getIscsiSR(conn, pool.getUuid(), pool.getHost(), pool.getPath(), null, null, false);
            } else if (pool.getType() == StoragePoolType.PreSetup) {
            } else {
                return new Answer(command, false, "The pool type: " + pool.getType().name() + " is not supported.");
            }
            return new Answer(command, true, "success");
        } catch (final Exception e) {
            final String msg = "Catch Exception " + e.getClass().getName() + ", create StoragePool failed due to " + e.toString() + " on host:"
                    + xenServerResourceBase.getHost().getUuid() + " pool: " + pool.getHost() + pool.getPath();
            s_logger.warn(msg, e);
            return new Answer(command, false, msg);
        }
    }
}