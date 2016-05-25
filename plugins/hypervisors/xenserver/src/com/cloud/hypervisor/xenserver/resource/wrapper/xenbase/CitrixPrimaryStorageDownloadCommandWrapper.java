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
import com.cloud.agent.api.storage.PrimaryStorageDownloadAnswer;
import com.cloud.agent.api.storage.PrimaryStorageDownloadCommand;
import com.cloud.hypervisor.xenserver.resource.XenServerResourceBase;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.SR;
import com.xensource.xenapi.VDI;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.HashMap;
import java.util.Set;

@ResourceWrapper(handles =  PrimaryStorageDownloadCommand.class)
public final class CitrixPrimaryStorageDownloadCommandWrapper extends CommandWrapper<PrimaryStorageDownloadCommand, Answer, XenServerResourceBase> {

    private static final Logger s_logger = Logger.getLogger(CitrixPrimaryStorageDownloadCommandWrapper.class);

    @Override
    public Answer execute(final PrimaryStorageDownloadCommand command, final XenServerResourceBase xenServerResourceBase) {
        final String tmplturl = command.getUrl();
        final String poolName = command.getPoolUuid();
        final int wait = command.getWait();
        try {
            final URI uri = new URI(tmplturl);
            final String tmplpath = uri.getHost() + ":" + uri.getPath();
            final Connection conn = xenServerResourceBase.getConnection();
            SR poolsr = null;
            final Set<SR> srs = SR.getByNameLabel(conn, poolName);
            if (srs.size() != 1) {
                final String msg = "There are " + srs.size() + " SRs with same name: " + poolName;
                s_logger.warn(msg);
                return new PrimaryStorageDownloadAnswer(msg);
            } else {
                poolsr = srs.iterator().next();
            }
            final String pUuid = poolsr.getUuid(conn);
            final boolean isISCSI = xenServerResourceBase.IsISCSI(poolsr.getType(conn));
            final String uuid = xenServerResourceBase.getStorageResource().copyVhdFromSecondaryStorage(conn, tmplpath, pUuid, wait);
            final VDI tmpl = xenServerResourceBase.getVDIbyUuid(conn, uuid);
            final VDI snapshotvdi = tmpl.snapshot(conn, new HashMap<String, String>());
            final String snapshotUuid = snapshotvdi.getUuid(conn);
            snapshotvdi.setNameLabel(conn, "Template " + command.getName());
            final String parentuuid = xenServerResourceBase.getVhdParent(conn, pUuid, snapshotUuid, isISCSI);
            final VDI parent = xenServerResourceBase.getVDIbyUuid(conn, parentuuid);
            final Long phySize = parent.getPhysicalUtilisation(conn);
            tmpl.destroy(conn);
            poolsr.scan(conn);
            try {
                Thread.sleep(5000);
            } catch (final Exception e) {
            }
            return new PrimaryStorageDownloadAnswer(snapshotvdi.getUuid(conn), phySize);
        } catch (final Exception e) {
            final String msg = "Catch Exception " + e.getClass().getName() + " on host:" + xenServerResourceBase.getHost().getUuid() + " for template: " + tmplturl + " due to "
                    + e.toString();
            s_logger.warn(msg, e);
            return new PrimaryStorageDownloadAnswer(msg);
        }
    }
}