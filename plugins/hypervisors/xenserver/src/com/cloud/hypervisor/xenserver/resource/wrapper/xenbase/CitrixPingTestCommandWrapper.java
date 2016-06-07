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
import com.cloud.agent.api.PingTestCommand;
import com.cloud.hypervisor.xenserver.resource.XenServerResourceBase;
import com.cloud.hypervisor.xenserver.resource.network.XenServerNetworkResource;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.xensource.xenapi.Connection;

@ResourceWrapper(handles =  PingTestCommand.class)
public final class CitrixPingTestCommandWrapper extends CommandWrapper<PingTestCommand, Answer, XenServerResourceBase> {

    @Override
    public Answer execute(final PingTestCommand command, final XenServerResourceBase xenServerResourceBase) {
        final Connection conn = xenServerResourceBase.getConnection();
        final XenServerNetworkResource networkResource = xenServerResourceBase.getNetworkResource();
        boolean result = false;
        final String computingHostIp = command.getComputingHostIp();

        if (computingHostIp != null) {
            result = networkResource.doPingTest(conn, computingHostIp);
        } else {
            result = networkResource.doPingTest(conn, command.getRouterIp(), command.getPrivateIp());
        }

        if (!result) {
            return new Answer(command, false, "PingTestCommand failed");
        }
        return new Answer(command);
    }
}