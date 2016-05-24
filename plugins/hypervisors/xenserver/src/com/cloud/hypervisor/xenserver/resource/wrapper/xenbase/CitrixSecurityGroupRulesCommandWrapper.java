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

import com.cloud.hypervisor.xenserver.resource.XenServerResourceBase;
import org.apache.log4j.Logger;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.SecurityGroupRuleAnswer;
import com.cloud.agent.api.SecurityGroupRulesCmd;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.xensource.xenapi.Connection;

@ResourceWrapper(handles =  SecurityGroupRulesCmd.class)
public final class CitrixSecurityGroupRulesCommandWrapper extends CommandWrapper<SecurityGroupRulesCmd, Answer, XenServerResourceBase> {

    private static final Logger s_logger = Logger.getLogger(CitrixSecurityGroupRulesCommandWrapper.class);

    @Override
    public Answer execute(final SecurityGroupRulesCmd command, final XenServerResourceBase xenServerResourceBase) {
        final Connection conn = xenServerResourceBase.getConnection();
        if (s_logger.isTraceEnabled()) {
            s_logger.trace("Sending network rules command to " + xenServerResourceBase.getHost().getIp());
        }

        if (!xenServerResourceBase.canBridgeFirewall()) {
            s_logger.warn("Host " + xenServerResourceBase.getHost().getIp() + " cannot do bridge firewalling");
            return new SecurityGroupRuleAnswer(command, false, "Host " + xenServerResourceBase.getHost().getIp() + " cannot do bridge firewalling",
                    SecurityGroupRuleAnswer.FailureReason.CANNOT_BRIDGE_FIREWALL);
        }

        final String result = xenServerResourceBase.callHostPlugin(conn, "vmops", "network_rules", "vmName", command.getVmName(), "vmIP", command.getGuestIp(), "vmMAC",
                command.getGuestMac(), "vmID", Long.toString(command.getVmId()), "signature", command.getSignature(), "seqno", Long.toString(command.getSeqNum()), "deflated",
                "true", "rules", command.compressStringifiedRules(), "secIps", command.getSecIpsString());

        if (result == null || result.isEmpty() || !Boolean.parseBoolean(result)) {
            s_logger.warn("Failed to program network rules for vm " + command.getVmName());
            return new SecurityGroupRuleAnswer(command, false, "programming network rules failed");
        } else {
            s_logger.info("Programmed network rules for vm " + command.getVmName() + " guestIp=" + command.getGuestIp() + ", ingress numrules="
                    + command.getIngressRuleSet().size() + ", egress numrules=" + command.getEgressRuleSet().size());
            return new SecurityGroupRuleAnswer(command);
        }
    }
}