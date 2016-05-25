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
package com.cloud.hypervisor.xenserver.resource.common;

import com.cloud.utils.exception.CloudRuntimeException;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.Pool;
import com.xensource.xenapi.Task;
import com.xensource.xenapi.Types;
import com.xensource.xenapi.XenAPIObject;
import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Reduce bloat inside XenServerResourceBase
 *
 */
public class XenServerHelper {
    private static final HashMap<String, MemoryValues> XenServerGuestOsMemoryMap = new HashMap<String, MemoryValues>(70);

    private static final Logger s_logger = Logger.getLogger(XenServerHelper.class);

    public static class MemoryValues {
        long max;
        long min;

        public MemoryValues(final long min, final long max) {
            this.min = min * 1024 * 1024;
            this.max = max * 1024 * 1024;
        }

        public long getMax() {
            return max;
        }

        public long getMin() {
            return min;
        }
    }

    static {
        XenServerGuestOsMemoryMap.put("CentOS 4.5 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 4.6 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 4.7 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 4.8 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.0 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.0 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.1 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.1 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.2 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.2 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.3 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.3 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.4 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.4 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.5 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.5 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.6 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.6 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.7 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.7 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.8 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.8 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.9 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.9 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.10 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 5.10 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.0 (32-bit)", new MemoryValues(512l, 8 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.0 (64-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.1 (32-bit)", new MemoryValues(512l, 8 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.1 (64-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.2 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.3 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.3 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.4 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.4 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.5 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("CentOS 6.5 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.0 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.0 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.1 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.1 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.2 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.3 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.3 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.4 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.4 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.5 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.5 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.6 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.6 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.7 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.7 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.8 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.8 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.9 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.9 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.10 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 5.10 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.0 (32-bit)", new MemoryValues(512l, 8 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.0 (64-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.1 (32-bit)", new MemoryValues(512l, 8 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.1 (64-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.2 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.3 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.3 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.4 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.4 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.5 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Oracle Enterprise Linux 6.5 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 4.5 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 4.6 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 4.7 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 4.8 (32-bit)", new MemoryValues(256l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.0 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.0 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.1 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.1 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.2 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.2 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.3 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.3 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.4 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.4 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.5 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.5 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.6 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.6 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.7 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.7 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.8 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.8 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.9 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.9 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.10 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 5.10 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.0 (32-bit)", new MemoryValues(512l, 8 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.0 (64-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.1 (32-bit)", new MemoryValues(512l, 8 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.1 (64-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.2 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.3 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.3 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.4 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.4 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.5 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 6.5 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Red Hat Enterprise Linux 7.0", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Debian GNU/Linux 5.0 (64-bit)", new MemoryValues(128l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Debian GNU/Linux 5(64-bit)", new MemoryValues(128l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Debian GNU/Linux 6(32-bit)", new MemoryValues(128l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Debian GNU/Linux 6(64-bit)", new MemoryValues(128l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Debian GNU/Linux 7(32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Debian GNU/Linux 7(64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP1 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP1 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP2 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP3 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP3 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP4 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 10 SP4 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 SP1 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 SP1 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 SP2 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 SP2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 SP3 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 11 SP3 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 12 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("SUSE Linux Enterprise Server 12 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows 7 (32-bit)", new MemoryValues(1024l, 4 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows 7 (64-bit)", new MemoryValues(2 * 1024l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows 8 (32-bit)", new MemoryValues(1024l, 4 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows 8 (64-bit)", new MemoryValues(2 * 1024l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2003 SP2 (32-bit)", new MemoryValues(256l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2003 SP2 (64-bit)", new MemoryValues(256l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2008 SP2 (32-bit)", new MemoryValues(512l, 64 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2008 SP2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2008 R2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2008 R2 SP1 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2012 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Server 2012 R2 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows Vista SP2 (32-bit)", new MemoryValues(1024l, 4 * 1024l));
        XenServerGuestOsMemoryMap.put("Windows XP SP3 (32-bit)", new MemoryValues(256l, 4 * 1024l));
        XenServerGuestOsMemoryMap.put("Ubuntu 10.04 (32-bit)", new MemoryValues(128l, 512l));
        XenServerGuestOsMemoryMap.put("Ubuntu 10.04 (64-bit)", new MemoryValues(128l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Ubuntu 10.10 (32-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Ubuntu 10.10 (64-bit)", new MemoryValues(512l, 16 * 1024l));
        XenServerGuestOsMemoryMap.put("Ubuntu 12.04 (32-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Ubuntu 12.04 (64-bit)", new MemoryValues(512l, 128 * 1024l));
        XenServerGuestOsMemoryMap.put("Ubuntu 14.04 (32-bit)", new MemoryValues(512l, 32 * 1024l));
        XenServerGuestOsMemoryMap.put("Ubuntu 14.04 (64-bit)", new MemoryValues(512l, 128 * 1024l));
    }

    public static long getXenServerStaticMax(final String stdType, final boolean bootFromCD) {
        final MemoryValues recommendedMaxMinMemory = XenServerGuestOsMemoryMap.get(stdType);
        if (recommendedMaxMinMemory == null) {
            return 0l;
        }
        return recommendedMaxMinMemory.getMax();
    }

    public static long getXenServerStaticMin(final String stdType, final boolean bootFromCD) {
        final MemoryValues recommendedMaxMinMemory = XenServerGuestOsMemoryMap.get(stdType);
        if (recommendedMaxMinMemory == null) {
            return 0l;
        }
        return recommendedMaxMinMemory.getMin();
    }

    public static String getProductVersion(final Host.Record record) {
        String prodVersion = record.softwareVersion.get("product_version");
        if (prodVersion == null) {
            prodVersion = record.softwareVersion.get("platform_version").trim();
        } else {
            prodVersion = prodVersion.trim();
            final String[] items = prodVersion.split("\\.");
            if (Integer.parseInt(items[0]) > 6) {
                prodVersion = "6.5.0";
            } else if (Integer.parseInt(items[0]) == 6 && Integer.parseInt(items[1]) >= 4) {
                prodVersion = "6.5.0";
            }
        }
        return prodVersion;
    }

    public static String getPVbootloaderArgs(String guestOS) {
        if (guestOS.startsWith("SUSE Linux Enterprise Server")) {
            if (guestOS.contains("64-bit")) {
                return "--kernel /boot/vmlinuz-xen --ramdisk /boot/initrd-xen";
            } else if (guestOS.contains("32-bit")) {
                return "--kernel /boot/vmlinuz-xenpae --ramdisk /boot/initrd-xenpae";
            }
        }
        return "";
    }

    public static void waitForTask(final Connection c, final Task task, final long pollInterval, final long timeout) throws TimeoutException, Types.XenAPIException, XmlRpcException {
        final long beginTime = System.currentTimeMillis();
        if (s_logger.isTraceEnabled()) {
            s_logger.trace("Task " + task.getNameLabel(c) + " (" + task.getUuid(c) + ") sent to " + c.getSessionReference() + " is pending completion with a " + timeout
                    + "ms timeout");
        }
        while (task.getStatus(c) == Types.TaskStatusType.PENDING) {
            try {
                if (s_logger.isTraceEnabled()) {
                    s_logger.trace("Task " + task.getNameLabel(c) + " (" + task.getUuid(c) + ") is pending, sleeping for " + pollInterval + "ms");
                }
                Thread.sleep(pollInterval);
            } catch (final InterruptedException e) {
            }
            if (System.currentTimeMillis() - beginTime > timeout) {
                final String msg = "Async " + timeout / 1000 + " seconds timeout for task " + task.toString();
                s_logger.warn(msg);
                task.cancel(c);
                task.destroy(c);
                throw new TimeoutException(msg);
            }
        }
    }

    public static void checkForSuccess(final Connection c, final Task task) throws Types.XenAPIException, XmlRpcException {
        if (task.getStatus(c) == Types.TaskStatusType.SUCCESS) {
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("Task " + task.getNameLabel(c) + " (" + task.getUuid(c) + ") completed");
            }
            return;
        } else {
            final String msg = "Task failed! Task record: " + task.getRecord(c);
            s_logger.warn(msg);
            task.cancel(c);
            task.destroy(c);
            throw new Types.BadAsyncResult(msg);
        }
    }


    public static String callHostPlugin(final Connection conn, final String plugin, final String cmd, XsHost xsHost, final String... params) {
        final Map<String, String> args = new HashMap<String, String>();
        String msg;
        try {
            for (int i = 0; i < params.length; i += 2) {
                args.put(params[i], params[i + 1]);
            }

            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin executing for command " + cmd + " with " + getArgsString(args));
            }
            final Host host = Host.getByUuid(conn, xsHost.getUuid());
            final String result = host.callPlugin(conn, plugin, cmd, args);
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin Result: " + result);
            }
            return result.replace("\n", "");
        } catch (final Types.XenAPIException e) {
            msg = "callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.toString();
            s_logger.warn(msg);
        } catch (final XmlRpcException e) {
            msg = "callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.getMessage();
            s_logger.debug(msg);
        }
        throw new CloudRuntimeException(msg);
    }

    public static String callHostPluginAsync(final Connection conn, final String plugin, final String cmd, final int wait,XsHost xsHost, final Map<String, String> params) {
        final int timeout = wait * 1000;
        final Map<String, String> args = new HashMap<String, String>();
        Task task = null;
        try {
            for (final Map.Entry<String, String> entry : params.entrySet()) {
                args.put(entry.getKey(), entry.getValue());
            }
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin executing for command " + cmd + " with " + getArgsString(args));
            }
            final Host host = Host.getByUuid(conn, xsHost.getUuid());
            task = host.callPluginAsync(conn, plugin, cmd, args);
            // poll every 1 seconds
            waitForTask(conn, task, 1000, timeout);
            checkForSuccess(conn, task);
            final String result = task.getResult(conn);
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin Result: " + result);
            }
            return result.replace("<value>", "").replace("</value>", "").replace("\n", "");
        } catch (final Types.HandleInvalid e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to HandleInvalid clazz:" + e.clazz + ", handle:" + e.handle);
        } catch (final Exception e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.toString(), e);
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e1) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + xsHost.getUuid() + ") due to " + e1.toString());
                }
            }
        }
        return null;
    }

    public static String callHostPluginAsync(final Connection conn, final String plugin, final String cmd, final int wait, XsHost xsHost, final String... params) {
        final int timeout = wait * 1000;
        final Map<String, String> args = new HashMap<String, String>();
        Task task = null;
        try {
            for (int i = 0; i < params.length; i += 2) {
                args.put(params[i], params[i + 1]);
            }
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin executing for command " + cmd + " with " + getArgsString(args));
            }
            final Host host = Host.getByUuid(conn, xsHost.getUuid());
            task = host.callPluginAsync(conn, plugin, cmd, args);
            // poll every 1 seconds
            waitForTask(conn, task, 1000, timeout);
            checkForSuccess(conn, task);
            final String result = task.getResult(conn);
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin Result: " + result);
            }
            return result.replace("<value>", "").replace("</value>", "").replace("\n", "");
        } catch (final Types.HandleInvalid e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to HandleInvalid clazz:" + e.clazz + ", handle:" + e.handle);
        } catch (final Types.XenAPIException e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.toString(), e);
        } catch (final Exception e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.getMessage(), e);
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e1) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + xsHost.getUuid() + ") due to " + e1.toString());
                }
            }
        }
        return null;
    }

    public static String callHostPluginPremium(final Connection conn, final String cmd, XsHost xsHost, final String... params) {
        return callHostPlugin(conn, "vmopspremium", cmd, xsHost, params);
    }

    public static String callHostPluginThroughMaster(final Connection conn, final String plugin, final String cmd, XsHost xsHost, final String... params) {
        final Map<String, String> args = new HashMap<String, String>();

        try {
            final Map<Pool, Pool.Record> poolRecs = Pool.getAllRecords(conn);
            if (poolRecs.size() != 1) {
                throw new CloudRuntimeException("There are " + poolRecs.size() + " pool for host :" + xsHost.getUuid());
            }
            final Host master = poolRecs.values().iterator().next().master;
            for (int i = 0; i < params.length; i += 2) {
                args.put(params[i], params[i + 1]);
            }

            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin executing for command " + cmd + " with " + getArgsString(args));
            }
            final String result = master.callPlugin(conn, plugin, cmd, args);
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin Result: " + result);
            }
            return result.replace("\n", "");
        } catch (final Types.HandleInvalid e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to HandleInvalid clazz:" + e.clazz + ", handle:" + e.handle);
        } catch (final Types.XenAPIException e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.toString(), e);
        } catch (final XmlRpcException e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.getMessage(), e);
        }
        return null;
    }

    public static String getArgsString(final Map<String, String> args) {
        final StringBuilder argString = new StringBuilder();
        for (final Map.Entry<String, String> arg : args.entrySet()) {
            argString.append(arg.getKey() + ": " + arg.getValue() + ", ");
        }
        return argString.toString();
    }

    public static String getGuestOsType(String platformEmulator) {
        if (org.apache.commons.lang.StringUtils.isBlank(platformEmulator)) {
            s_logger.debug("no guest OS type, start it as HVM guest");
            platformEmulator = "Other install media";
        }
        return platformEmulator;
    }

    public static boolean isRefNull(final XenAPIObject object) {
        return object == null || object.toWireString().equals("OpaqueRef:NULL") || object.toWireString().equals("<not in database>");
    }
}
