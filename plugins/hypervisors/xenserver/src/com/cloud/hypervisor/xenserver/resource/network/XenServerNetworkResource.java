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

package com.cloud.hypervisor.xenserver.resource.network;

import com.cloud.agent.api.SetupGuestNetworkCommand;
import com.cloud.agent.api.routing.IpAssocCommand;
import com.cloud.agent.api.routing.IpAssocVpcCommand;
import com.cloud.agent.api.routing.NetworkElementCommand;
import com.cloud.agent.api.routing.SetNetworkACLCommand;
import com.cloud.agent.api.routing.SetSourceNatCommand;
import com.cloud.agent.api.to.IpAddressTO;
import com.cloud.agent.api.to.NicTO;
import com.cloud.agent.api.to.VirtualMachineTO;
import com.cloud.exception.InternalErrorException;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHelper;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHost;
import com.cloud.network.Networks;
import com.cloud.network.Networks.BroadcastDomainType;
import com.cloud.utils.ExecutionResult;
import com.cloud.utils.Pair;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.net.NetUtils;
import com.cloud.utils.ssh.SSHCmdHelper;
import com.xensource.xenapi.Bond;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.Network;
import com.xensource.xenapi.PIF;
import com.xensource.xenapi.Types;
import com.xensource.xenapi.VIF;
import com.xensource.xenapi.VLAN;
import com.xensource.xenapi.VM;
import org.apache.log4j.Logger;

import com.xensource.xenapi.Types.XenAPIException;
import com.xensource.xenapi.Types.BadServerResponse;
import org.apache.xmlrpc.XmlRpcException;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class XenServerNetworkResource {

    private static final Logger s_logger = Logger.getLogger(XenServerNetworkResource.class);
    private boolean _canBridgeFirewall;

    private XenServerHost _host;
    private String _instance;

    public XenServerNetworkResource(boolean _canBridgeFirewall, XenServerHost host, String _instance) {
        this._canBridgeFirewall = _canBridgeFirewall;
        this._host = host;
        this._instance = _instance;
    }

    public boolean canBridgeFirewall() {
        return _canBridgeFirewall;
    }

    public boolean canBridgeFirewall(final Connection conn) {
        return Boolean.valueOf(XenServerHelper.callHostPlugin(conn, "vmops", "can_bridge_firewall", _host, "host_uuid", _host.getUuid(), "instance", _instance));
    }

    protected ExecutionResult cleanupNetworkElementCommand(final IpAssocCommand cmd) {
        final Connection conn = getConnection();
        final String routerName = cmd.getAccessDetail(NetworkElementCommand.ROUTER_NAME);
        final String routerIp = cmd.getAccessDetail(NetworkElementCommand.ROUTER_IP);
        try {
            final IpAddressTO[] ips = cmd.getIpAddresses();
            final int ipsCount = ips.length;
            for (final IpAddressTO ip : ips) {

                final VM router = getVM(conn, routerName);

                final NicTO nic = new NicTO();
                nic.setMac(ip.getVifMacAddress());
                nic.setType(ip.getTrafficType());
                if (ip.getBroadcastUri() == null) {
                    nic.setBroadcastType(BroadcastDomainType.Native);
                } else {
                    final URI uri = BroadcastDomainType.fromString(ip.getBroadcastUri());
                    nic.setBroadcastType(BroadcastDomainType.getSchemeValue(uri));
                    nic.setBroadcastUri(uri);
                }
                nic.setDeviceId(0);
                nic.setNetworkRateMbps(ip.getNetworkRate());
                nic.setName(ip.getNetworkName());

                Network network = getNetwork(conn, nic);

                // If we are disassociating the last IP address in the VLAN, we
                // need
                // to remove a VIF
                boolean removeVif = false;

                // there is only one ip in this public vlan and removing it, so
                // remove the nic
                if (ipsCount == 1 && !ip.isAdd()) {
                    removeVif = true;
                }

                if (removeVif) {

                    // Determine the correct VIF on DomR to
                    // associate/disassociate the
                    // IP address with
                    final VIF correctVif = getCorrectVif(conn, router, network);
                    if (correctVif != null) {
                        network = correctVif.getNetwork(conn);

                        // Mark this vif to be removed from network usage
                        networkUsage(conn, routerIp, "deleteVif", "eth" + correctVif.getDevice(conn));

                        // Remove the VIF from DomR
                        correctVif.unplug(conn);
                        correctVif.destroy(conn);

                        // Disable the VLAN network if necessary
                        disableVlanNetwork(conn, network);
                    }
                }
            }
        } catch (final Exception e) {
            s_logger.debug("Ip Assoc failure on applying one ip due to exception:  ", e);
            return new ExecutionResult(false, e.getMessage());
        }
        return new ExecutionResult(true, null);
    }

        public void cleanUpTmpDomVif(final Connection conn, final Network nw) throws XenAPIException, XmlRpcException {

            final Pair<VM, VM.Record> vm = getControlDomain(conn);
            final VM dom0 = vm.first();
            final Set<VIF> dom0Vifs = dom0.getVIFs(conn);
            for (final VIF v : dom0Vifs) {
                String vifName = "unknown";
                try {
                    final VIF.Record vifr = v.getRecord(conn);
                    if (v.getNetwork(conn).getUuid(conn).equals(nw.getUuid(conn))) {
                        if (vifr != null) {
                            final Map<String, String> config = vifr.otherConfig;
                            vifName = config.get("nameLabel");
                        }
                        s_logger.debug("A VIF in dom0 for the network is found - so destroy the vif");
                        v.destroy(conn);
                        s_logger.debug("Destroy temp dom0 vif" + vifName + " success");
                    }
                } catch (final Exception e) {
                    s_logger.warn("Destroy temp dom0 vif " + vifName + "failed", e);
                }
            }
        }

    /**
     * This method creates a XenServer network and configures it for being used
     * as a L2-in-L3 tunneled network
     */
    public synchronized Network configureTunnelNetwork(final Connection conn, final Long networkId, final long hostId, final String bridgeName) {
        try {
            final Network nw = findOrCreateTunnelNetwork(conn, bridgeName);
            // Invoke plugin to setup the bridge which will be used by this
            // network
            final String bridge = nw.getBridge(conn);
            final Map<String, String> nwOtherConfig = nw.getOtherConfig(conn);
            final String configuredHosts = nwOtherConfig.get("ovs-host-setup");
            boolean configured = false;
            if (configuredHosts != null) {
                final String hostIdsStr[] = configuredHosts.split(",");
                for (final String hostIdStr : hostIdsStr) {
                    if (hostIdStr.equals(((Long) hostId).toString())) {
                        configured = true;
                        break;
                    }
                }
            }

            if (!configured) {
                String result;
                if (bridgeName.startsWith("OVS-DR-VPC-Bridge")) {
                    result = callHostPlugin(conn, "ovstunnel", "setup_ovs_bridge_for_distributed_routing", "bridge", bridge, "key", bridgeName, "xs_nw_uuid", nw.getUuid(conn),
                            "cs_host_id", ((Long) hostId).toString());
                } else {
                    result = callHostPlugin(conn, "ovstunnel", "setup_ovs_bridge", "bridge", bridge, "key", bridgeName, "xs_nw_uuid", nw.getUuid(conn), "cs_host_id",
                            ((Long) hostId).toString());
                }

                // Note down the fact that the ovs bridge has been setup
                final String[] res = result.split(":");
                if (res.length != 2 || !res[0].equalsIgnoreCase("SUCCESS")) {
                    // TODO: Should make this error not fatal?
                    throw new CloudRuntimeException("Unable to pre-configure OVS bridge " + bridge);
                }
            }
            return nw;
        } catch (final Exception e) {
            s_logger.warn("createandConfigureTunnelNetwork failed", e);
            return null;
        }

    }

        public VIF createVif(final Connection conn, final String vmName, final VM vm, final VirtualMachineTO vmSpec, final NicTO nic) throws XmlRpcException, XenAPIException {
        assert nic.getUuid() != null : "Nic should have a uuid value";

        if (s_logger.isDebugEnabled()) {
            s_logger.debug("Creating VIF for " + vmName + " on nic " + nic);
        }
        VIF.Record vifr = new VIF.Record();
        vifr.VM = vm;
        vifr.device = Integer.toString(nic.getDeviceId());
        vifr.MAC = nic.getMac();

        // Nicira needs these IDs to find the NIC
        vifr.otherConfig = new HashMap<String, String>();
        vifr.otherConfig.put("nicira-iface-id", nic.getUuid());
        vifr.otherConfig.put("nicira-vm-id", vm.getUuid(conn));
        // Provide XAPI with the cloudstack vm and nic uids.
        vifr.otherConfig.put("cloudstack-nic-id", nic.getUuid());
        if (vmSpec != null) {
            vifr.otherConfig.put("cloudstack-vm-id", vmSpec.getUuid());
        }

        // OVS plugin looks at network UUID in the vif 'otherconfig' details to
        // group VIF's & tunnel ports as part of tier
        // when bridge is setup for distributed routing
        vifr.otherConfig.put("cloudstack-network-id", nic.getNetworkUuid());

        // Nuage Vsp needs Virtual Router IP to be passed in the otherconfig
        // get the virtual router IP information from broadcast uri
        final URI broadcastUri = nic.getBroadcastUri();
        if (broadcastUri != null && broadcastUri.getScheme().equalsIgnoreCase(Networks.BroadcastDomainType.Vsp.scheme())) {
            final String path = broadcastUri.getPath();
            vifr.otherConfig.put("vsp-vr-ip", path.substring(1));
        }
        vifr.network = getNetwork(conn, nic);

        if (nic.getNetworkRateMbps() != null && nic.getNetworkRateMbps().intValue() != -1) {
            vifr.qosAlgorithmType = "ratelimit";
            vifr.qosAlgorithmParams = new HashMap<String, String>();
            // convert mbs to kilobyte per second
            vifr.qosAlgorithmParams.put("kbps", Integer.toString(nic.getNetworkRateMbps() * 128));
        }

        vifr.lockingMode = Types.VifLockingMode.NETWORK_DEFAULT;
        final VIF vif = VIF.create(conn, vifr);
        if (s_logger.isDebugEnabled()) {
            vifr = vif.getRecord(conn);
            if (vifr != null) {
                s_logger.debug("Created a vif " + vifr.uuid + " on " + nic.getDeviceId());
            }
        }

        return vif;
    }

        public synchronized void destroyTunnelNetwork(final Connection conn, final Network nw, final long hostId) {
        try {
            final String bridge = nw.getBridge(conn);
            final String result = callHostPlugin(conn, "ovstunnel", "destroy_ovs_bridge", "bridge", bridge, "cs_host_id", ((Long) hostId).toString());
            final String[] res = result.split(":");
            if (res.length != 2 || !res[0].equalsIgnoreCase("SUCCESS")) {
                // TODO: Should make this error not fatal?
                // Can Concurrent VM shutdown/migration/reboot events can cause
                // this method
                // to be executed on a bridge which has already been removed?
                throw new CloudRuntimeException("Unable to remove OVS bridge " + bridge + ":" + result);
            }
            return;
        } catch (final Exception e) {
            s_logger.warn("destroyTunnelNetwork failed:", e);
            return;
        }
    }

    public void disableVlanNetwork(final Connection conn, final Network network) {
    }
        public boolean doPingTest(final Connection conn, final String computingHostIp) {
        final com.trilead.ssh2.Connection sshConnection = new com.trilead.ssh2.Connection(_host.getIp(), 22);
        try {
            sshConnection.connect(null, 60000, 60000);
            if (!sshConnection.authenticateWithPassword(_username, _password.peek())) {
                throw new CloudRuntimeException("Unable to authenticate");
            }

            final String cmd = "ping -c 2 " + computingHostIp;
            if (!SSHCmdHelper.sshExecuteCmd(sshConnection, cmd)) {
                throw new CloudRuntimeException("Cannot ping host " + computingHostIp + " from host " + _host.getIp());
            }
            return true;
        } catch (final Exception e) {
            s_logger.warn("Catch exception " + e.toString(), e);
            return false;
        } finally {
            sshConnection.close();
        }
    }

    public boolean doPingTest(final Connection conn, final String domRIp, final String vmIp) {
        final String args = "-i " + domRIp + " -p " + vmIp;
        final String result = callHostPlugin(conn, "vmops", "pingtest", "args", args);
        if (result == null || result.isEmpty()) {
            return false;
        }
        return true;
    }

    /**
     * enableVlanNetwork creates a Network object, Vlan object, and thereby a
     * tagged PIF object in Xapi.
     *
     * In XenServer, VLAN is added by - Create a network, which is unique
     * cluster wide. - Find the PIF that you want to create the VLAN on. -
     * Create a VLAN using the network and the PIF. As a result of this
     * operation, a tagged PIF object is also created.
     *
     * Here is a list of problems with clustered Xapi implementation that we are
     * trying to circumvent. - There can be multiple Networks with the same
     * name-label so searching using name-label is not unique. - There are no
     * other ways to search for Networks other than listing all of them which is
     * not efficient in our implementation because we can have over 4000 VLAN
     * networks. - In a clustered situation, it's possible for both hosts to
     * detect that the Network is missing and both creates it. This causes a lot
     * of problems as one host may be using one Network and another may be using
     * a different network for their VMs. This causes problems in migration
     * because the VMs are logically attached to different networks in Xapi's
     * database but in reality, they are attached to the same network.
     *
     * To work around these problems, we do the following.
     *
     * - When creating the VLAN network, we name it as VLAN-UUID of the Network
     * it is created on-VLAN Tag. Because VLAN tags is unique with one
     * particular network, this is a unique name-label to quickly retrieve the
     * the VLAN network with when we need it again. - When we create the VLAN
     * network, we add a timestamp and a random number as a tag into the
     * network. Then instead of creating VLAN on that network, we actually
     * retrieve the Network again and this time uses the VLAN network with
     * lowest timestamp or lowest random number as the VLAN network. This allows
     * VLAN creation to happen on multiple hosts concurrently but even if two
     * VLAN networks were created with the same name, only one of them is used.
     *
     * One cavaet about this approach is that it relies on the timestamp to be
     * relatively accurate among different hosts.
     *
     * @param conn
     *            Xapi Connection
     * @param tag
     *            VLAN tag
     * @param network
     *            network on this host to create the VLAN on.
     * @return VLAN Network created.
     * @throws XenAPIException
     * @throws XmlRpcException
     */
    protected Network enableVlanNetwork(final Connection conn, final long tag, final XsLocalNetwork network) throws XenAPIException, XmlRpcException {
        Network vlanNetwork = null;
        final String oldName = "VLAN" + Long.toString(tag);
        final String newName = "VLAN-" + network.getNetworkRecord(conn).uuid + "-" + tag;
        XsLocalNetwork vlanNic = getNetworkByName(conn, newName);
        if (vlanNic == null) {
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Couldn't find vlan network with the new name so trying old name: " + oldName);
            }
            vlanNic = getNetworkByName(conn, oldName);
            if (vlanNic != null) {
                s_logger.info("Renaming VLAN with old name " + oldName + " to " + newName);
                vlanNic.getNetwork().setNameLabel(conn, newName);
            }
        }
        if (vlanNic == null) { // Can't find it, then create it.
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Creating VLAN network for " + tag + " on host " + _host.getIp());
            }
            final Network.Record nwr = new Network.Record();
            nwr.nameLabel = newName;
            nwr.tags = new HashSet<String>();
            nwr.tags.add(generateTimeStamp());
            vlanNetwork = Network.create(conn, nwr);
            vlanNic = getNetworkByName(conn, newName);
            if (vlanNic == null) { // Still vlanNic is null means we could not
                // create it for some reason and no exception
                // capture happened.
                throw new CloudRuntimeException("Could not find/create vlan network with name: " + newName);
            }
        }

        final PIF nPif = network.getPif(conn);
        final PIF.Record nPifr = network.getPifRecord(conn);

        vlanNetwork = vlanNic.getNetwork();
        if (vlanNic.getPif(conn) != null) {
            return vlanNetwork;
        }

        if (s_logger.isDebugEnabled()) {
            s_logger.debug("Creating VLAN " + tag + " on host " + _host.getIp() + " on device " + nPifr.device);
        }
        final VLAN vlan = VLAN.create(conn, nPif, tag, vlanNetwork);
        if (vlan != null) {
            final VLAN.Record vlanr = vlan.getRecord(conn);
            if (vlanr != null) {
                if (s_logger.isDebugEnabled()) {
                    s_logger.debug("VLAN is created for " + tag + ".  The uuid is " + vlanr.uuid);
                }
            }
        }
        return vlanNetwork;
    }
    /**
     * This method just creates a XenServer network following the tunnel network
     * naming convention
     */
    public synchronized Network findOrCreateTunnelNetwork(final Connection conn, final String nwName) {
        try {
            Network nw = null;
            final Network.Record rec = new Network.Record();
            final Set<Network> networks = Network.getByNameLabel(conn, nwName);

            if (networks.size() == 0) {
                rec.nameDescription = "tunnel network id# " + nwName;
                rec.nameLabel = nwName;
                // Initialize the ovs-host-setup to avoid error when doing
                // get-param in plugin
                final Map<String, String> otherConfig = new HashMap<String, String>();
                otherConfig.put("ovs-host-setup", "");
                // Mark 'internal network' as shared so bridge gets
                // automatically created on each host in the cluster
                // when VM with vif connected to this internal network is
                // started
                otherConfig.put("assume_network_is_shared", "true");
                rec.otherConfig = otherConfig;
                nw = Network.create(conn, rec);
                s_logger.debug("### XenServer network for tunnels created:" + nwName);
            } else {
                nw = networks.iterator().next();
                s_logger.debug("XenServer network for tunnels found:" + nwName);
            }
            return nw;
        } catch (final Exception e) {
            s_logger.warn("createTunnelNetwork failed", e);
            return null;
        }
    }
    protected VIF getCorrectVif(final Connection conn, final VM router, final IpAddressTO ip) throws XmlRpcException, XenAPIException {
        final NicTO nic = new NicTO();
        nic.setType(ip.getTrafficType());
        nic.setName(ip.getNetworkName());
        if (ip.getBroadcastUri() == null) {
            nic.setBroadcastType(BroadcastDomainType.Native);
        } else {
            final URI uri = BroadcastDomainType.fromString(ip.getBroadcastUri());
            nic.setBroadcastType(BroadcastDomainType.getSchemeValue(uri));
            nic.setBroadcastUri(uri);
        }
        final Network network = getNetwork(conn, nic);
        // Determine the correct VIF on DomR to associate/disassociate the
        // IP address with
        final Set<VIF> routerVIFs = router.getVIFs(conn);
        for (final VIF vif : routerVIFs) {
            final Network vifNetwork = vif.getNetwork(conn);
            if (vifNetwork.getUuid(conn).equals(network.getUuid(conn))) {
                return vif;
            }
        }
        return null;
    }

    protected VIF getCorrectVif(final Connection conn, final VM router, final Network network) throws XmlRpcException, XenAPIException {
        final Set<VIF> routerVIFs = router.getVIFs(conn);
        for (final VIF vif : routerVIFs) {
            final Network vifNetwork = vif.getNetwork(conn);
            if (vifNetwork.getUuid(conn).equals(network.getUuid(conn))) {
                return vif;
            }
        }

        return null;
    }

        public String getLowestAvailableVIFDeviceNum(final Connection conn, final VM vm) {
        String vmName = "";
        try {
            vmName = vm.getNameLabel(conn);
            final List<Integer> usedDeviceNums = new ArrayList<Integer>();
            final Set<VIF> vifs = vm.getVIFs(conn);
            final Iterator<VIF> vifIter = vifs.iterator();
            while (vifIter.hasNext()) {
                final VIF vif = vifIter.next();
                try {
                    final String deviceId = vif.getDevice(conn);
                    if (vm.getIsControlDomain(conn) || vif.getCurrentlyAttached(conn)) {
                        usedDeviceNums.add(Integer.valueOf(deviceId));
                    } else {
                        s_logger.debug("Found unplugged VIF " + deviceId + " in VM " + vmName + " destroy it");
                        vif.destroy(conn);
                    }
                } catch (final NumberFormatException e) {
                    final String msg = "Obtained an invalid value for an allocated VIF device number for VM: " + vmName;
                    s_logger.debug(msg, e);
                    throw new CloudRuntimeException(msg);
                }
            }

            for (Integer i = 0; i < _maxNics; i++) {
                if (!usedDeviceNums.contains(i)) {
                    s_logger.debug("Lowest available Vif device number: " + i + " for VM: " + vmName);
                    return i.toString();
                }
            }
        } catch (final XmlRpcException e) {
            final String msg = "Caught XmlRpcException: " + e.getMessage();
            s_logger.warn(msg, e);
        } catch (final XenAPIException e) {
            final String msg = "Caught XenAPIException: " + e.toString();
            s_logger.warn(msg, e);
        }

        throw new CloudRuntimeException("Could not find available VIF slot in VM with name: " + vmName);
    }

    protected XsLocalNetwork getManagementNetwork(final Connection conn) throws XmlRpcException, XenAPIException {
        PIF mgmtPif = null;
        PIF.Record mgmtPifRec = null;
        final Host host = Host.getByUuid(conn, _host.getUuid());
        final Set<PIF> hostPifs = host.getPIFs(conn);
        for (final PIF pif : hostPifs) {
            final PIF.Record rec = pif.getRecord(conn);
            if (rec.management) {
                if (rec.VLAN != null && rec.VLAN != -1) {
                    final String msg = new StringBuilder("Unsupported configuration.  Management network is on a VLAN.  host=").append(_host.getUuid()).append("; pif=")
                            .append(rec.uuid).append("; vlan=").append(rec.VLAN).toString();
                    s_logger.warn(msg);
                    throw new CloudRuntimeException(msg);
                }
                if (s_logger.isDebugEnabled()) {
                    s_logger.debug("Management network is on pif=" + rec.uuid);
                }
                mgmtPif = pif;
                mgmtPifRec = rec;
                break;
            }
        }
        if (mgmtPif == null) {
            final String msg = "Unable to find management network for " + _host.getUuid();
            s_logger.warn(msg);
            throw new CloudRuntimeException(msg);
        }
        final Bond bond = mgmtPifRec.bondSlaveOf;
        if (!XenServerHelper.isRefNull(bond)) {
            final String msg = "Management interface is on slave(" + mgmtPifRec.uuid + ") of bond(" + bond.getUuid(conn) + ") on host(" + _host.getUuid()
                    + "), please move management interface to bond!";
            s_logger.warn(msg);
            throw new CloudRuntimeException(msg);
        }
        final Network nk = mgmtPifRec.network;
        final Network.Record nkRec = nk.getRecord(conn);
        return new XsLocalNetwork(this, nk, nkRec, mgmtPif, mgmtPifRec);
    }

    public Network getNetwork(final Connection conn, final NicTO nic) throws XenAPIException, XmlRpcException {
        final String name = nic.getName();
        final XsLocalNetwork network = getNativeNetworkForTraffic(conn, nic.getType(), name);
        if (network == null) {
            s_logger.error("Network is not configured on the backend for nic " + nic.toString());
            throw new CloudRuntimeException("Network for the backend is not configured correctly for network broadcast domain: " + nic.getBroadcastUri());
        }
        final URI uri = nic.getBroadcastUri();
        final BroadcastDomainType type = nic.getBroadcastType();
        if (uri != null && uri.toString().contains("untagged")) {
            return network.getNetwork();
        } else if (uri != null && type == BroadcastDomainType.Vlan) {
            assert BroadcastDomainType.getSchemeValue(uri) == BroadcastDomainType.Vlan;
            final long vlan = Long.parseLong(BroadcastDomainType.getValue(uri));
            return enableVlanNetwork(conn, vlan, network);
        } else if (type == BroadcastDomainType.Native || type == BroadcastDomainType.LinkLocal || type == BroadcastDomainType.Vsp) {
            return network.getNetwork();
        } else if (uri != null && type == BroadcastDomainType.Vswitch) {
            final String header = uri.toString().substring(Networks.BroadcastDomainType.Vswitch.scheme().length() + "://".length());
            if (header.startsWith("vlan")) {
                _isOvs = true;
                return setupvSwitchNetwork(conn);
            } else {
                return findOrCreateTunnelNetwork(conn, getOvsTunnelNetworkName(uri.getAuthority()));
            }
        } else if (type == BroadcastDomainType.Storage) {
            if (uri == null) {
                return network.getNetwork();
            } else {
                final long vlan = Long.parseLong(BroadcastDomainType.getValue(uri));
                return enableVlanNetwork(conn, vlan, network);
            }
        } else if (type == BroadcastDomainType.Lswitch) {
            // Nicira Logical Switch
            return network.getNetwork();
        } else if (uri != null && type == BroadcastDomainType.Pvlan) {
            assert BroadcastDomainType.getSchemeValue(uri) == BroadcastDomainType.Pvlan;
            // should we consider moving this NetUtils method to
            // BroadcastDomainType?
            final long vlan = Long.parseLong(NetUtils.getPrimaryPvlanFromUri(uri));
            return enableVlanNetwork(conn, vlan, network);
        }

        throw new CloudRuntimeException("Unable to support this type of network broadcast domain: " + nic.getBroadcastUri());
    }

    /**
     * getNetworkByName() retrieves what the server thinks is the actual network
     * used by the XenServer host. This method should always be used to talk to
     * retrieve a network by the name. The reason is because of the problems in
     * using the name label as the way to find the Network.
     *
     * To see how we are working around these problems, take a look at
     * enableVlanNetwork(). The following description assumes you have looked at
     * the description on that method.
     *
     * In order to understand this, we have to see what type of networks are
     * within a XenServer that's under CloudStack control.
     *
     * - Native Networks: these are networks that are untagged on the XenServer
     * and are used to crate VLAN networks on. These are created by the user and
     * is assumed to be one per cluster. - VLAN Networks: these are dynamically
     * created by CloudStack and can have problems with duplicated names. -
     * LinkLocal Networks: these are dynamically created by CloudStack and can
     * also have problems with duplicated names but these don't have actual
     * PIFs.
     *
     * In order to speed to retrieval of a network, we do the following: - We
     * retrieve by the name. If only one network is retrieved, we assume we
     * retrieved the right network. - If more than one network is retrieved, we
     * check to see which one has the pif for the local host and use that. - If
     * a pif is not found, then we look at the tags and find the one with the
     * lowest timestamp. (See enableVlanNetwork())
     *
     * @param conn
     *            Xapi connection
     * @param name
     *            name of the network
     * @return XsNic an object that contains network, network record, pif, and
     *         pif record.
     * @throws XenAPIException
     * @throws XmlRpcException
     *
     * @see XenServerResourceBase#enableVlanNetwork
     */
    public XsLocalNetwork getNetworkByName(final Connection conn, final String name) throws XenAPIException, XmlRpcException {
        final Set<Network> networks = Network.getByNameLabel(conn, name);
        if (networks.size() == 1) {
            return new XsLocalNetwork(this, networks.iterator().next(), null, null, null);
        }

        if (networks.size() == 0) {
            return null;
        }

        if (s_logger.isDebugEnabled()) {
            s_logger.debug("Found more than one network with the name " + name);
        }
        Network earliestNetwork = null;
        Network.Record earliestNetworkRecord = null;
        long earliestTimestamp = Long.MAX_VALUE;
        int earliestRandom = Integer.MAX_VALUE;
        for (final Network network : networks) {
            final XsLocalNetwork nic = new XsLocalNetwork(this, network);

            if (nic.getPif(conn) != null) {
                return nic;
            }

            final Network.Record record = network.getRecord(conn);
            if (record.tags != null) {
                for (final String tag : record.tags) {
                    final Pair<Long, Integer> stamp = parseTimestamp(tag);
                    if (stamp == null) {
                        continue;
                    }

                    if (stamp.first() < earliestTimestamp || stamp.first() == earliestTimestamp && stamp.second() < earliestRandom) {
                        earliestTimestamp = stamp.first();
                        earliestRandom = stamp.second();
                        earliestNetwork = network;
                        earliestNetworkRecord = record;
                    }
                }
            }
        }

        return earliestNetwork != null ? new XsLocalNetwork(this, earliestNetwork, earliestNetworkRecord, null, null) : null;
    }

    public long[] getNetworkStats(final Connection conn, final String privateIP) {
        final String result = networkUsage(conn, privateIP, "get", null);
        final long[] stats = new long[2];
        if (result != null) {
            final String[] splitResult = result.split(":");
            int i = 0;
            while (i < splitResult.length - 1) {
                stats[0] += Long.parseLong(splitResult[i++]);
                stats[1] += Long.parseLong(splitResult[i++]);
            }
        }
        return stats;
    }

        private String getOvsTunnelNetworkName(final String broadcastUri) {
        if (broadcastUri.contains(".")) {
            final String[] parts = broadcastUri.split("\\.");
            return "OVS-DR-VPC-Bridge" + parts[0];
        } else {
            try {
                return "OVSTunnel" + broadcastUri;
            } catch (final Exception e) {
                return null;
            }
        }
    }
    public VIF getVifByMac(final Connection conn, final VM router, String mac) throws XmlRpcException, XenAPIException {
        final Set<VIF> routerVIFs = router.getVIFs(conn);
        mac = mac.trim();
        for (final VIF vif : routerVIFs) {
            final String lmac = vif.getMAC(conn);
            if (lmac.trim().equals(mac)) {
                return vif;
            }
        }
        return null;
    }

        private List<Pair<String, Long>> ovsFullSyncStates() {
        final Connection conn = getConnection();
        final String result = callHostPlugin(conn, "ovsgre", "ovs_get_vm_log", "host_uuid", _host.getUuid());
        final String[] logs = result != null ? result.split(";") : new String[0];
        final List<Pair<String, Long>> states = new ArrayList<Pair<String, Long>>();
        for (final String log : logs) {
            final String[] info = log.split(",");
            if (info.length != 5) {
                s_logger.warn("Wrong element number in ovs log(" + log + ")");
                continue;
            }

            // ','.join([bridge, vmName, vmId, seqno, tag])
            try {
                states.add(new Pair<String, Long>(info[0], Long.parseLong(info[3])));
            } catch (final NumberFormatException nfe) {
                states.add(new Pair<String, Long>(info[0], -1L));
            }
        }
        return states;
    }

    public HashMap<String, String> parseDefaultOvsRuleComamnd(final String str) {
        final HashMap<String, String> cmd = new HashMap<String, String>();
        final String[] sarr = str.split("/");
        for (int i = 0; i < sarr.length; i++) {
            String c = sarr[i];
            c = c.startsWith("/") ? c.substring(1) : c;
            c = c.endsWith("/") ? c.substring(0, c.length() - 1) : c;
            final String[] p = c.split(";");
            if (p.length != 2) {
                continue;
            }
            if (p[0].equalsIgnoreCase("vlans")) {
                p[1] = p[1].replace("@", "[");
                p[1] = p[1].replace("#", "]");
            }
            cmd.put(p[0], p[1]);
        }
        return cmd;
    }
    protected boolean pingdomr(final Connection conn, final String host, final String port) {
        String status;
        status = callHostPlugin(conn, "vmops", "pingdomr", "host", host, "port", port);

        if (status == null || status.isEmpty()) {
            return false;
        }

        return true;

    }
        protected void plugDom0Vif(final Connection conn, final VIF dom0Vif) throws XmlRpcException, XenAPIException {
        if (dom0Vif != null) {
            dom0Vif.plug(conn);
        }
    }
    protected ExecutionResult prepareNetworkElementCommand(final IpAssocCommand cmd) {
        final Connection conn = getConnection();
        final String routerName = cmd.getAccessDetail(NetworkElementCommand.ROUTER_NAME);
        final String routerIp = cmd.getAccessDetail(NetworkElementCommand.ROUTER_IP);

        try {
            final IpAddressTO[] ips = cmd.getIpAddresses();
            for (final IpAddressTO ip : ips) {

                final VM router = getVM(conn, routerName);

                final NicTO nic = new NicTO();
                nic.setMac(ip.getVifMacAddress());
                nic.setType(ip.getTrafficType());
                if (ip.getBroadcastUri() == null) {
                    nic.setBroadcastType(BroadcastDomainType.Native);
                } else {
                    final URI uri = BroadcastDomainType.fromString(ip.getBroadcastUri());
                    nic.setBroadcastType(BroadcastDomainType.getSchemeValue(uri));
                    nic.setBroadcastUri(uri);
                }
                nic.setDeviceId(0);
                nic.setNetworkRateMbps(ip.getNetworkRate());
                nic.setName(ip.getNetworkName());

                final Network network = getNetwork(conn, nic);

                // Determine the correct VIF on DomR to associate/disassociate
                // the
                // IP address with
                VIF correctVif = getCorrectVif(conn, router, network);

                // If we are associating an IP address and DomR doesn't have a
                // VIF
                // for the specified vlan ID, we need to add a VIF
                // If we are disassociating the last IP address in the VLAN, we
                // need
                // to remove a VIF
                boolean addVif = false;
                if (ip.isAdd() && correctVif == null) {
                    addVif = true;
                }

                if (addVif) {
                    // Add a new VIF to DomR
                    final String vifDeviceNum = getLowestAvailableVIFDeviceNum(conn, router);

                    if (vifDeviceNum == null) {
                        throw new InternalErrorException("There were no more available slots for a new VIF on router: " + router.getNameLabel(conn));
                    }

                    nic.setDeviceId(Integer.parseInt(vifDeviceNum));

                    correctVif = createVif(conn, routerName, router, null, nic);
                    correctVif.plug(conn);
                    // Add iptables rule for network usage
                    networkUsage(conn, routerIp, "addVif", "eth" + correctVif.getDevice(conn));
                }

                if (ip.isAdd() && correctVif == null) {
                    throw new InternalErrorException("Failed to find DomR VIF to associate/disassociate IP with.");
                }
                if (correctVif != null) {
                    ip.setNicDevId(Integer.valueOf(correctVif.getDevice(conn)));
                    ip.setNewNic(addVif);
                }
            }
        } catch (final InternalErrorException e) {
            s_logger.error("Ip Assoc failure on applying one ip due to exception:  ", e);
            return new ExecutionResult(false, e.getMessage());
        } catch (final Exception e) {
            return new ExecutionResult(false, e.getMessage());
        }
        return new ExecutionResult(true, null);
    }

    protected ExecutionResult prepareNetworkElementCommand(final IpAssocVpcCommand cmd) {
        final Connection conn = getConnection();
        final String routerName = cmd.getAccessDetail(NetworkElementCommand.ROUTER_NAME);
        try {
            final IpAddressTO[] ips = cmd.getIpAddresses();
            for (final IpAddressTO ip : ips) {

                final VM router = getVM(conn, routerName);

                final VIF correctVif = getVifByMac(conn, router, ip.getVifMacAddress());
                setNicDevIdIfCorrectVifIsNotNull(conn, ip, correctVif);
            }
        } catch (final Exception e) {
            s_logger.error("Ip Assoc failure on applying one ip due to exception:  ", e);
            return new ExecutionResult(false, e.getMessage());
        }

        return new ExecutionResult(true, null);
    }

    protected ExecutionResult prepareNetworkElementCommand(final SetNetworkACLCommand cmd) {
        final Connection conn = getConnection();
        final String routerName = cmd.getAccessDetail(NetworkElementCommand.ROUTER_NAME);

        try {
            final VM router = getVM(conn, routerName);

            final NicTO nic = cmd.getNic();
            if (nic != null) {
                final VIF vif = getVifByMac(conn, router, nic.getMac());
                if (vif == null) {
                    final String msg = "Prepare SetNetworkACL failed due to VIF is null for : " + nic.getMac() + " with routername: " + routerName;
                    s_logger.error(msg);
                    return new ExecutionResult(false, msg);
                }
                nic.setDeviceId(Integer.parseInt(vif.getDevice(conn)));
            } else {
                final String msg = "Prepare SetNetworkACL failed due to nic is null for : " + routerName;
                s_logger.error(msg);
                return new ExecutionResult(false, msg);
            }
        } catch (final Exception e) {
            final String msg = "Prepare SetNetworkACL failed due to " + e.toString();
            s_logger.error(msg, e);
            return new ExecutionResult(false, msg);
        }
        return new ExecutionResult(true, null);
    }

    protected ExecutionResult prepareNetworkElementCommand(final SetSourceNatCommand cmd) {
        final Connection conn = getConnection();
        final String routerName = cmd.getAccessDetail(NetworkElementCommand.ROUTER_NAME);
        final IpAddressTO pubIp = cmd.getIpAddress();
        try {
            final VM router = getVM(conn, routerName);

            final VIF correctVif = getCorrectVif(conn, router, pubIp);

            pubIp.setNicDevId(Integer.valueOf(correctVif.getDevice(conn)));

        } catch (final Exception e) {
            final String msg = "Ip SNAT failure due to " + e.toString();
            s_logger.error(msg, e);
            return new ExecutionResult(false, msg);
        }
        return new ExecutionResult(true, null);
    }

    /**
     * @param cmd
     * @return
     */
    private ExecutionResult prepareNetworkElementCommand(final SetupGuestNetworkCommand cmd) {
        final Connection conn = getConnection();
        final NicTO nic = cmd.getNic();
        final String domrName = cmd.getAccessDetail(NetworkElementCommand.ROUTER_NAME);
        try {
            final Set<VM> vms = VM.getByNameLabel(conn, domrName);
            if (vms == null || vms.isEmpty()) {
                return new ExecutionResult(false, "Can not find VM " + domrName);
            }
            final VM vm = vms.iterator().next();
            final String mac = nic.getMac();
            VIF domrVif = null;
            for (final VIF vif : vm.getVIFs(conn)) {
                final String lmac = vif.getMAC(conn);
                if (lmac.equals(mac)) {
                    domrVif = vif;
                    // Do not break it! We have 2 routers.
                    // break;
                }
            }
            if (domrVif == null) {
                return new ExecutionResult(false, "Can not find vif with mac " + mac + " for VM " + domrName);
            }

            nic.setDeviceId(Integer.parseInt(domrVif.getDevice(conn)));
        } catch (final Exception e) {
            final String msg = "Creating guest network failed due to " + e.toString();
            s_logger.warn(msg, e);
            return new ExecutionResult(false, msg);
        }
        return new ExecutionResult(true, null);
    }

        public void setIsOvs(final boolean isOvs) {
        _isOvs = isOvs;
    }
        protected void setNicDevIdIfCorrectVifIsNotNull(final Connection conn, final IpAddressTO ip, final VIF correctVif) throws InternalErrorException, BadServerResponse,
    XenAPIException, XmlRpcException {
        if (correctVif == null) {
            if (ip.isAdd()) {
                throw new InternalErrorException("Failed to find DomR VIF to associate IP with.");
            } else {
                s_logger.debug("VIF to deassociate IP with does not exist, return success");
            }
        } else {
            ip.setNicDevId(Integer.valueOf(correctVif.getDevice(conn)));
        }
    }
    public void setupLinkLocalNetwork(final Connection conn) {
        try {
            final Network.Record rec = new Network.Record();
            final Set<Network> networks = Network.getByNameLabel(conn, _linkLocalPrivateNetworkName);
            Network linkLocal = null;

            if (networks.size() == 0) {
                rec.nameDescription = "link local network used by system vms";
                rec.nameLabel = _linkLocalPrivateNetworkName;
                final Map<String, String> configs = new HashMap<String, String>();
                configs.put("ip_begin", NetUtils.getLinkLocalGateway());
                configs.put("ip_end", NetUtils.getLinkLocalIpEnd());
                configs.put("netmask", NetUtils.getLinkLocalNetMask());
                configs.put("vswitch-disable-in-band", "true");
                rec.otherConfig = configs;
                linkLocal = Network.create(conn, rec);
            } else {
                linkLocal = networks.iterator().next();
                if (!linkLocal.getOtherConfig(conn).containsKey("vswitch-disable-in-band")) {
                    linkLocal.addToOtherConfig(conn, "vswitch-disable-in-band", "true");
                }
            }

            /* Make sure there is a physical bridge on this network */
            VIF dom0vif = null;
            final Pair<VM, VM.Record> vm = getControlDomain(conn);
            final VM dom0 = vm.first();
            final Set<VIF> vifs = dom0.getVIFs(conn);
            if (vifs.size() != 0) {
                for (final VIF vif : vifs) {
                    final Map<String, String> otherConfig = vif.getOtherConfig(conn);
                    if (otherConfig != null) {
                        final String nameLabel = otherConfig.get("nameLabel");
                        if (nameLabel != null && nameLabel.equalsIgnoreCase("link_local_network_vif")) {
                            dom0vif = vif;
                        }
                    }
                }
            }

            /* create temp VIF0 */
            if (dom0vif == null) {
                s_logger.debug("Can't find a vif on dom0 for link local, creating a new one");
                final VIF.Record vifr = new VIF.Record();
                vifr.VM = dom0;
                vifr.device = getLowestAvailableVIFDeviceNum(conn, dom0);
                if (vifr.device == null) {
                    s_logger.debug("Failed to create link local network, no vif available");
                    return;
                }
                final Map<String, String> config = new HashMap<String, String>();
                config.put("nameLabel", "link_local_network_vif");
                vifr.otherConfig = config;
                vifr.MAC = "FE:FF:FF:FF:FF:FF";
                vifr.network = linkLocal;
                vifr.lockingMode = Types.VifLockingMode.NETWORK_DEFAULT;
                dom0vif = VIF.create(conn, vifr);
                plugDom0Vif(conn, dom0vif);
            } else {
                s_logger.debug("already have a vif on dom0 for link local network");
                if (!dom0vif.getCurrentlyAttached(conn)) {
                    plugDom0Vif(conn, dom0vif);
                }
            }

            final String brName = linkLocal.getBridge(conn);
            callHostPlugin(conn, "vmops", "setLinkLocalIP", "brName", brName);
            _host.setLinkLocalNetwork(linkLocal.getUuid(conn));

        } catch (final XenAPIException e) {
            s_logger.warn("Unable to create local link network", e);
            throw new CloudRuntimeException("Unable to create local link network due to " + e.toString(), e);
        } catch (final XmlRpcException e) {
            s_logger.warn("Unable to create local link network", e);
            throw new CloudRuntimeException("Unable to create local link network due to " + e.toString(), e);
        }
    }
    public synchronized Network setupvSwitchNetwork(final Connection conn) {
        try {
            if (_host.getVswitchNetwork() == null) {
                Network vswitchNw = null;
                final Network.Record rec = new Network.Record();
                final String nwName = Networks.BroadcastScheme.VSwitch.toString();
                final Set<Network> networks = Network.getByNameLabel(conn, nwName);

                if (networks.size() == 0) {
                    rec.nameDescription = "vswitch network for " + nwName;
                    rec.nameLabel = nwName;
                    vswitchNw = Network.create(conn, rec);
                } else {
                    vswitchNw = networks.iterator().next();
                }
                _host.setVswitchNetwork(vswitchNw);
            }
            return _host.getVswitchNetwork();
        } catch (final BadServerResponse e) {
            s_logger.error("Failed to setup vswitch network", e);
        } catch (final XenAPIException e) {
            s_logger.error("Failed to setup vswitch network", e);
        } catch (final XmlRpcException e) {
            s_logger.error("Failed to setup vswitch network", e);
        }

        return null;
    }
    private HashMap<String, Pair<Long, Long>> syncNetworkGroups(final Connection conn, final long id) {
        final HashMap<String, Pair<Long, Long>> states = new HashMap<String, Pair<Long, Long>>();

        final String result = callHostPlugin(conn, "vmops", "get_rule_logs_for_vms", "host_uuid", _host.getUuid());
        s_logger.trace("syncNetworkGroups: id=" + id + " got: " + result);
        final String[] rulelogs = result != null ? result.split(";") : new String[0];
        for (final String rulesforvm : rulelogs) {
            final String[] log = rulesforvm.split(",");
            if (log.length != 6) {
                continue;
            }
            // output = ','.join([vmName, vmID, vmIP, domID, signature, seqno])
            try {
                states.put(log[0], new Pair<Long, Long>(Long.parseLong(log[1]), Long.parseLong(log[5])));
            } catch (final NumberFormatException nfe) {
                states.put(log[0], new Pair<Long, Long>(-1L, -1L));
            }
        }
        return states;
    }

    public boolean transferManagementNetwork(final Connection conn, final Host host, final PIF src, final PIF.Record spr, final PIF dest) throws XmlRpcException, XenAPIException {
        dest.reconfigureIp(conn, spr.ipConfigurationMode, spr.IP, spr.netmask, spr.gateway, spr.DNS);
        Host.managementReconfigure(conn, dest);
        String hostUuid = null;
        int count = 0;
        while (count < 10) {
            try {
                Thread.sleep(10000);
                hostUuid = host.getUuid(conn);
                if (hostUuid != null) {
                    break;
                }
                ++count;
            } catch (final XmlRpcException e) {
                s_logger.debug("Waiting for host to come back: " + e.getMessage());
            } catch (final XenAPIException e) {
                s_logger.debug("Waiting for host to come back: " + e.getMessage());
            } catch (final InterruptedException e) {
                s_logger.debug("Gotta run");
                return false;
            }
        }
        if (hostUuid == null) {
            s_logger.warn("Unable to transfer the management network from " + spr.uuid);
            return false;
        }

        src.reconfigureIp(conn, Types.IpConfigurationMode.NONE, null, null, null, null);
        return true;
    }

}
