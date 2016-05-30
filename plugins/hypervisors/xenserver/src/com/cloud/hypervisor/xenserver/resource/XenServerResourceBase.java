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
package com.cloud.hypervisor.xenserver.resource;

import com.cloud.agent.IAgentControl;
import com.cloud.agent.api.Answer;
import com.cloud.agent.api.Command;
import com.cloud.agent.api.GetHostStatsCommand;
import com.cloud.agent.api.GetVmStatsCommand;
import com.cloud.agent.api.HostStatsEntry;
import com.cloud.agent.api.HostVmStateReportEntry;
import com.cloud.agent.api.PingCommand;
import com.cloud.agent.api.PingRoutingCommand;
import com.cloud.agent.api.PingRoutingWithNwGroupsCommand;
import com.cloud.agent.api.PingRoutingWithOvsCommand;
import com.cloud.agent.api.RebootAnswer;
import com.cloud.agent.api.RebootCommand;
import com.cloud.agent.api.SetupGuestNetworkCommand;
import com.cloud.agent.api.StartAnswer;
import com.cloud.agent.api.StartCommand;
import com.cloud.agent.api.StartupCommand;
import com.cloud.agent.api.StartupRoutingCommand;
import com.cloud.agent.api.StartupStorageCommand;
import com.cloud.agent.api.StopAnswer;
import com.cloud.agent.api.StopCommand;
import com.cloud.agent.api.VgpuTypesInfo;
import com.cloud.agent.api.VmStatsEntry;
import com.cloud.agent.api.routing.IpAssocCommand;
import com.cloud.agent.api.routing.IpAssocVpcCommand;
import com.cloud.agent.api.routing.NetworkElementCommand;
import com.cloud.agent.api.routing.SetNetworkACLCommand;
import com.cloud.agent.api.routing.SetSourceNatCommand;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.agent.api.to.GPUDeviceTO;
import com.cloud.agent.api.to.IpAddressTO;
import com.cloud.agent.api.to.NicTO;
import com.cloud.agent.api.to.VirtualMachineTO;
import com.cloud.agent.resource.virtualnetwork.VirtualRouterDeployer;
import com.cloud.agent.resource.virtualnetwork.VirtualRoutingResource;
import com.cloud.exception.InternalErrorException;
import com.cloud.host.Host.Type;
import com.cloud.hypervisor.Hypervisor.HypervisorType;
import com.cloud.hypervisor.xenserver.resource.common.XenServerConnectionPool;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHelper;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHost;
import com.cloud.hypervisor.xenserver.resource.compute.XenServerComputeResource;
import com.cloud.hypervisor.xenserver.resource.network.XenServerLocalNetwork;
import com.cloud.hypervisor.xenserver.resource.network.XenServerNetworkResource;
import com.cloud.hypervisor.xenserver.resource.storage.XenServerStorageProcessor;
import com.cloud.hypervisor.xenserver.resource.storage.XenServerStorageResource;
import com.cloud.hypervisor.xenserver.resource.wrapper.xenbase.CitrixRequestWrapper;
import com.cloud.hypervisor.xenserver.resource.wrapper.xenbase.XenServerUtilitiesHelper;
import com.cloud.network.Networks;
import com.cloud.network.Networks.BroadcastDomainType;
import com.cloud.network.Networks.TrafficType;
import com.cloud.resource.ServerResource;
import com.cloud.resource.hypervisor.HypervisorResource;
import com.cloud.storage.Volume;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.resource.StorageSubsystemCommandHandler;
import com.cloud.storage.resource.StorageSubsystemCommandHandlerBase;
import com.cloud.template.VirtualMachineTemplate.BootloaderType;
import com.cloud.utils.ExecutionResult;
import com.cloud.utils.NumbersUtil;
import com.cloud.utils.Pair;
import com.cloud.utils.PropertiesUtil;
import com.cloud.utils.StringUtils;
import com.cloud.utils.Ternary;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.net.NetUtils;
import com.cloud.utils.script.Script;
import com.cloud.utils.ssh.SSHCmdHelper;
import com.cloud.utils.ssh.SshHelper;
import com.cloud.vm.VirtualMachine.PowerState;
import com.trilead.ssh2.SCPClient;
import com.xensource.xenapi.Bond;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Console;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.HostCpu;
import com.xensource.xenapi.HostMetrics;
import com.xensource.xenapi.Network;
import com.xensource.xenapi.PIF;
import com.xensource.xenapi.Pool;
import com.xensource.xenapi.SR;
import com.xensource.xenapi.Session;
import com.xensource.xenapi.Task;
import com.xensource.xenapi.Types;
import com.xensource.xenapi.Types.BadServerResponse;
import com.xensource.xenapi.Types.VmPowerState;
import com.xensource.xenapi.Types.XenAPIException;
import com.xensource.xenapi.VBD;
import com.xensource.xenapi.VDI;
import com.xensource.xenapi.VIF;
import com.xensource.xenapi.VLAN;
import com.xensource.xenapi.VM;
import com.xensource.xenapi.XenAPIObject;
import org.apache.cloudstack.storage.to.TemplateObjectTO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.naming.ConfigurationException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * XenServerResourceBase encapsulates the calls to the XenServer Xapi process to
 * perform the required functionalities for CloudStack.
 *
 * ==============> READ THIS <============== Because the XenServer objects can
 * expire when the session expires, we cannot keep any of the actual XenServer
 * objects in this class. The only thing that is constant is the UUID of the
 * XenServer objects but not the objects themselves! This is very important
 * before you do any changes in this code here.
 *
 */
public abstract class XenServerResourceBase implements ServerResource, HypervisorResource, VirtualRouterDeployer {
    /**
     * used to describe what type of resource a storage device is of
     */
    private final static int BASE_TO_CONVERT_BYTES_INTO_KILOBYTES = 1024;

    protected static final XenServerConnectionPool ConnPool = XenServerConnectionPool.getInstance();
    // static min values for guests on xenserver
    private static final long mem_128m = 134217728L;

    static final Random Rand = new Random(System.currentTimeMillis());
    private static final Logger s_logger = Logger.getLogger(XenServerResourceBase.class);
    protected static final HashMap<VmPowerState, PowerState> s_powerStatesTable;

    static {
        s_powerStatesTable = new HashMap<VmPowerState, PowerState>();
        s_powerStatesTable.put(VmPowerState.HALTED, PowerState.PowerOff);
        s_powerStatesTable.put(VmPowerState.PAUSED, PowerState.PowerOff);
        s_powerStatesTable.put(VmPowerState.RUNNING, PowerState.PowerOn);
        s_powerStatesTable.put(VmPowerState.SUSPENDED, PowerState.PowerOff);
        s_powerStatesTable.put(VmPowerState.UNRECOGNIZED, PowerState.PowerUnknown);
    }



    protected IAgentControl _agentControl;
    protected boolean _canBridgeFirewall = false;
    protected String _cluster;
    // Guest and Host Performance Statistics
    protected String _consolidationFunction = "AVERAGE";
    protected long _dcId;
    protected String _guestNetworkName;
    protected int _heartbeatInterval = 60;
    protected int _heartbeatTimeout = 120;
    protected final XenServerHost _host = new XenServerHost();
    protected String _instance; // instance name (default is usually "VM")
    protected boolean _isOvs = false;
    protected String _linkLocalPrivateNetworkName;
    protected int _maxNics = 7;

    final int _maxWeight = 256;
    protected int _migratewait;
    protected String _name;
    protected Queue<String> _password = new LinkedList<String>();

    protected String _pod;
    protected int _pollingIntervalInSeconds = 60;

    protected String _privateNetworkName;
    protected String _publicNetworkName;

    protected final int _retry = 100;

    protected boolean _securityGroupEnabled;
    protected final int _sleep = 10000;
    protected String _storageNetworkName1;
    protected String _storageNetworkName2;

    protected String _username;

    protected VirtualRoutingResource _vrResource;

    public String _attachIsoDeviceNum = "3";

    protected XenServerUtilitiesHelper xenServerUtilitiesHelper = new XenServerUtilitiesHelper();


    protected XenServerStorageResource xenServerStorageResource;
    protected XenServerComputeResource xenServerComputeResource;
    protected XenServerNetworkResource xenServerNetworkResource;

    protected int _wait;
    // Hypervisor specific params with generic value, may need to be overridden
    // for specific versions
    protected long _xsMemoryUsed = 128 * 1024 * 1024L; // xenserver hypervisor used 128 M

    protected double _xsVirtualizationFactor = 63.0 / 64.0; // 1 - virtualization overhead

    protected StorageSubsystemCommandHandler storageHandler;

    public XenServerResourceBase() {
    }

    /**
     * Replaces the old password with the new password used to connect to the host.
     *
     * @param password  - the new host password.
     * @return the old password.
     */
    public String replaceOldPasswdInQueue(final String password) {
        final String oldPasswd = _password.poll();
        _password.add(password);

        return oldPasswd;
    }

    public String getPwdFromQueue() {
        return _password.peek();
    }

    public XenServerUtilitiesHelper getXenServerUtilitiesHelper() {
        return xenServerUtilitiesHelper;
    }

    public XenServerStorageResource getStorageResource() {
        return xenServerStorageResource;
    }

    public XenServerNetworkResource getNetworkResource() {
        return xenServerNetworkResource;
    }

    public XenServerComputeResource getComputeResource() {
        return xenServerComputeResource;
    }

    protected StorageSubsystemCommandHandler buildStorageHandler() {
        final XenServerStorageProcessor processor = new XenServerStorageProcessor(this);
        return new StorageSubsystemCommandHandlerBase(processor);
    }

    public String callHostPlugin(final Connection conn, final String plugin, final String cmd, final String... params) {
        final Map<String, String> args = new HashMap<String, String>();
        String msg;
        try {
            for (int i = 0; i < params.length; i += 2) {
                args.put(params[i], params[i + 1]);
            }

            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin executing for command " + cmd + " with " + getArgsString(args));
            }
            final Host host = Host.getByUuid(conn, _host.getUuid());
            final String result = host.callPlugin(conn, plugin, cmd, args);
            if (s_logger.isTraceEnabled()) {
                s_logger.trace("callHostPlugin Result: " + result);
            }
            return result.replace("\n", "");
        } catch (final XenAPIException e) {
            msg = "callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.toString();
            s_logger.warn(msg);
        } catch (final XmlRpcException e) {
            msg = "callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.getMessage();
            s_logger.debug(msg);
        }
        throw new CloudRuntimeException(msg);
    }

    protected String callHostPluginAsync(final Connection conn, final String plugin, final String cmd, final int wait, final Map<String, String> params) {
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
            final Host host = Host.getByUuid(conn, _host.getUuid());
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
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + _host.getUuid() + ") due to " + e1.toString());
                }
            }
        }
        return null;
    }

    protected String callHostPluginAsync(final Connection conn, final String plugin, final String cmd, final int wait, final String... params) {
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
            final Host host = Host.getByUuid(conn, _host.getUuid());
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
        } catch (final XenAPIException e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.toString(), e);
        } catch (final Exception e) {
            s_logger.warn("callHostPlugin failed for cmd: " + cmd + " with args " + getArgsString(args) + " due to " + e.getMessage(), e);
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e1) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + _host.getUuid() + ") due to " + e1.toString());
                }
            }
        }
        return null;
    }

    public String callHostPluginPremium(final Connection conn, final String cmd, final String... params) {
        return callHostPlugin(conn, "vmopspremium", cmd, params);
    }

    public boolean canBridgeFirewall() {
        return _canBridgeFirewall;
    }

    public boolean canBridgeFirewall(final Connection conn) {
        return Boolean.valueOf(callHostPlugin(conn, "vmops", "can_bridge_firewall", "host_uuid", _host.getUuid(), "instance", _instance));
    }

    public void checkForSuccess(final Connection c, final Task task) throws XenAPIException, XmlRpcException {
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

    private void CheckXenHostInfo() throws ConfigurationException {
        final Connection conn = ConnPool.getConnect(_host.getIp(), _username, _password);
        if (conn == null) {
            throw new ConfigurationException("Can not create connection to " + _host.getIp());
        }
        try {
            Host.Record hostRec = null;
            try {
                final Host host = Host.getByUuid(conn, _host.getUuid());
                hostRec = host.getRecord(conn);
                final Pool.Record poolRec = Pool.getAllRecords(conn).values().iterator().next();
                _host.setPool(poolRec.uuid);

            } catch (final Exception e) {
                throw new ConfigurationException("Can not get host information from " + _host.getIp());
            }
            if (!hostRec.address.equals(_host.getIp())) {
                final String msg = "Host " + _host.getIp() + " seems be reinstalled, please remove this host and readd";
                s_logger.error(msg);
                throw new ConfigurationException(msg);
            }
        } finally {
            try {
                Session.logout(conn);
            } catch (final Exception e) {
            }
        }
    }

    @Override
    public ExecutionResult cleanupCommand(final NetworkElementCommand cmd) {
        if (cmd instanceof IpAssocCommand && !(cmd instanceof IpAssocVpcCommand)) {
            return cleanupNetworkElementCommand((IpAssocCommand) cmd);
        }
        return new ExecutionResult(true, null);
    }

    public boolean cleanupHaltedVms(final Connection conn) throws XenAPIException, XmlRpcException {
        final Host host = Host.getByUuid(conn, _host.getUuid());
        final Map<VM, VM.Record> vms = VM.getAllRecords(conn);
        boolean success = true;
        if (vms != null && !vms.isEmpty()) {
            for (final Map.Entry<VM, VM.Record> entry : vms.entrySet()) {
                final VM vm = entry.getKey();
                final VM.Record vmRec = entry.getValue();
                if (vmRec.isATemplate || vmRec.isControlDomain) {
                    continue;
                }

                if (VmPowerState.HALTED.equals(vmRec.powerState) && vmRec.affinity.equals(host) && !XenServerHelper.isAlienVm(vm, conn)) {
                    try {
                        vm.destroy(conn);
                    } catch (final Exception e) {
                        s_logger.warn("Catch Exception " + e.getClass().getName() + ": unable to destroy VM " + vmRec.nameLabel + " due to ", e);
                        success = false;
                    }
                }
            }
        }
        return success;
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

    public HashMap<String, String> clusterVMMetaDataSync(final Connection conn) {
        final HashMap<String, String> vmMetaDatum = new HashMap<String, String>();
        try {
            final Map<VM, VM.Record> vm_map = VM.getAllRecords(conn); // USE
            // THIS TO
            // GET ALL
            // VMS
            // FROM A
            // CLUSTER
            if (vm_map != null) {
                for (final VM.Record record : vm_map.values()) {
                    if (record.isControlDomain || record.isASnapshot || record.isATemplate) {
                        continue; // Skip DOM0
                    }
                    final String platform = StringUtils.mapToString(record.platform);
                    if (platform.isEmpty()) {
                        continue; //Skip if platform is null
                    }
                    vmMetaDatum.put(record.nameLabel, StringUtils.mapToString(record.platform));
                }
            }
        } catch (final Throwable e) {
            final String msg = "Unable to get vms through host " + _host.getUuid() + " due to to " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg);
        }
        return vmMetaDatum;
    }

    @Override
    public boolean configure(final String name, final Map<String, Object> params) throws ConfigurationException {
        _name = name;

        try {
            _dcId = Long.parseLong((String) params.get("zone"));
        } catch (final NumberFormatException e) {
            throw new ConfigurationException("Unable to get the zone " + params.get("zone"));
        }

        _host.setUuid((String) params.get("guid"));

        _name = _host.getUuid();
        _host.setIp((String) params.get("ipaddress"));

        _username = (String) params.get("username");
        _password.add((String) params.get("password"));
        _pod = (String) params.get("pod");
        _cluster = (String) params.get("cluster");
        _privateNetworkName = (String) params.get("private.network.device");
        _publicNetworkName = (String) params.get("public.network.device");
        _guestNetworkName = (String) params.get("guest.network.device");
        _instance = (String) params.get("instance.name");
        _securityGroupEnabled = Boolean.parseBoolean((String) params.get("securitygroupenabled"));

        _linkLocalPrivateNetworkName = (String) params.get("private.linkLocal.device");
        if (_linkLocalPrivateNetworkName == null) {
            _linkLocalPrivateNetworkName = "cloud_link_local_network";
        }

        _storageNetworkName1 = (String) params.get("storage.network.device1");
        _storageNetworkName2 = (String) params.get("storage.network.device2");

        _heartbeatTimeout = NumbersUtil.parseInt((String) params.get("xenserver.heartbeat.timeout"), 120);
        _heartbeatInterval = NumbersUtil.parseInt((String) params.get("xenserver.heartbeat.interval"), 60);

        String value = (String) params.get("wait");
        _wait = NumbersUtil.parseInt(value, 600);

        value = (String) params.get("migratewait");
        _migratewait = NumbersUtil.parseInt(value, 3600);

        _maxNics = NumbersUtil.parseInt((String) params.get("xenserver.nics.max"), 7);

        if (_pod == null) {
            throw new ConfigurationException("Unable to get the pod");
        }

        if (_host.getIp() == null) {
            throw new ConfigurationException("Unable to get the host address");
        }

        if (_username == null) {
            throw new ConfigurationException("Unable to get the username");
        }

        if (_password.peek() == null) {
            throw new ConfigurationException("Unable to get the password");
        }

        if (_host.getUuid() == null) {
            throw new ConfigurationException("Unable to get the uuid");
        }

        CheckXenHostInfo();

        storageHandler = buildStorageHandler();

        _vrResource = new VirtualRoutingResource(this);
        if (!_vrResource.configure(name, params)) {
            throw new ConfigurationException("Unable to configure VirtualRoutingResource");
        }

        xenServerStorageResource = new XenServerStorageResource(_host, _dcId, _username, _password);
        return true;
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

    public String connect(final Connection conn, final String vmname, final String ipAddress) {
        return connect(conn, vmname, ipAddress, 3922);
    }

    public String connect(final Connection conn, final String vmName, final String ipAddress, final int port) {
        for (int i = 0; i <= _retry; i++) {
            try {
                final Set<VM> vms = VM.getByNameLabel(conn, vmName);
                if (vms.size() < 1) {
                    final String msg = "VM " + vmName + " is not running";
                    s_logger.warn(msg);
                    return msg;
                }
            } catch (final Exception e) {
                final String msg = "VM.getByNameLabel " + vmName + " failed due to " + e.toString();
                s_logger.warn(msg, e);
                return msg;
            }
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Trying to connect to " + ipAddress + " attempt " + i + " of " + _retry);
            }
            if (pingdomr(conn, ipAddress, Integer.toString(port))) {
                return null;
            }
            try {
                Thread.sleep(_sleep);
            } catch (final InterruptedException e) {
            }
        }
        final String msg = "Timeout, Unable to logon to " + ipAddress;
        s_logger.debug(msg);

        return msg;
    }

    @Override
    public ExecutionResult createFileInVR(final String routerIp, final String path, final String filename, final String content) {
        final Connection conn = getConnection();
        final String hostPath = "/tmp/";

        s_logger.debug("Copying VR with ip " + routerIp + " config file into host " + _host.getIp());
        try {
            SshHelper.scpTo(_host.getIp(), 22, _username, null, _password.peek(), hostPath, content.getBytes(Charset.defaultCharset()), filename, null);
        } catch (final Exception e) {
            s_logger.warn("scp VR config file into host " + _host.getIp() + " failed with exception " + e.getMessage().toString());
        }

        final String rc = callHostPlugin(conn, "vmops", "createFileInDomr", "domrip", routerIp, "srcfilepath", hostPath + filename, "dstfilepath", path);
        s_logger.debug("VR Config file " + filename + " got created in VR, ip " + routerIp + " with content \n" + content);

        return new ExecutionResult(rc.startsWith("succ#"), rc.substring(5));
    }

    protected SR createIsoSRbyURI(final Connection conn, final URI uri, final String vmName, final boolean shared) {
        try {
            final Map<String, String> deviceConfig = new HashMap<String, String>();
            String path = uri.getPath();
            path = path.replace("//", "/");
            deviceConfig.put("location", uri.getHost() + ":" + path);
            final Host host = Host.getByUuid(conn, _host.getUuid());
            final SR sr = SR.create(conn, host, deviceConfig, new Long(0), uri.getHost() + path, "iso", "iso", "iso", shared, new HashMap<String, String>());
            sr.setNameLabel(conn, vmName + "-ISO");
            sr.setNameDescription(conn, deviceConfig.get("location"));

            sr.scan(conn);
            return sr;
        } catch (final XenAPIException e) {
            final String msg = "createIsoSRbyURI failed! mountpoint: " + uri.getHost() + uri.getPath() + " due to " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        } catch (final Exception e) {
            final String msg = "createIsoSRbyURI failed! mountpoint: " + uri.getHost() + uri.getPath() + " due to " + e.getMessage();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        }
    }

    public VDI createVdi(final SR sr, final String vdiNameLabel, final Long volumeSize) throws Types.XenAPIException, XmlRpcException {
        final Connection conn = getConnection();

        final VDI.Record vdir = new VDI.Record();

        vdir.nameLabel = vdiNameLabel;
        vdir.SR = sr;
        vdir.type = Types.VdiType.USER;

        final long totalSrSpace = sr.getPhysicalSize(conn);
        final long unavailableSrSpace = sr.getPhysicalUtilisation(conn);
        final long availableSrSpace = totalSrSpace - unavailableSrSpace;

        if (availableSrSpace < volumeSize) {
            throw new CloudRuntimeException("Available space for SR cannot be less than " + volumeSize + ".");
        }

        vdir.virtualSize = volumeSize;

        return VDI.create(conn, vdir);
    }

    public void createVGPU(final Connection conn, final StartCommand cmd, final VM vm, final GPUDeviceTO gpuDevice) throws XenAPIException, XmlRpcException {
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

    public VM createVmFromTemplate(final Connection conn, final VirtualMachineTO vmSpec, final Host host) throws XenAPIException, XmlRpcException {
        final String guestOsTypeName = getGuestOsType(vmSpec.getPlatformEmulator());
        final Set<VM> templates = VM.getByNameLabel(conn, guestOsTypeName);
        if (templates == null || templates.isEmpty()) {
            throw new CloudRuntimeException("Cannot find template " + guestOsTypeName + " on XenServer host");
        }
        assert templates.size() == 1 : "Should only have 1 template but found " + templates.size();
        final VM template = templates.iterator().next();

        final VM.Record vmr = template.getRecord(conn);
        vmr.affinity = host;
        vmr.otherConfig.remove("disks");
        vmr.otherConfig.remove("default_template");
        vmr.otherConfig.remove("mac_seed");
        vmr.isATemplate = false;
        vmr.nameLabel = vmSpec.getName();
        vmr.actionsAfterCrash = Types.OnCrashBehaviour.DESTROY;
        vmr.actionsAfterShutdown = Types.OnNormalExit.DESTROY;
        vmr.otherConfig.put("vm_uuid", vmSpec.getUuid());
        vmr.VCPUsMax = (long) vmSpec.getCpus(); // FIX ME: In case of dynamic
        // scaling this VCPU max should
        // be the minumum of
        // recommended value for that template and capacity remaining on host

        if (isDmcEnabled(conn, host) && vmSpec.isEnableDynamicallyScaleVm()) {
            // scaling is allowed
            vmr.memoryStaticMin = getStaticMin(vmSpec.getOs(), vmSpec.getBootloader() == BootloaderType.CD, vmSpec.getMinRam(), vmSpec.getMaxRam());
            vmr.memoryStaticMax = getStaticMax(vmSpec.getOs(), vmSpec.getBootloader() == BootloaderType.CD, vmSpec.getMinRam(), vmSpec.getMaxRam());
            vmr.memoryDynamicMin = vmSpec.getMinRam();
            vmr.memoryDynamicMax = vmSpec.getMaxRam();
            if (guestOsTypeName.toLowerCase().contains("windows")) {
                vmr.VCPUsMax = (long) vmSpec.getCpus();
            } else {
                if (vmSpec.getVcpuMaxLimit() != null) {
                    vmr.VCPUsMax = (long) vmSpec.getVcpuMaxLimit();
                }
            }
        } else {
            // scaling disallowed, set static memory target
            if (vmSpec.isEnableDynamicallyScaleVm() && !isDmcEnabled(conn, host)) {
                s_logger.warn("Host " + host.getHostname(conn) + " does not support dynamic scaling, so the vm " + vmSpec.getName() + " is not dynamically scalable");
            }
            vmr.memoryStaticMin = vmSpec.getMinRam();
            vmr.memoryStaticMax = vmSpec.getMaxRam();
            vmr.memoryDynamicMin = vmSpec.getMinRam();
            ;
            vmr.memoryDynamicMax = vmSpec.getMaxRam();

            vmr.VCPUsMax = (long) vmSpec.getCpus();
        }

        vmr.VCPUsAtStartup = (long) vmSpec.getCpus();
        vmr.consoles.clear();

        final VM vm = VM.create(conn, vmr);
        if (s_logger.isDebugEnabled()) {
            s_logger.debug("Created VM " + vm.getUuid(conn) + " for " + vmSpec.getName());
        }

        final Map<String, String> vcpuParams = new HashMap<String, String>();

        final Integer speed = vmSpec.getMinSpeed();
        if (speed != null) {

            int cpuWeight = _maxWeight; // cpu_weight
            int utilization = 0; // max CPU cap, default is unlimited

            // weight based allocation, CPU weight is calculated per VCPU
            cpuWeight = (int) (speed * 0.99 / _host.getSpeed() * _maxWeight);
            if (cpuWeight > _maxWeight) {
                cpuWeight = _maxWeight;
            }

            if (vmSpec.getLimitCpuUse()) {
                // CPU cap is per VM, so need to assign cap based on the number
                // of vcpus
                utilization = (int) (vmSpec.getMaxSpeed() * 0.99 * vmSpec.getCpus() / _host.getSpeed() * 100);
            }

            vcpuParams.put("weight", Integer.toString(cpuWeight));
            vcpuParams.put("cap", Integer.toString(utilization));

        }

        if (vcpuParams.size() > 0) {
            vm.setVCPUsParams(conn, vcpuParams);
        }

        final String bootArgs = vmSpec.getBootArgs();
        if (bootArgs != null && bootArgs.length() > 0) {
            String pvargs = vm.getPVArgs(conn);
            pvargs = pvargs + vmSpec.getBootArgs().replaceAll(" ", "%");
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("PV args are " + pvargs);
            }
            vm.setPVArgs(conn, pvargs);
        }

        if (!(guestOsTypeName.startsWith("Windows") || guestOsTypeName.startsWith("Citrix") || guestOsTypeName.startsWith("Other"))) {
            if (vmSpec.getBootloader() == BootloaderType.CD) {
                final DiskTO[] disks = vmSpec.getDisks();
                for (final DiskTO disk : disks) {
                    if (disk.getType() == Volume.Type.ISO) {
                        final TemplateObjectTO iso = (TemplateObjectTO) disk.getData();
                        final String osType = iso.getGuestOsType();
                        if (osType != null) {
                            final String isoGuestOsName = getGuestOsType(vmSpec.getPlatformEmulator());
                            if (!isoGuestOsName.equals(guestOsTypeName)) {
                                vmSpec.setBootloader(BootloaderType.PyGrub);
                            }
                        }
                    }
                }
            }
            if (vmSpec.getBootloader() == BootloaderType.CD) {
                vm.setPVBootloader(conn, "eliloader");
                if (!vm.getOtherConfig(conn).containsKey("install-repository")) {
                    vm.addToOtherConfig(conn, "install-repository", "cdrom");
                }
            } else if (vmSpec.getBootloader() == BootloaderType.PyGrub) {
                vm.setPVBootloader(conn, "pygrub");
                vm.setPVBootloaderArgs(conn, XenServerHelper.getPVbootloaderArgs(guestOsTypeName));
            } else {
                vm.destroy(conn);
                throw new CloudRuntimeException("Unable to handle boot loader type: " + vmSpec.getBootloader());
            }
        }
        try {
            finalizeVmMetaData(vm, conn, vmSpec);
        } catch (final Exception e) {
            throw new CloudRuntimeException("Unable to finalize VM MetaData: " + vmSpec);
        }
        return vm;
    }

    public VM createWorkingVM(final Connection conn, final String vmName, final String guestOSType, final String platformEmulator, final List<VolumeObjectTO> listVolumeTo)
            throws BadServerResponse, Types.VmBadPowerState, Types.SrFull, Types.OperationNotAllowed, XenAPIException, XmlRpcException {
        // below is redundant but keeping for consistency and code readabilty
        final String guestOsTypeName = platformEmulator;
        if (guestOsTypeName == null) {
            final String msg = " Hypervisor " + this.getClass().getName() + " doesn't support guest OS type " + guestOSType
                    + ". you can choose 'Other install media' to run it as HVM";
            s_logger.warn(msg);
            throw new CloudRuntimeException(msg);
        }
        final VM template = getVM(conn, guestOsTypeName);
        final VM vm = template.createClone(conn, vmName);
        vm.setIsATemplate(conn, false);
        final Map<VDI, VolumeObjectTO> vdiMap = new HashMap<VDI, VolumeObjectTO>();
        for (final VolumeObjectTO volume : listVolumeTo) {
            final String vdiUuid = volume.getPath();
            try {
                final VDI vdi = VDI.getByUuid(conn, vdiUuid);
                vdiMap.put(vdi, volume);
            } catch (final Types.UuidInvalid e) {
                s_logger.warn("Unable to find vdi by uuid: " + vdiUuid + ", skip it");
            }
        }
        for (final Map.Entry<VDI, VolumeObjectTO> entry : vdiMap.entrySet()) {
            final VDI vdi = entry.getKey();
            final VolumeObjectTO volumeTO = entry.getValue();
            final VBD.Record vbdr = new VBD.Record();
            vbdr.VM = vm;
            vbdr.VDI = vdi;
            if (volumeTO.getVolumeType() == Volume.Type.ROOT) {
                vbdr.bootable = true;
                vbdr.unpluggable = false;
            } else {
                vbdr.bootable = false;
                vbdr.unpluggable = true;
            }
            vbdr.userdevice = "autodetect";
            vbdr.mode = Types.VbdMode.RW;
            vbdr.type = Types.VbdType.DISK;
            VBD.create(conn, vbdr);
        }
        return vm;
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

    @Override
    public void disconnected() {
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
    protected Network enableVlanNetwork(final Connection conn, final long tag, final XenServerLocalNetwork network) throws XenAPIException, XmlRpcException {
        Network vlanNetwork = null;
        final String oldName = "VLAN" + Long.toString(tag);
        final String newName = "VLAN-" + network.getNetworkRecord(conn).uuid + "-" + tag;
        XenServerLocalNetwork vlanNic = getNetworkByName(conn, newName);
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

    @Override
    public RebootAnswer execute(final RebootCommand cmd) {
        throw new CloudRuntimeException("The method has been replaced but the implementation CitrixRebootCommandWrapper. "
                + "Please use the new design in order to keep compatibility. Once all ServerResource implementation are refactored those methods will dissapper.");
    }

    @Override
    public StartAnswer execute(final StartCommand cmd) {
        throw new CloudRuntimeException("The method has been replaced but the implementation CitrixStartCommandWrapper. "
                + "Please use the new design in order to keep compatibility. Once all ServerResource implementation are refactored those methods will dissapper.");
    }

    @Override
    public StopAnswer execute(final StopCommand cmd) {
        throw new CloudRuntimeException("The method has been replaced but the implementation CitrixStopCommandWrapper. "
                + "Please use the new design in order to keep compatibility. Once all ServerResource implementation are refactored those methods will dissapper.");
    }

    @Override
    public ExecutionResult executeInVR(final String routerIP, final String script, final String args) {
        // Timeout is 120 seconds by default
        return executeInVR(routerIP, script, args, 120);
    }

    @Override
    public ExecutionResult executeInVR(final String routerIP, final String script, final String args, final int timeout) {
        Pair<Boolean, String> result;
        String cmdline = "/opt/cloud/bin/router_proxy.sh " + script + " " + routerIP + " " + args;
        // semicolon need to be escape for bash
        cmdline = cmdline.replaceAll(";", "\\\\;");
        try {
            s_logger.debug("Executing command in VR: " + cmdline);
            result = SshHelper.sshExecute(_host.getIp(), 22, _username, null, _password.peek(), cmdline, 60000, 60000, timeout * 1000);
        } catch (final Exception e) {
            return new ExecutionResult(false, e.getMessage());
        }
        return new ExecutionResult(result.first(), result.second());
    }

    @Override
    public Answer executeRequest(final Command cmd) {
        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        try {
            return wrapper.execute(cmd, this);
        } catch (final Exception e) {
            return Answer.createUnsupportedCommandAnswer(cmd);
        }
    }

    protected void fillHostInfo(final Connection conn, final StartupRoutingCommand cmd) {
        final StringBuilder caps = new StringBuilder();
        try {

            final Host host = Host.getByUuid(conn, _host.getUuid());
            final Host.Record hr = host.getRecord(conn);

            Map<String, String> details = cmd.getHostDetails();
            if (details == null) {
                details = new HashMap<String, String>();
            }

            String productBrand = hr.softwareVersion.get("product_brand");
            if (productBrand == null) {
                productBrand = hr.softwareVersion.get("platform_name");
            }
            details.put("product_brand", productBrand);
            details.put("product_version", _host.getProductVersion());
            if (hr.softwareVersion.get("product_version_text_short") != null) {
                details.put("product_version_text_short", hr.softwareVersion.get("product_version_text_short"));
                cmd.setHypervisorVersion(hr.softwareVersion.get("product_version_text_short"));

                cmd.setHypervisorVersion(_host.getProductVersion());
            }
            if (_privateNetworkName != null) {
                details.put("private.network.device", _privateNetworkName);
            }

            cmd.setHostDetails(details);
            cmd.setName(hr.nameLabel);
            cmd.setGuid(_host.getUuid());
            cmd.setPool(_host.getPool());
            cmd.setDataCenter(Long.toString(_dcId));
            for (final String cap : hr.capabilities) {
                if (cap.length() > 0) {
                    caps.append(cap).append(" , ");
                }
            }
            if (caps.length() > 0) {
                caps.delete(caps.length() - 3, caps.length());
            }
            cmd.setCaps(caps.toString());

            cmd.setSpeed(_host.getSpeed());
            cmd.setCpuSockets(_host.getCpuSockets());
            cmd.setCpus(_host.getCpus());

            final HostMetrics hm = host.getMetrics(conn);

            long ram = 0;
            long dom0Ram = 0;
            ram = hm.getMemoryTotal(conn);
            final Set<VM> vms = host.getResidentVMs(conn);
            for (final VM vm : vms) {
                if (vm.getIsControlDomain(conn)) {
                    dom0Ram = vm.getMemoryStaticMax(conn);
                    break;
                }
            }

            ram = (long) ((ram - dom0Ram - _xsMemoryUsed) * _xsVirtualizationFactor);
            cmd.setMemory(ram);
            cmd.setDom0MinMemory(dom0Ram);

            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Total Ram: " + ram + " dom0 Ram: " + dom0Ram);
            }

            PIF pif = PIF.getByUuid(conn, _host.getPrivatePif());
            PIF.Record pifr = pif.getRecord(conn);
            if (pifr.IP != null && pifr.IP.length() > 0) {
                cmd.setPrivateIpAddress(pifr.IP);
                cmd.setPrivateMacAddress(pifr.MAC);
                cmd.setPrivateNetmask(pifr.netmask);
            } else {
                cmd.setPrivateIpAddress(_host.getIp());
                cmd.setPrivateMacAddress(pifr.MAC);
                cmd.setPrivateNetmask("255.255.255.0");
            }

            pif = PIF.getByUuid(conn, _host.getPublicPif());
            pifr = pif.getRecord(conn);
            if (pifr.IP != null && pifr.IP.length() > 0) {
                cmd.setPublicIpAddress(pifr.IP);
                cmd.setPublicMacAddress(pifr.MAC);
                cmd.setPublicNetmask(pifr.netmask);
            }

            if (_host.getStoragePif1() != null) {
                pif = PIF.getByUuid(conn, _host.getStoragePif1());
                pifr = pif.getRecord(conn);
                if (pifr.IP != null && pifr.IP.length() > 0) {
                    cmd.setStorageIpAddress(pifr.IP);
                    cmd.setStorageMacAddress(pifr.MAC);
                    cmd.setStorageNetmask(pifr.netmask);
                }
            }

            if (_host.getStoragePif2() != null) {
                pif = PIF.getByUuid(conn, _host.getStoragePif2());
                pifr = pif.getRecord(conn);
                if (pifr.IP != null && pifr.IP.length() > 0) {
                    cmd.setStorageIpAddressDeux(pifr.IP);
                    cmd.setStorageMacAddressDeux(pifr.MAC);
                    cmd.setStorageNetmaskDeux(pifr.netmask);
                }
            }

            final Map<String, String> configs = hr.otherConfig;
            cmd.setIqn(configs.get("iscsi_iqn"));

            cmd.setPod(_pod);
            cmd.setVersion(XenServerResourceBase.class.getPackage().getImplementationVersion());

            try {
                final String cmdLine = "xe sm-list | grep \"resigning of duplicates\"";

                final XenServerUtilitiesHelper xenServerUtilitiesHelper = getXenServerUtilitiesHelper();

                Pair<Boolean, String> result = xenServerUtilitiesHelper.executeSshWrapper(_host.getIp(), 22, _username, null, getPwdFromQueue(), cmdLine);

                boolean supportsClonedVolumes = result != null && result.first() != null && result.first() &&
                        result.second() != null && result.second().length() > 0;

                cmd.setSupportsClonedVolumes(supportsClonedVolumes);
            } catch (NumberFormatException ex) {
                s_logger.warn("Issue sending 'xe sm-list' via SSH to XenServer host: " + ex.getMessage());
            }
        } catch (final XmlRpcException e) {
            throw new CloudRuntimeException("XML RPC Exception: " + e.getMessage(), e);
        } catch (final XenAPIException e) {
            throw new CloudRuntimeException("XenAPIException: " + e.toString(), e);
        } catch (final Exception e) {
            throw new CloudRuntimeException("Exception: " + e.toString(), e);
        }
    }

    protected void finalizeVmMetaData(final VM vm, final Connection conn, final VirtualMachineTO vmSpec) throws Exception {

        final Map<String, String> details = vmSpec.getDetails();
        if (details != null) {
            final String platformstring = details.get("platform");
            if (platformstring != null && !platformstring.isEmpty()) {
                final Map<String, String> platform = StringUtils.stringToMap(platformstring);
                vm.setPlatform(conn, platform);
            } else {
                final String timeoffset = details.get("timeoffset");
                if (timeoffset != null) {
                    final Map<String, String> platform = vm.getPlatform(conn);
                    platform.put("timeoffset", timeoffset);
                    vm.setPlatform(conn, platform);
                }
                final String coresPerSocket = details.get("cpu.corespersocket");
                if (coresPerSocket != null) {
                    final Map<String, String> platform = vm.getPlatform(conn);
                    platform.put("cores-per-socket", coresPerSocket);
                    vm.setPlatform(conn, platform);
                }
            }
            if (!BootloaderType.CD.equals(vmSpec.getBootloader())) {
                final String xenservertoolsversion = details.get("hypervisortoolsversion");
                if ((xenservertoolsversion == null || !xenservertoolsversion.equalsIgnoreCase("xenserver61")) && vmSpec.getGpuDevice() == null) {
                    final Map<String, String> platform = vm.getPlatform(conn);
                    platform.remove("device_id");
                    vm.setPlatform(conn, platform);
                }
            }
        }
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

    void forceShutdownVM(final Connection conn, final VM vm) {
        try {
            final Long domId = vm.getDomid(conn);
            callHostPlugin(conn, "vmopspremium", "forceShutdownVM", "domId", domId.toString());
            vm.powerStateReset(conn);
            vm.destroy(conn);
        } catch (final Exception e) {
            final String msg = "forceShutdown failed due to " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg);
        }
    }

    protected String generateTimeStamp() {
        return new StringBuilder("CsCreateTime-").append(System.currentTimeMillis()).append("-").append(Rand.nextInt(Integer.MAX_VALUE)).toString();
    }

    @Override
    public IAgentControl getAgentControl() {
        return _agentControl;
    }

    protected String getArgsString(final Map<String, String> args) {
        final StringBuilder argString = new StringBuilder();
        for (final Map.Entry<String, String> arg : args.entrySet()) {
            argString.append(arg.getKey() + ": " + arg.getValue() + ", ");
        }
        return argString.toString();
    }

    @Override
    public Map<String, Object> getConfigParams() {
        return null;
    }

    public Connection getConnection() {
        return ConnPool.connect(_host.getUuid(), _host.getPool(), _host.getIp(), _username, _password, _wait);
    }

    protected Pair<VM, VM.Record> getControlDomain(final Connection conn) throws XenAPIException, XmlRpcException {
        final Host host = Host.getByUuid(conn, _host.getUuid());
        Set<VM> vms = null;
        vms = host.getResidentVMs(conn);
        for (final VM vm : vms) {
            if (vm.getIsControlDomain(conn)) {
                return new Pair<VM, VM.Record>(vm, vm.getRecord(conn));
            }
        }

        throw new CloudRuntimeException("Com'on no control domain?  What the crap?!#@!##$@");
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

    @Override
    public PingCommand getCurrentStatus(final long id) {
        try {
            if (!pingXAPI()) {
                Thread.sleep(1000);
                if (!pingXAPI()) {
                    s_logger.warn("can not ping xenserver " + _host.getUuid());
                    return null;
                }
            }
            final Connection conn = getConnection();
            if (!_canBridgeFirewall && !_isOvs) {
                return new PingRoutingCommand(getType(), id, getHostVmStateReport(conn));
            } else if (_isOvs) {
                final List<Pair<String, Long>> ovsStates = ovsFullSyncStates();
                return new PingRoutingWithOvsCommand(getType(), id, getHostVmStateReport(conn), ovsStates);
            } else {
                final HashMap<String, Pair<Long, Long>> nwGrpStates = syncNetworkGroups(conn, id);
                return new PingRoutingWithNwGroupsCommand(getType(), id, getHostVmStateReport(conn), nwGrpStates);
            }
        } catch (final Exception e) {
            s_logger.warn("Unable to get current status", e);
            return null;
        }
    }

    protected double getDataAverage(final Node dataNode, final int col, final int numRows) {
        double value = 0;
        final double dummy = 0;
        int numRowsUsed = 0;
        for (int row = 0; row < numRows; row++) {
            final Node data = dataNode.getChildNodes().item(numRows - 1 - row).getChildNodes().item(col + 1);
            final Double currentDataAsDouble = Double.valueOf(getXMLNodeValue(data));
            if (!currentDataAsDouble.equals(Double.NaN)) {
                numRowsUsed += 1;
                value += currentDataAsDouble;
            }
        }

        if (numRowsUsed == 0) {
            if (!Double.isInfinite(value) && !Double.isNaN(value)) {
                return value;
            } else {
                s_logger.warn("Found an invalid value (infinity/NaN) in getDataAverage(), numRows=0");
                return dummy;
            }
        } else {
            if (!Double.isInfinite(value / numRowsUsed) && !Double.isNaN(value / numRowsUsed)) {
                return value / numRowsUsed;
            } else {
                s_logger.warn("Found an invalid value (infinity/NaN) in getDataAverage(), numRows>0");
                return dummy;
            }
        }

    }

    public HashMap<String, HashMap<String, VgpuTypesInfo>> getGPUGroupDetails(final Connection conn) throws XenAPIException, XmlRpcException {
        return null;
    }

    protected String getGuestOsType(String platformEmulator) {
        if (org.apache.commons.lang.StringUtils.isBlank(platformEmulator)) {
            s_logger.debug("no guest OS type, start it as HVM guest");
            platformEmulator = "Other install media";
        }
        return platformEmulator;
    }

    public XenServerHost getHost() {
        return _host;
    }

    public int getMigrateWait() {
        return _migratewait;
    }

    public StorageSubsystemCommandHandler getStorageHandler() {
        return storageHandler;
    }

    protected boolean getHostInfo(final Connection conn) throws IllegalArgumentException {
        try {
            final Host myself = Host.getByUuid(conn, _host.getUuid());
            Set<HostCpu> hcs = null;
            for (int i = 0; i < 10; i++) {
                hcs = myself.getHostCPUs(conn);
                if (hcs != null) {
                    _host.setCpus(hcs.size());
                    if (_host.getCpus() > 0) {
                        break;
                    }
                }
                Thread.sleep(5000);
            }
            if (_host.getCpus() <= 0) {
                throw new CloudRuntimeException("Cannot get the numbers of cpu from XenServer host " + _host.getIp());
            }
            final Map<String, String> cpuInfo = myself.getCpuInfo(conn);
            if (cpuInfo.get("socket_count") != null) {
                _host.setCpuSockets(Integer.parseInt(cpuInfo.get("socket_count")));
            }
            // would hcs be null we would have thrown an exception on condition
            // (_host.getCpus() <= 0) by now
            for (final HostCpu hc : hcs) {
                _host.setSpeed(hc.getSpeed(conn).intValue());
                break;
            }
            final Host.Record hr = myself.getRecord(conn);
            _host.setProductVersion(XenServerHelper.getProductVersion(hr));

            final XenServerLocalNetwork privateNic = getManagementNetwork(conn);
            _privateNetworkName = privateNic.getNetworkRecord(conn).nameLabel;
            _host.setPrivatePif(privateNic.getPifRecord(conn).uuid);
            _host.setPrivateNetwork(privateNic.getNetworkRecord(conn).uuid);
            _host.setSystemvmisouuid(null);

            XenServerLocalNetwork guestNic = null;
            if (_guestNetworkName != null && !_guestNetworkName.equals(_privateNetworkName)) {
                guestNic = getNetworkByName(conn, _guestNetworkName);
                if (guestNic == null) {
                    s_logger.warn("Unable to find guest network " + _guestNetworkName);
                    throw new IllegalArgumentException("Unable to find guest network " + _guestNetworkName + " for host " + _host.getIp());
                }
            } else {
                guestNic = privateNic;
                _guestNetworkName = _privateNetworkName;
            }
            _host.setGuestNetwork(guestNic.getNetworkRecord(conn).uuid);
            _host.setGuestPif(guestNic.getPifRecord(conn).uuid);

            XenServerLocalNetwork publicNic = null;
            if (_publicNetworkName != null && !_publicNetworkName.equals(_guestNetworkName)) {
                publicNic = getNetworkByName(conn, _publicNetworkName);
                if (publicNic == null) {
                    s_logger.warn("Unable to find public network " + _publicNetworkName + " for host " + _host.getIp());
                    throw new IllegalArgumentException("Unable to find public network " + _publicNetworkName + " for host " + _host.getIp());
                }
            } else {
                publicNic = guestNic;
                _publicNetworkName = _guestNetworkName;
            }
            _host.setPublicPif(publicNic.getPifRecord(conn).uuid);
            _host.setPublicNetwork(publicNic.getNetworkRecord(conn).uuid);
            if (_storageNetworkName1 == null) {
                _storageNetworkName1 = _guestNetworkName;
            }
            XenServerLocalNetwork storageNic1 = null;
            storageNic1 = getNetworkByName(conn, _storageNetworkName1);
            if (storageNic1 == null) {
                s_logger.warn("Unable to find storage network " + _storageNetworkName1 + " for host " + _host.getIp());
                throw new IllegalArgumentException("Unable to find storage network " + _storageNetworkName1 + " for host " + _host.getIp());
            } else {
                _host.setStorageNetwork1(storageNic1.getNetworkRecord(conn).uuid);
                _host.setStoragePif1(storageNic1.getPifRecord(conn).uuid);
            }

            XenServerLocalNetwork storageNic2 = null;
            if (_storageNetworkName2 != null) {
                storageNic2 = getNetworkByName(conn, _storageNetworkName2);
                if (storageNic2 != null) {
                    _host.setStoragePif2(storageNic2.getPifRecord(conn).uuid);
                }
            }

            s_logger.info("XenServer Version is " + _host.getProductVersion() + " for host " + _host.getIp());
            s_logger.info("Private Network is " + _privateNetworkName + " for host " + _host.getIp());
            s_logger.info("Guest Network is " + _guestNetworkName + " for host " + _host.getIp());
            s_logger.info("Public Network is " + _publicNetworkName + " for host " + _host.getIp());

            return true;
        } catch (final XenAPIException e) {
            s_logger.warn("Unable to get host information for " + _host.getIp(), e);
            return false;
        } catch (final Exception e) {
            s_logger.warn("Unable to get host information for " + _host.getIp(), e);
            return false;
        }
    }

    public HostStatsEntry getHostStats(final Connection conn, final GetHostStatsCommand cmd, final String hostGuid, final long hostId) {

        final HostStatsEntry hostStats = new HostStatsEntry(hostId, 0, 0, 0, "host", 0, 0, 0, 0);
        final Object[] rrdData = getRRDData(conn, 1); // call rrd method with 1
        // for host

        if (rrdData == null) {
            return null;
        }

        final Integer numRows = (Integer) rrdData[0];
        final Integer numColumns = (Integer) rrdData[1];
        final Node legend = (Node) rrdData[2];
        final Node dataNode = (Node) rrdData[3];

        final NodeList legendChildren = legend.getChildNodes();
        for (int col = 0; col < numColumns; col++) {

            if (legendChildren == null || legendChildren.item(col) == null) {
                continue;
            }

            final String columnMetadata = getXMLNodeValue(legendChildren.item(col));

            if (columnMetadata == null) {
                continue;
            }

            final String[] columnMetadataList = columnMetadata.split(":");

            if (columnMetadataList.length != 4) {
                continue;
            }

            final String type = columnMetadataList[1];
            final String param = columnMetadataList[3];

            if (type.equalsIgnoreCase("host")) {

                if (param.matches("pif_eth0_rx")) {
                    hostStats.setNetworkReadKBs(getDataAverage(dataNode, col, numRows) / 1000);
                } else if (param.matches("pif_eth0_tx")) {
                    hostStats.setNetworkWriteKBs(getDataAverage(dataNode, col, numRows) / 1000);
                } else if (param.contains("memory_total_kib")) {
                    hostStats.setTotalMemoryKBs(getDataAverage(dataNode, col, numRows));
                } else if (param.contains("memory_free_kib")) {
                    hostStats.setFreeMemoryKBs(getDataAverage(dataNode, col, numRows));
                } else if (param.matches("cpu_avg")) {
                    // hostStats.setNumCpus(hostStats.getNumCpus() + 1);
                    hostStats.setCpuUtilization(hostStats.getCpuUtilization() + getDataAverage(dataNode, col, numRows));
                }

                /*
                 * if (param.contains("loadavg")) {
                 * hostStats.setAverageLoad((hostStats.getAverageLoad() +
                 * getDataAverage(dataNode, col, numRows))); }
                 */
            }
        }

        // add the host cpu utilization
        /*
         * if (hostStats.getNumCpus() != 0) {
         * hostStats.setCpuUtilization(hostStats.getCpuUtilization() /
         * hostStats.getNumCpus()); s_logger.debug("Host cpu utilization " +
         * hostStats.getCpuUtilization()); }
         */

        return hostStats;
    }

    protected HashMap<String, HostVmStateReportEntry> getHostVmStateReport(final Connection conn) {

        // TODO : new VM sync model does not require a cluster-scope report, we
        // need to optimize
        // the report accordingly
        final HashMap<String, HostVmStateReportEntry> vmStates = new HashMap<String, HostVmStateReportEntry>();
        Map<VM, VM.Record> vm_map = null;
        for (int i = 0; i < 2; i++) {
            try {
                vm_map = VM.getAllRecords(conn); // USE THIS TO GET ALL VMS FROM
                // A CLUSTER
                break;
            } catch (final Throwable e) {
                s_logger.warn("Unable to get vms", e);
            }
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ex) {

            }
        }

        if (vm_map == null) {
            return vmStates;
        }
        for (final VM.Record record : vm_map.values()) {
            if (record.isControlDomain || record.isASnapshot || record.isATemplate) {
                continue; // Skip DOM0
            }

            final VmPowerState ps = record.powerState;
            final Host host = record.residentOn;
            String host_uuid = null;
            if (!isRefNull(host)) {
                try {
                    host_uuid = host.getUuid(conn);
                } catch (final BadServerResponse e) {
                    s_logger.error("Failed to get host uuid for host " + host.toWireString(), e);
                } catch (final XenAPIException e) {
                    s_logger.error("Failed to get host uuid for host " + host.toWireString(), e);
                } catch (final XmlRpcException e) {
                    s_logger.error("Failed to get host uuid for host " + host.toWireString(), e);
                }

                if (host_uuid.equalsIgnoreCase(_host.getUuid())) {
                    vmStates.put(record.nameLabel, new HostVmStateReportEntry(XenServerHelper.convertToPowerState(ps), host_uuid));
                }
            }
        }

        return vmStates;
    }

    public String getLabel() {
        final Connection conn = getConnection();
        final String result = callHostPlugin(conn, "ovstunnel", "getLabel");
        return result;
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

    protected XenServerLocalNetwork getManagementNetwork(final Connection conn) throws XmlRpcException, XenAPIException {
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
        if (!isRefNull(bond)) {
            final String msg = "Management interface is on slave(" + mgmtPifRec.uuid + ") of bond(" + bond.getUuid(conn) + ") on host(" + _host.getUuid()
                    + "), please move management interface to bond!";
            s_logger.warn(msg);
            throw new CloudRuntimeException(msg);
        }
        final Network nk = mgmtPifRec.network;
        final Network.Record nkRec = nk.getRecord(conn);
        return new XenServerLocalNetwork(this, nk, nkRec, mgmtPif, mgmtPifRec);
    }

    @Override
    public String getName() {
        return _name;
    }

    public XenServerLocalNetwork getNativeNetworkForTraffic(final Connection conn, final TrafficType type, final String name) throws XenAPIException, XmlRpcException {
        if (name != null) {
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Looking for network named " + name);
            }
            return getNetworkByName(conn, name);
        }

        if (type == TrafficType.Guest) {
            return new XenServerLocalNetwork(this, Network.getByUuid(conn, _host.getGuestNetwork()), null, PIF.getByUuid(conn, _host.getGuestPif()), null);
        } else if (type == TrafficType.Control) {
            setupLinkLocalNetwork(conn);
            return new XenServerLocalNetwork(this, Network.getByUuid(conn, _host.getLinkLocalNetwork()));
        } else if (type == TrafficType.Management) {
            return new XenServerLocalNetwork(this, Network.getByUuid(conn, _host.getPrivateNetwork()), null, PIF.getByUuid(conn, _host.getPrivatePif()), null);
        } else if (type == TrafficType.Public) {
            return new XenServerLocalNetwork(this, Network.getByUuid(conn, _host.getPublicNetwork()), null, PIF.getByUuid(conn, _host.getPublicPif()), null);
        } else if (type == TrafficType.Storage) {
            /*
             * TrafficType.Storage is for secondary storage, while
             * storageNetwork1 is for primary storage, we need better name here
             */
            return new XenServerLocalNetwork(this, Network.getByUuid(conn, _host.getStorageNetwork1()), null, PIF.getByUuid(conn, _host.getStoragePif1()), null);
        }

        throw new CloudRuntimeException("Unsupported network type: " + type);
    }

    public Network getNetwork(final Connection conn, final NicTO nic) throws XenAPIException, XmlRpcException {
        final String name = nic.getName();
        final XenServerLocalNetwork network = getNativeNetworkForTraffic(conn, nic.getType(), name);
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
    public XenServerLocalNetwork getNetworkByName(final Connection conn, final String name) throws XenAPIException, XmlRpcException {
        final Set<Network> networks = Network.getByNameLabel(conn, name);
        if (networks.size() == 1) {
            return new XenServerLocalNetwork(this, networks.iterator().next(), null, null, null);
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
            final XenServerLocalNetwork nic = new XenServerLocalNetwork(this, network);

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

        return earliestNetwork != null ? new XenServerLocalNetwork(this, earliestNetwork, earliestNetworkRecord, null, null) : null;
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

    protected List<File> getPatchFiles() {
        String patch = getPatchFilePath();
        String patchfilePath = Script.findScript("", patch);
        if (patchfilePath == null) {
            throw new CloudRuntimeException("Unable to find patch file "+patch);
        }
        List<File> files = new ArrayList<File>();
        files.add(new File(patchfilePath));
        return files;
    }

    protected abstract String getPatchFilePath();

    public String getPerfMon(final Connection conn, final Map<String, String> params, final int wait) {
        String result = null;
        try {
            result = callHostPluginAsync(conn, "vmopspremium", "asmonitor", 60, params);
            if (result != null) {
                return result;
            }
        } catch (final Exception e) {
            s_logger.error("Can not get performance monitor for AS due to ", e);
        }
        return null;
    }

    protected Object[] getRRDData(final Connection conn, final int flag) {

        /*
         * Note: 1 => called from host, hence host stats 2 => called from vm,
         * hence vm stats
         */
        Document doc = null;

        try {
            doc = getStatsRawXML(conn, flag == 1 ? true : false);
        } catch (final Exception e1) {
            s_logger.warn("Error whilst collecting raw stats from plugin: ", e1);
            return null;
        }

        if (doc == null) { // stats are null when the host plugin call fails
            // (host down state)
            return null;
        }

        final NodeList firstLevelChildren = doc.getChildNodes();
        final NodeList secondLevelChildren = firstLevelChildren.item(0).getChildNodes();
        final Node metaNode = secondLevelChildren.item(0);
        final Node dataNode = secondLevelChildren.item(1);

        Integer numRows = 0;
        Integer numColumns = 0;
        Node legend = null;
        final NodeList metaNodeChildren = metaNode.getChildNodes();
        for (int i = 0; i < metaNodeChildren.getLength(); i++) {
            final Node n = metaNodeChildren.item(i);
            if (n.getNodeName().equals("rows")) {
                numRows = Integer.valueOf(getXMLNodeValue(n));
            } else if (n.getNodeName().equals("columns")) {
                numColumns = Integer.valueOf(getXMLNodeValue(n));
            } else if (n.getNodeName().equals("legend")) {
                legend = n;
            }
        }

        return new Object[] { numRows, numColumns, legend, dataNode };
    }

    @Override
    public int getRunLevel() {
        return 0;
    }

    private long getStaticMax(final String os, final boolean b, final long dynamicMinRam, final long dynamicMaxRam) {
        final long recommendedValue = XenServerHelper.getXenServerStaticMax(os, b);
        if (recommendedValue == 0) {
            s_logger.warn("No recommended value found for dynamic max, setting static max and dynamic max equal");
            return dynamicMaxRam;
        }
        final long staticMax = Math.min(recommendedValue, 4l * dynamicMinRam); // XS
        // constraint
        // for
        // stability
        if (dynamicMaxRam > staticMax) { // XS contraint that dynamic max <=
            // static max
            s_logger.warn("dynamixMax " + dynamicMaxRam + " cant be greater than static max " + staticMax
                    + ", can lead to stability issues. Setting static max as much as dynamic max ");
            return dynamicMaxRam;
        }
        return staticMax;
    }

    private long getStaticMin(final String os, final boolean b, final long dynamicMinRam, final long dynamicMaxRam) {
        final long recommendedValue = XenServerHelper.getXenServerStaticMin(os, b);
        if (recommendedValue == 0) {
            s_logger.warn("No recommended value found for dynamic min");
            return dynamicMinRam;
        }

        if (dynamicMinRam < recommendedValue) { // XS contraint that dynamic min
            // > static min
            s_logger.warn("Vm is set to dynamixMin " + dynamicMinRam + " less than the recommended static min " + recommendedValue + ", could lead to stability issues");
        }
        return dynamicMinRam;
    }

    protected Document getStatsRawXML(final Connection conn, final boolean host) {
        final Date currentDate = new Date();
        String urlStr = "http://" + _host.getIp() + "/rrd_updates?";
        urlStr += "session_id=" + conn.getSessionReference();
        urlStr += "&host=" + (host ? "true" : "false");
        urlStr += "&cf=" + _consolidationFunction;
        urlStr += "&interval=" + _pollingIntervalInSeconds;
        urlStr += "&start=" + (currentDate.getTime() / 1000 - 1000 - 100);

        URL url;
        BufferedReader in = null;
        try {
            url = new URL(urlStr);
            url.openConnection();
            final URLConnection uc = url.openConnection();
            in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
            final InputSource statsSource = new InputSource(in);
            return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(statsSource);
        } catch (final MalformedURLException e) {
            s_logger.warn("Malformed URL?  come on...." + urlStr);
            return null;
        } catch (final IOException e) {
            s_logger.warn("Problems getting stats using " + urlStr, e);
            return null;
        } catch (final SAXException e) {
            s_logger.warn("Problems getting stats using " + urlStr, e);
            return null;
        } catch (final ParserConfigurationException e) {
            s_logger.warn("Problems getting stats using " + urlStr, e);
            return null;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (final IOException e) {
                    s_logger.warn("Unable to close the buffer ", e);
                }
            }
        }
    }

    @Override
    public Type getType() {
        return com.cloud.host.Host.Type.Routing;
    }

    protected VDI getVDIbyLocationandSR(final Connection conn, final String loc, final SR sr) {
        try {
            final Set<VDI> vdis = sr.getVDIs(conn);
            for (final VDI vdi : vdis) {
                if (vdi.getLocation(conn).startsWith(loc)) {
                    return vdi;
                }
            }

            final String msg = "can not getVDIbyLocationandSR " + loc;
            s_logger.warn(msg);
            return null;
        } catch (final XenAPIException e) {
            final String msg = "getVDIbyLocationandSR exception " + loc + " due to " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        } catch (final Exception e) {
            final String msg = "getVDIbyLocationandSR exception " + loc + " due to " + e.getMessage();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        }

    }

    public VDI getVDIbyUuid(final Connection conn, final String uuid) {
        return getVDIbyUuid(conn, uuid, true);
    }

    public VDI getVDIbyUuid(final Connection conn, final String uuid, final boolean throwExceptionIfNotFound) {
        try {
            return VDI.getByUuid(conn, uuid);
        } catch (final Exception e) {
            if (throwExceptionIfNotFound) {
                final String msg = "Catch Exception " + e.getClass().getName() + " :VDI getByUuid for uuid: " + uuid + " failed due to " + e.toString();

                s_logger.debug(msg);

                throw new CloudRuntimeException(msg, e);
            }

            return null;
        }
    }

    public String getVhdParent(final Connection conn, final String primaryStorageSRUuid, final String snapshotUuid, final Boolean isISCSI) {
        final String parentUuid = callHostPlugin(conn, "vmopsSnapshot", "getVhdParent", "primaryStorageSRUuid", primaryStorageSRUuid, "snapshotUuid", snapshotUuid, "isISCSI",
                isISCSI.toString());

        if (parentUuid == null || parentUuid.isEmpty() || parentUuid.equalsIgnoreCase("None")) {
            s_logger.debug("Unable to get parent of VHD " + snapshotUuid + " in SR " + primaryStorageSRUuid);
            // errString is already logged.
            return null;
        }
        return parentUuid;
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

    public VirtualRoutingResource getVirtualRoutingResource() {
        return _vrResource;
    }

    public VM getVM(final Connection conn, final String vmName) {
        // Look up VMs with the specified name
        Set<VM> vms;
        try {
            vms = VM.getByNameLabel(conn, vmName);
        } catch (final XenAPIException e) {
            throw new CloudRuntimeException("Unable to get " + vmName + ": " + e.toString(), e);
        } catch (final Exception e) {
            throw new CloudRuntimeException("Unable to get " + vmName + ": " + e.getMessage(), e);
        }

        // If there are no VMs, throw an exception
        if (vms.size() == 0) {
            throw new CloudRuntimeException("VM with name: " + vmName + " does not exist.");
        }

        // If there is more than one VM, print a warning
        if (vms.size() > 1) {
            s_logger.warn("Found " + vms.size() + " VMs with name: " + vmName);
        }

        // Return the first VM in the set
        return vms.iterator().next();
    }

    public String getVMInstanceName() {
        return _instance;
    }

    public long getVMSnapshotChainSize(final Connection conn, final VolumeObjectTO volumeTo, final String vmName) throws BadServerResponse, XenAPIException, XmlRpcException {
        if (volumeTo.getVolumeType() == Volume.Type.DATADISK) {
            final VDI dataDisk = VDI.getByUuid(conn, volumeTo.getPath());
            if (dataDisk != null) {
                final String dataDiskName = dataDisk.getNameLabel(conn);
                if (dataDiskName != null && !dataDiskName.isEmpty()) {
                    volumeTo.setName(dataDiskName);
                }
            }
        }
        final Set<VDI> allvolumeVDIs = VDI.getByNameLabel(conn, volumeTo.getName());
        long size = 0;
        for (final VDI vdi : allvolumeVDIs) {
            try {
                if (vdi.getIsASnapshot(conn) && vdi.getSmConfig(conn).get("vhd-parent") != null) {
                    final String parentUuid = vdi.getSmConfig(conn).get("vhd-parent");
                    final VDI parentVDI = VDI.getByUuid(conn, parentUuid);
                    // add size of snapshot vdi node, usually this only contains
                    // meta data
                    size = size + vdi.getPhysicalUtilisation(conn);
                    // add size of snapshot vdi parent, this contains data
                    if (!isRefNull(parentVDI)) {
                        size = size + parentVDI.getPhysicalUtilisation(conn).longValue();
                    }
                }
            } catch (final Exception e) {
                s_logger.debug("Exception occurs when calculate snapshot capacity for volumes: due to " + e.toString());
                continue;
            }
        }
        if (volumeTo.getVolumeType() == Volume.Type.ROOT) {
            final Map<VM, VM.Record> allVMs = VM.getAllRecords(conn);
            // add size of memory snapshot vdi
            if (allVMs != null && allVMs.size() > 0) {
                for (final VM vmr : allVMs.keySet()) {
                    try {
                        final String vName = vmr.getNameLabel(conn);
                        if (vName != null && vName.contains(vmName) && vmr.getIsASnapshot(conn)) {
                            final VDI memoryVDI = vmr.getSuspendVDI(conn);
                            if (!isRefNull(memoryVDI)) {
                                size = size + memoryVDI.getPhysicalUtilisation(conn);
                                final VDI pMemoryVDI = memoryVDI.getParent(conn);
                                if (!isRefNull(pMemoryVDI)) {
                                    size = size + pMemoryVDI.getPhysicalUtilisation(conn);
                                }
                            }
                        }
                    } catch (final Exception e) {
                        s_logger.debug("Exception occurs when calculate snapshot capacity for memory: due to " + e.toString());
                        continue;
                    }
                }
            }
        }
        return size;
    }

    public PowerState getVmState(final Connection conn, final String vmName) {
        int retry = 3;
        while (retry-- > 0) {
            try {
                final Set<VM> vms = VM.getByNameLabel(conn, vmName);
                for (final VM vm : vms) {
                    return XenServerHelper.convertToPowerState(vm.getPowerState(conn));
                }
            } catch (final BadServerResponse e) {
                // There is a race condition within xenserver such that if a vm
                // is
                // deleted and we
                // happen to ask for it, it throws this stupid response. So
                // if this happens,
                // we take a nap and try again which then avoids the race
                // condition because
                // the vm's information is now cleaned up by xenserver. The
                // error
                // is as follows
                // com.xensource.xenapi.Types$BadServerResponse
                // [HANDLE_INVALID, VM,
                // 3dde93f9-c1df-55a7-2cde-55e1dce431ab]
                s_logger.info("Unable to get a vm PowerState due to " + e.toString() + ". We are retrying.  Count: " + retry);
                try {
                    Thread.sleep(3000);
                } catch (final InterruptedException ex) {

                }
            } catch (final XenAPIException e) {
                final String msg = "Unable to get a vm PowerState due to " + e.toString();
                s_logger.warn(msg, e);
                break;
            } catch (final XmlRpcException e) {
                final String msg = "Unable to get a vm PowerState due to " + e.getMessage();
                s_logger.warn(msg, e);
                break;
            }
        }

        return PowerState.PowerOff;
    }

    public HashMap<String, VmStatsEntry> getVmStats(final Connection conn, final GetVmStatsCommand cmd, final List<String> vmUUIDs, final String hostGuid) {
        final HashMap<String, VmStatsEntry> vmResponseMap = new HashMap<String, VmStatsEntry>();

        for (final String vmUUID : vmUUIDs) {
            vmResponseMap.put(vmUUID, new VmStatsEntry(0,0,0,0, 0, 0, 0, "vm"));
        }

        final Object[] rrdData = getRRDData(conn, 2); // call rrddata with 2 for
        // vm

        if (rrdData == null) {
            return null;
        }

        final Integer numRows = (Integer) rrdData[0];
        final Integer numColumns = (Integer) rrdData[1];
        final Node legend = (Node) rrdData[2];
        final Node dataNode = (Node) rrdData[3];

        final NodeList legendChildren = legend.getChildNodes();
        for (int col = 0; col < numColumns; col++) {

            if (legendChildren == null || legendChildren.item(col) == null) {
                continue;
            }

            final String columnMetadata = getXMLNodeValue(legendChildren.item(col));

            if (columnMetadata == null) {
                continue;
            }

            final String[] columnMetadataList = columnMetadata.split(":");

            if (columnMetadataList.length != 4) {
                continue;
            }

            final String type = columnMetadataList[1];
            final String uuid = columnMetadataList[2];
            final String param = columnMetadataList[3];

            if (type.equals("vm") && vmResponseMap.keySet().contains(uuid)) {
                final VmStatsEntry vmStatsAnswer = vmResponseMap.get(uuid);

                vmStatsAnswer.setEntityType("vm");

                if (param.contains("cpu")) {
                    vmStatsAnswer.setNumCPUs(vmStatsAnswer.getNumCPUs() + 1);
                    vmStatsAnswer.setCPUUtilization(vmStatsAnswer.getCPUUtilization() + getDataAverage(dataNode, col, numRows));
                } else if (param.matches("vif_\\d*_rx")) {
                    vmStatsAnswer.setNetworkReadKBs(vmStatsAnswer.getNetworkReadKBs() + getDataAverage(dataNode, col, numRows) / BASE_TO_CONVERT_BYTES_INTO_KILOBYTES);
                } else if (param.matches("vif_\\d*_tx")) {
                    vmStatsAnswer.setNetworkWriteKBs(vmStatsAnswer.getNetworkWriteKBs() + getDataAverage(dataNode, col, numRows) / BASE_TO_CONVERT_BYTES_INTO_KILOBYTES);
                } else if (param.matches("vbd_.*_read")) {
                    vmStatsAnswer.setDiskReadKBs(vmStatsAnswer.getDiskReadKBs() + getDataAverage(dataNode, col, numRows) / BASE_TO_CONVERT_BYTES_INTO_KILOBYTES);
                } else if (param.matches("vbd_.*_write")) {
                    vmStatsAnswer.setDiskWriteKBs(vmStatsAnswer.getDiskWriteKBs() + getDataAverage(dataNode, col, numRows) / BASE_TO_CONVERT_BYTES_INTO_KILOBYTES);
                } else if (param.contains("memory_internal_free")) {
                    vmStatsAnswer.setIntFreeMemoryKBs(vmStatsAnswer.getIntFreeMemoryKBs() + getDataAverage(dataNode, col, numRows) / BASE_TO_CONVERT_BYTES_INTO_KILOBYTES);
                } else if (param.contains("memory_target")) {
                    vmStatsAnswer.setTargetMemoryKBs(vmStatsAnswer.getTargetMemoryKBs() + getDataAverage(dataNode, col, numRows) / BASE_TO_CONVERT_BYTES_INTO_KILOBYTES);
                } else if (param.contains("memory")) {
                    vmStatsAnswer.setMemoryKBs(vmStatsAnswer.getMemoryKBs() + getDataAverage(dataNode, col, numRows) / BASE_TO_CONVERT_BYTES_INTO_KILOBYTES);
                }

            }
        }

        for (final Map.Entry<String, VmStatsEntry> entry : vmResponseMap.entrySet()) {
            final VmStatsEntry vmStatsAnswer = entry.getValue();

            if (vmStatsAnswer.getNumCPUs() != 0) {
                vmStatsAnswer.setCPUUtilization(vmStatsAnswer.getCPUUtilization() / vmStatsAnswer.getNumCPUs());
            }

            vmStatsAnswer.setCPUUtilization(vmStatsAnswer.getCPUUtilization() * 100);
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Vm cpu utilization " + vmStatsAnswer.getCPUUtilization());
            }
        }

        return vmResponseMap;
    }

    public String getVncUrl(final Connection conn, final VM vm) {
        VM.Record record;
        Console c;
        try {
            record = vm.getRecord(conn);
            final Set<Console> consoles = record.consoles;

            if (consoles.isEmpty()) {
                s_logger.warn("There are no Consoles available to the vm : " + record.nameDescription);
                return null;
            }
            final Iterator<Console> i = consoles.iterator();
            while (i.hasNext()) {
                c = i.next();
                if (c.getProtocol(conn) == Types.ConsoleProtocol.RFB) {
                    return c.getLocation(conn);
                }
            }
        } catch (final XenAPIException e) {
            final String msg = "Unable to get console url due to " + e.toString();
            s_logger.warn(msg, e);
            return null;
        } catch (final XmlRpcException e) {
            final String msg = "Unable to get console url due to " + e.getMessage();
            s_logger.warn(msg, e);
            return null;
        }
        return null;
    }

    protected String getXMLNodeValue(final Node n) {
        return n.getChildNodes().item(0).getNodeValue();
    }

    protected void destroyUnattachedVBD(Connection conn, VM vm) {
        try {
            for (VBD vbd : vm.getVBDs(conn)) {
                if (Types.VbdType.DISK.equals(vbd.getType(conn)) && !vbd.getCurrentlyAttached(conn)) {
                    vbd.destroy(conn);
                }
            }
        } catch (final Exception e) {
            s_logger.debug("Failed to destroy unattached VBD due to ", e);
        }
    }

    public String handleVmStartFailure(final Connection conn, final String vmName, final VM vm, final String message, final Throwable th) {
        final String msg = "Unable to start " + vmName + " due to " + message;
        s_logger.warn(msg, th);

        if (vm == null) {
            return msg;
        }

        try {
            final VM.Record vmr = vm.getRecord(conn);
            final List<Network> networks = new ArrayList<Network>();
            for (final VIF vif : vmr.VIFs) {
                try {
                    final VIF.Record rec = vif.getRecord(conn);
                    if (rec != null) {
                        networks.add(rec.network);
                    } else {
                        s_logger.warn("Unable to cleanup VIF: " + vif.toWireString() + " As vif record is null");
                    }
                } catch (final Exception e) {
                    s_logger.warn("Unable to cleanup VIF", e);
                }
            }
            if (vmr.powerState == VmPowerState.RUNNING) {
                try {
                    vm.hardShutdown(conn);
                } catch (final Exception e) {
                    s_logger.warn("VM hardshutdown failed due to ", e);
                }
            }
            if (vm.getPowerState(conn) == VmPowerState.HALTED) {
                try {
                    vm.destroy(conn);
                } catch (final Exception e) {
                    s_logger.warn("VM destroy failed due to ", e);
                }
            }
            for (final VBD vbd : vmr.VBDs) {
                try {
                    vbd.unplug(conn);
                    vbd.destroy(conn);
                } catch (final Exception e) {
                    s_logger.warn("Unable to clean up VBD due to ", e);
                }
            }
            for (final VIF vif : vmr.VIFs) {
                try {
                    vif.unplug(conn);
                    vif.destroy(conn);
                } catch (final Exception e) {
                    s_logger.warn("Unable to cleanup VIF", e);
                }
            }
            for (final Network network : networks) {
                if (network.getNameLabel(conn).startsWith("VLAN")) {
                    disableVlanNetwork(conn, network);
                }
            }
        } catch (final Exception e) {
            s_logger.warn("VM getRecord failed due to ", e);
        }

        return msg;
    }

    @Override
    public StartupCommand[] initialize() throws IllegalArgumentException {
        final Connection conn = getConnection();
        if (!getHostInfo(conn)) {
            s_logger.warn("Unable to get host information for " + _host.getIp());
            return null;
        }
        final StartupRoutingCommand cmd = new StartupRoutingCommand();
        fillHostInfo(conn, cmd);
        cmd.setHypervisorType(HypervisorType.XenServer);
        cmd.setCluster(_cluster);
        cmd.setPoolSync(false);

        try {
            final Pool pool = Pool.getByUuid(conn, _host.getPool());
            final Pool.Record poolr = pool.getRecord(conn);
            poolr.master.getRecord(conn);
        } catch (final Throwable e) {
            s_logger.warn("Check for master failed, failing the FULL Cluster sync command");
        }
        final StartupStorageCommand sscmd = xenServerStorageResource.initializeLocalSR(conn);
        if (sscmd != null) {
            return new StartupCommand[] { cmd, sscmd };
        }
        return new StartupCommand[] { cmd };
    }

    public boolean isDeviceUsed(final Connection conn, final VM vm, final Long deviceId) {
        // Figure out the disk number to attach the VM to

        String msg = null;
        try {
            final Set<String> allowedVBDDevices = vm.getAllowedVBDDevices(conn);
            if (allowedVBDDevices.contains(deviceId.toString())) {
                return false;
            }
            return true;
        } catch (final XmlRpcException e) {
            msg = "Catch XmlRpcException due to: " + e.getMessage();
            s_logger.warn(msg, e);
        } catch (final XenAPIException e) {
            msg = "Catch XenAPIException due to: " + e.toString();
            s_logger.warn(msg, e);
        }
        throw new CloudRuntimeException("When check deviceId " + msg);
    }

    /**
     * When Dynamic Memory Control (DMC) is enabled - xenserver allows scaling
     * the guest memory while the guest is running
     *
     * By default this is disallowed, override the specific xenserver resource
     * if this is enabled
     */
    public boolean isDmcEnabled(final Connection conn, final Host host) throws XenAPIException, XmlRpcException {
        return false;
    }

    public boolean isNetworkSetupByName(final String nameTag) throws XenAPIException, XmlRpcException {
        if (nameTag != null) {
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Looking for network setup by name " + nameTag);
            }
            final Connection conn = getConnection();
            final XenServerLocalNetwork network = getNetworkByName(conn, nameTag);
            if (network == null) {
                return false;
            }
        }
        return true;
    }

    public boolean isOvs() {
        return _isOvs;
    }

    public boolean isRefNull(final XenAPIObject object) {
        return object == null || object.toWireString().equals("OpaqueRef:NULL") || object.toWireString().equals("<not in database>");
    }

    public boolean isSecurityGroupEnabled() {
        return _securityGroupEnabled;
    }

    public boolean isXcp() {
        final Connection conn = getConnection();
        final String result = callHostPlugin(conn, "ovstunnel", "is_xcp");
        if (result.equals("XCP")) {
            return true;
        }
        return false;
    }

    boolean killCopyProcess(final Connection conn, final String nameLabel) {
        final String results = callHostPluginAsync(conn, "vmops", "kill_copy_process", 60, "namelabel", nameLabel);
        String errMsg = null;
        if (results == null || results.equals("false")) {
            errMsg = "kill_copy_process failed";
            s_logger.warn(errMsg);
            return false;
        } else {
            return true;
        }
    }

    public boolean launchHeartBeat(final Connection conn) {
        final String result = callHostPluginPremium(conn, "heartbeat", "host", _host.getUuid(), "timeout", Integer.toString(_heartbeatTimeout), "interval",
                Integer.toString(_heartbeatInterval));
        if (result == null || !result.contains("> DONE <")) {
            s_logger.warn("Unable to launch the heartbeat process on " + _host.getIp());
            return false;
        }
        return true;
    }

    protected String logX(final XenAPIObject obj, final String msg) {
        return new StringBuilder("Host ").append(_host.getIp()).append(" ").append(obj.toWireString()).append(": ").append(msg).toString();
    }

    public void migrateVM(final Connection conn, final Host destHost, final VM vm, final String vmName) throws Exception {
        Task task = null;
        try {
            final Map<String, String> other = new HashMap<String, String>();
            other.put("live", "true");
            task = vm.poolMigrateAsync(conn, destHost, other);
            try {
                // poll every 1 seconds
                final long timeout = _migratewait * 1000L;
                waitForTask(conn, task, 1000, timeout);
                checkForSuccess(conn, task);
            } catch (final Types.HandleInvalid e) {
                if (vm.getResidentOn(conn).equals(destHost)) {
                    task = null;
                    return;
                }
                throw new CloudRuntimeException("migrate VM catch HandleInvalid and VM is not running on dest host");
            }
        } catch (final XenAPIException e) {
            final String msg = "Unable to migrate VM(" + vmName + ") from host(" + _host.getUuid() + ")";
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg);
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e1) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + _host.getUuid() + ") due to " + e1.toString());
                }
            }
        }
    }


    public String networkUsage(final Connection conn, final String privateIpAddress, final String option, final String vif) {
        if (option.equals("get")) {
            return "0:0";
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

    protected Pair<Long, Integer> parseTimestamp(final String timeStampStr) {
        final String[] tokens = timeStampStr.split("-");
        if (tokens.length != 3) {
            s_logger.debug("timeStamp in network has wrong pattern: " + timeStampStr);
            return null;
        }
        if (!tokens[0].equals("CsCreateTime")) {
            s_logger.debug("timeStamp in network doesn't start with CsCreateTime: " + timeStampStr);
            return null;
        }
        return new Pair<Long, Integer>(Long.parseLong(tokens[1]), Integer.parseInt(tokens[2]));
    }

    protected boolean pingdomr(final Connection conn, final String host, final String port) {
        String status;
        status = callHostPlugin(conn, "vmops", "pingdomr", "host", host, "port", port);

        if (status == null || status.isEmpty()) {
            return false;
        }

        return true;

    }

    public boolean pingXAPI() {
        final Connection conn = getConnection();
        try {
            final Host host = Host.getByUuid(conn, _host.getUuid());
            if (!host.getEnabled(conn)) {
                s_logger.debug("Host " + _host.getIp() + " is not enabled!");
                return false;
            }
        } catch (final Exception e) {
            s_logger.debug("cannot get host enabled status, host " + _host.getIp() + " due to " + e.toString(), e);
            return false;
        }
        try {
            callHostPlugin(conn, "echo", "main");
        } catch (final Exception e) {
            s_logger.debug("cannot ping host " + _host.getIp() + " due to " + e.toString(), e);
            return false;
        }
        return true;
    }

    protected void plugDom0Vif(final Connection conn, final VIF dom0Vif) throws XmlRpcException, XenAPIException {
        if (dom0Vif != null) {
            dom0Vif.plug(conn);
        }
    }

    @Override
    public ExecutionResult prepareCommand(final NetworkElementCommand cmd) {
        // Update IP used to access router
        cmd.setRouterAccessIp(cmd.getAccessDetail(NetworkElementCommand.ROUTER_IP));
        assert cmd.getRouterAccessIp() != null;

        if (cmd instanceof IpAssocVpcCommand) {
            return prepareNetworkElementCommand((IpAssocVpcCommand) cmd);
        } else if (cmd instanceof IpAssocCommand) {
            return prepareNetworkElementCommand((IpAssocCommand) cmd);
        } else if (cmd instanceof SetupGuestNetworkCommand) {
            return prepareNetworkElementCommand((SetupGuestNetworkCommand) cmd);
        } else if (cmd instanceof SetSourceNatCommand) {
            return prepareNetworkElementCommand((SetSourceNatCommand) cmd);
        } else if (cmd instanceof SetNetworkACLCommand) {
            return prepareNetworkElementCommand((SetNetworkACLCommand) cmd);
        }
        return new ExecutionResult(true, null);
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

    public void rebootVM(final Connection conn, final VM vm, final String vmName) throws Exception {
        Task task = null;
        try {
            task = vm.cleanRebootAsync(conn);
            try {
                // poll every 1 seconds , timeout after 10 minutes
                waitForTask(conn, task, 1000, 10 * 60 * 1000);
                checkForSuccess(conn, task);
            } catch (final Types.HandleInvalid e) {
                if (vm.getPowerState(conn) == VmPowerState.RUNNING) {
                    task = null;
                    return;
                }
                throw new CloudRuntimeException("Reboot VM catch HandleInvalid and VM is not in RUNNING state");
            }
        } catch (final XenAPIException e) {
            s_logger.debug("Unable to Clean Reboot VM(" + vmName + ") on host(" + _host.getUuid() + ") due to " + e.toString() + ", try hard reboot");
            try {
                vm.hardReboot(conn);
            } catch (final Exception e1) {
                final String msg = "Unable to hard Reboot VM(" + vmName + ") on host(" + _host.getUuid() + ") due to " + e.toString();
                s_logger.warn(msg, e1);
                throw new CloudRuntimeException(msg);
            }
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e1) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + _host.getUuid() + ") due to " + e1.toString());
                }
            }
        }
    }

    public void scaleVM(final Connection conn, final VM vm, final VirtualMachineTO vmSpec, final Host host) throws XenAPIException, XmlRpcException {

        final Long staticMemoryMax = vm.getMemoryStaticMax(conn);
        final Long staticMemoryMin = vm.getMemoryStaticMin(conn);
        final Long newDynamicMemoryMin = vmSpec.getMinRam();
        final Long newDynamicMemoryMax = vmSpec.getMaxRam();
        if (staticMemoryMin > newDynamicMemoryMin || newDynamicMemoryMax > staticMemoryMax) {
            throw new CloudRuntimeException("Cannot scale up the vm because of memory constraint violation: " + "0 <= memory-static-min(" + staticMemoryMin
                    + ") <= memory-dynamic-min(" + newDynamicMemoryMin + ") <= memory-dynamic-max(" + newDynamicMemoryMax + ") <= memory-static-max(" + staticMemoryMax + ")");
        }

        vm.setMemoryDynamicRange(conn, newDynamicMemoryMin, newDynamicMemoryMax);
        vm.setVCPUsNumberLive(conn, (long) vmSpec.getCpus());

        final Integer speed = vmSpec.getMinSpeed();
        if (speed != null) {

            int cpuWeight = _maxWeight; // cpu_weight

            // weight based allocation

            cpuWeight = (int) (speed * 0.99 / _host.getSpeed() * _maxWeight);
            if (cpuWeight > _maxWeight) {
                cpuWeight = _maxWeight;
            }

            if (vmSpec.getLimitCpuUse()) {
                long utilization = 0; // max CPU cap, default is unlimited
                utilization = (int) (vmSpec.getMaxSpeed() * 0.99 * vmSpec.getCpus() / _host.getSpeed() * 100);
                // vm.addToVCPUsParamsLive(conn, "cap",
                // Long.toString(utilization)); currently xenserver doesnot
                // support Xapi to add VCPUs params live.
                callHostPlugin(conn, "vmops", "add_to_VCPUs_params_live", "key", "cap", "value", Long.toString(utilization), "vmname", vmSpec.getName());
            }
            // vm.addToVCPUsParamsLive(conn, "weight",
            // Integer.toString(cpuWeight));
            callHostPlugin(conn, "vmops", "add_to_VCPUs_params_live", "key", "weight", "value", Integer.toString(cpuWeight), "vmname", vmSpec.getName());
        }
    }

    @Override
    public void setAgentControl(final IAgentControl agentControl) {
        _agentControl = agentControl;
    }

    public void setCanBridgeFirewall(final boolean canBridgeFirewall) {
        _canBridgeFirewall = canBridgeFirewall;
    }

    @Override
    public void setConfigParams(final Map<String, Object> params) {
    }

    public boolean setIptables(final Connection conn) {
        final String result = callHostPlugin(conn, "vmops", "setIptables");
        if (result == null || result.isEmpty()) {
            return false;
        }
        return true;
    }

    public void setIsOvs(final boolean isOvs) {
        _isOvs = isOvs;
    }

    /**
     * WARN: static-min <= dynamic-min <= dynamic-max <= static-max
     *
     * @see XcpServerResource#setMemory(com.xensource.xenapi.Connection,
     *      com.xensource.xenapi.VM, long, long)
     * @param conn
     * @param vm
     * @param minMemsize
     * @param maxMemsize
     * @throws XmlRpcException
     * @throws XenAPIException
     */
    protected void setMemory(final Connection conn, final VM vm, final long minMemsize, final long maxMemsize) throws XmlRpcException, XenAPIException {
        vm.setMemoryLimits(conn, mem_128m, maxMemsize, minMemsize, maxMemsize);
    }

    @Override
    public void setName(final String name) {
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

    @Override
    public void setRunLevel(final int level) {
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

    /* return : if setup is needed */
    public boolean setupServer(final Connection conn, final Host host) {
        final String packageVersion = XenServerResourceBase.class.getPackage().getImplementationVersion();
        final String version = this.getClass().getName() + "-" + (packageVersion == null ? Long.toString(System.currentTimeMillis()) : packageVersion);

        try {
            /* push patches to XenServer */
            final Host.Record hr = host.getRecord(conn);

            final Iterator<String> it = hr.tags.iterator();

            while (it.hasNext()) {
                final String tag = it.next();
                if (tag.startsWith("vmops-version-")) {
                    if (tag.contains(version)) {
                        s_logger.info(logX(host, "Host " + hr.address + " is already setup."));
                        return false;
                    } else {
                        it.remove();
                    }
                }
            }

            final com.trilead.ssh2.Connection sshConnection = new com.trilead.ssh2.Connection(hr.address, 22);
            try {
                sshConnection.connect(null, 60000, 60000);
                if (!sshConnection.authenticateWithPassword(_username, _password.peek())) {
                    throw new CloudRuntimeException("Unable to authenticate");
                }

                final String cmd = "mkdir -p /opt/cloud/bin /var/log/cloud";
                if (!SSHCmdHelper.sshExecuteCmd(sshConnection, cmd)) {
                    throw new CloudRuntimeException("Cannot create directory /opt/cloud/bin on XenServer hosts");
                }

                final SCPClient scp = new SCPClient(sshConnection);

                final List<File> files = getPatchFiles();
                if (files == null || files.isEmpty()) {
                    throw new CloudRuntimeException("Can not find patch file");
                }
                for (final File file : files) {
                    final String path = file.getParentFile().getAbsolutePath() + "/";
                    final Properties props = PropertiesUtil.loadFromFile(file);

                    for (final Map.Entry<Object, Object> entry : props.entrySet()) {
                        final String k = (String) entry.getKey();
                        final String v = (String) entry.getValue();

                        assert k != null && k.length() > 0 && v != null && v.length() > 0 : "Problems with " + k + "=" + v;

                        final String[] tokens = v.split(",");
                        String f = null;
                        if (tokens.length == 3 && tokens[0].length() > 0) {
                            if (tokens[0].startsWith("/")) {
                                f = tokens[0];
                            } else if (tokens[0].startsWith("~")) {
                                final String homedir = System.getenv("HOME");
                                f = homedir + tokens[0].substring(1) + k;
                            } else {
                                f = path + tokens[0] + '/' + k;
                            }
                        } else {
                            f = path + k;
                        }
                        final String directoryPath = tokens[tokens.length - 1];

                        f = f.replace('/', File.separatorChar);

                        String permissions = "0755";
                        if (tokens.length == 3) {
                            permissions = tokens[1];
                        } else if (tokens.length == 2) {
                            permissions = tokens[0];
                        }

                        if (!new File(f).exists()) {
                            s_logger.warn("We cannot locate " + f);
                            continue;
                        }
                        if (s_logger.isDebugEnabled()) {
                            s_logger.debug("Copying " + f + " to " + directoryPath + " on " + hr.address + " with permission " + permissions);
                        }

                        if (!SSHCmdHelper.sshExecuteCmd(sshConnection, "mkdir -m 700 -p " + directoryPath)) {
                            s_logger.debug("Unable to create destination path: " + directoryPath + " on " + hr.address + ".");
                        }

                        try {
                            scp.put(f, directoryPath, permissions);
                        } catch (final IOException e) {
                            final String msg = "Unable to copy file " + f + " to path " + directoryPath + " with permissions  " + permissions;
                            s_logger.debug(msg);
                            throw new CloudRuntimeException("Unable to setup the server: " + msg, e);
                        }
                    }
                }

            } catch (final IOException e) {
                throw new CloudRuntimeException("Unable to setup the server correctly", e);
            } finally {
                sshConnection.close();
            }
            hr.tags.add("vmops-version-" + version);
            host.setTags(conn, hr.tags);
            return true;
        } catch (final XenAPIException e) {
            final String msg = "XenServer setup failed due to " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException("Unable to get host information " + e.toString(), e);
        } catch (final XmlRpcException e) {
            final String msg = "XenServer setup failed due to " + e.getMessage();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException("Unable to get host information ", e);
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

    public void shutdownVM(final Connection conn, final VM vm, final String vmName) throws XmlRpcException {
        Task task = null;
        try {
            task = vm.cleanShutdownAsync(conn);
            try {
                // poll every 1 seconds , timeout after 10 minutes
                waitForTask(conn, task, 1000, 10 * 60 * 1000);
                checkForSuccess(conn, task);
            } catch (final TimeoutException e) {
                if (vm.getPowerState(conn) == VmPowerState.HALTED) {
                    task = null;
                    return;
                }
                throw new CloudRuntimeException("Shutdown VM catch HandleInvalid and VM is not in HALTED state");
            }
        } catch (final XenAPIException e) {
            s_logger.debug("Unable to cleanShutdown VM(" + vmName + ") on host(" + _host.getUuid() + ") due to " + e.toString());
            try {
                VmPowerState state = vm.getPowerState(conn);
                if (state == VmPowerState.RUNNING) {
                    try {
                        vm.hardShutdown(conn);
                    } catch (final Exception e1) {
                        s_logger.debug("Unable to hardShutdown VM(" + vmName + ") on host(" + _host.getUuid() + ") due to " + e.toString());
                        state = vm.getPowerState(conn);
                        if (state == VmPowerState.RUNNING) {
                            forceShutdownVM(conn, vm);
                        }
                        return;
                    }
                } else if (state == VmPowerState.HALTED) {
                    return;
                } else {
                    final String msg = "After cleanShutdown the VM status is " + state.toString() + ", that is not expected";
                    s_logger.warn(msg);
                    throw new CloudRuntimeException(msg);
                }
            } catch (final Exception e1) {
                final String msg = "Unable to hardShutdown VM(" + vmName + ") on host(" + _host.getUuid() + ") due to " + e.toString();
                s_logger.warn(msg, e1);
                throw new CloudRuntimeException(msg);
            }
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e1) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + _host.getUuid() + ") due to " + e1.toString());
                }
            }
        }
    }

    @Override
    public boolean start() {
        return true;
    }

    public void startVM(final Connection conn, final Host host, final VM vm, final String vmName) throws Exception {
        Task task = null;
        try {
            task = vm.startOnAsync(conn, host, false, true);
            try {
                // poll every 1 seconds , timeout after 10 minutes
                waitForTask(conn, task, 1000, 10 * 60 * 1000);
                checkForSuccess(conn, task);
            } catch (final Types.HandleInvalid e) {
                if (vm.getPowerState(conn) == VmPowerState.RUNNING) {
                    s_logger.debug("VM " + vmName + " is in Running status");
                    task = null;
                    return;
                }
                throw new CloudRuntimeException("Start VM " + vmName + " catch HandleInvalid and VM is not in RUNNING state");
            } catch (final TimeoutException e) {
                if (vm.getPowerState(conn) == VmPowerState.RUNNING) {
                    s_logger.debug("VM " + vmName + " is in Running status");
                    task = null;
                    return;
                }
                throw new CloudRuntimeException("Start VM " + vmName + " catch BadAsyncResult and VM is not in RUNNING state");
            }
        } catch (final XenAPIException e) {
            final String msg = "Unable to start VM(" + vmName + ") on host(" + _host.getUuid() + ") due to " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg);
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e1) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + _host.getUuid() + ") due to " + e1.toString());
                }
            }
        }
    }

    protected void startvmfailhandle(final Connection conn, final VM vm, final List<Ternary<SR, VDI, VolumeVO>> mounts) {
        if (vm != null) {
            try {

                if (vm.getPowerState(conn) == VmPowerState.RUNNING) {
                    try {
                        vm.hardShutdown(conn);
                    } catch (final Exception e) {
                        final String msg = "VM hardshutdown failed due to " + e.toString();
                        s_logger.warn(msg, e);
                    }
                }
                if (vm.getPowerState(conn) == VmPowerState.HALTED) {
                    try {
                        vm.destroy(conn);
                    } catch (final Exception e) {
                        final String msg = "VM destroy failed due to " + e.toString();
                        s_logger.warn(msg, e);
                    }
                }
            } catch (final Exception e) {
                final String msg = "VM getPowerState failed due to " + e.toString();
                s_logger.warn(msg, e);
            }
        }
        if (mounts != null) {
            for (final Ternary<SR, VDI, VolumeVO> mount : mounts) {
                final VDI vdi = mount.second();
                Set<VBD> vbds = null;
                try {
                    vbds = vdi.getVBDs(conn);
                } catch (final Exception e) {
                    final String msg = "VDI getVBDS failed due to " + e.toString();
                    s_logger.warn(msg, e);
                    continue;
                }
                for (final VBD vbd : vbds) {
                    try {
                        vbd.unplug(conn);
                        vbd.destroy(conn);
                    } catch (final Exception e) {
                        final String msg = "VBD destroy failed due to " + e.toString();
                        s_logger.warn(msg, e);
                    }
                }
            }
        }
    }

    @Override
    public boolean stop() {
        disconnected();
        return true;
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


    public void waitForTask(final Connection c, final Task task, final long pollInterval, final long timeout) throws XenAPIException, XmlRpcException, TimeoutException {
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
}
