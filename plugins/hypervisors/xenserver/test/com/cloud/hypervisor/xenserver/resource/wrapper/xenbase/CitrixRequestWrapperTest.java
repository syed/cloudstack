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
package com.cloud.hypervisor.xenserver.resource.wrapper.xenbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.cloud.hypervisor.xenserver.resource.XenServerResourceBase;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHelper;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHost;
import org.apache.cloudstack.storage.command.AttachAnswer;
import org.apache.cloudstack.storage.command.AttachCommand;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.AttachIsoCommand;
import com.cloud.agent.api.CheckHealthCommand;
import com.cloud.agent.api.CheckNetworkCommand;
import com.cloud.agent.api.CheckOnHostCommand;
import com.cloud.agent.api.CheckVirtualMachineCommand;
import com.cloud.agent.api.CleanupNetworkRulesCmd;
import com.cloud.agent.api.ClusterVMMetaDataSyncCommand;
import com.cloud.agent.api.Command;
import com.cloud.agent.api.CreateStoragePoolCommand;
import com.cloud.agent.api.CreateVMSnapshotCommand;
import com.cloud.agent.api.DeleteStoragePoolCommand;
import com.cloud.agent.api.DeleteVMSnapshotCommand;
import com.cloud.agent.api.GetHostStatsCommand;
import com.cloud.agent.api.GetStorageStatsCommand;
import com.cloud.agent.api.GetVmDiskStatsCommand;
import com.cloud.agent.api.GetVmIpAddressCommand;
import com.cloud.agent.api.GetVmStatsCommand;
import com.cloud.agent.api.GetVncPortCommand;
import com.cloud.agent.api.MaintainCommand;
import com.cloud.agent.api.MigrateCommand;
import com.cloud.agent.api.ModifySshKeysCommand;
import com.cloud.agent.api.ModifyStoragePoolCommand;
import com.cloud.agent.api.NetworkRulesSystemVmCommand;
import com.cloud.agent.api.NetworkRulesVmSecondaryIpCommand;
import com.cloud.agent.api.OvsCreateGreTunnelCommand;
import com.cloud.agent.api.OvsCreateTunnelCommand;
import com.cloud.agent.api.OvsDeleteFlowCommand;
import com.cloud.agent.api.OvsDestroyBridgeCommand;
import com.cloud.agent.api.OvsDestroyTunnelCommand;
import com.cloud.agent.api.OvsFetchInterfaceCommand;
import com.cloud.agent.api.OvsSetTagAndFlowCommand;
import com.cloud.agent.api.OvsSetupBridgeCommand;
import com.cloud.agent.api.OvsVpcPhysicalTopologyConfigCommand;
import com.cloud.agent.api.OvsVpcRoutingPolicyConfigCommand;
import com.cloud.agent.api.PerformanceMonitorCommand;
import com.cloud.agent.api.PingTestCommand;
import com.cloud.agent.api.PlugNicCommand;
import com.cloud.agent.api.PrepareForMigrationCommand;
import com.cloud.agent.api.PvlanSetupCommand;
import com.cloud.agent.api.ReadyCommand;
import com.cloud.agent.api.RebootAnswer;
import com.cloud.agent.api.RebootCommand;
import com.cloud.agent.api.RebootRouterCommand;
import com.cloud.agent.api.RevertToVMSnapshotCommand;
import com.cloud.agent.api.ScaleVmCommand;
import com.cloud.agent.api.SecurityGroupRulesCmd;
import com.cloud.agent.api.SetupCommand;
import com.cloud.agent.api.StartCommand;
import com.cloud.agent.api.StopCommand;
import com.cloud.agent.api.UnPlugNicCommand;
import com.cloud.agent.api.UpdateHostPasswordCommand;
import com.cloud.agent.api.UpgradeSnapshotCommand;
import com.cloud.agent.api.VMSnapshotTO;
import com.cloud.agent.api.SecurityGroupRulesCmd.IpPortAndProto;
import com.cloud.agent.api.check.CheckSshCommand;
import com.cloud.agent.api.proxy.CheckConsoleProxyLoadCommand;
import com.cloud.agent.api.proxy.WatchConsoleProxyLoadCommand;
import com.cloud.agent.api.routing.IpAssocCommand;
import com.cloud.agent.api.routing.IpAssocVpcCommand;
import com.cloud.agent.api.storage.CreateAnswer;
import com.cloud.agent.api.storage.CreateCommand;
import com.cloud.agent.api.storage.DestroyCommand;
import com.cloud.agent.api.storage.PrimaryStorageDownloadCommand;
import com.cloud.agent.api.storage.ResizeVolumeCommand;
import com.cloud.agent.api.to.DataStoreTO;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.agent.api.to.IpAddressTO;
import com.cloud.agent.api.to.NicTO;
import com.cloud.agent.api.to.StorageFilerTO;
import com.cloud.agent.api.to.VirtualMachineTO;
import com.cloud.agent.resource.virtualnetwork.VRScripts;
import com.cloud.agent.resource.virtualnetwork.VirtualRoutingResource;
import com.cloud.host.HostEnvironment;
import com.cloud.hypervisor.xenserver.resource.network.XsLocalNetwork;
import com.cloud.network.Networks.TrafficType;
import com.cloud.network.PhysicalNetworkSetupInfo;
import com.cloud.storage.Storage.ImageFormat;
import com.cloud.storage.VMTemplateStorageResourceAssoc;
import com.cloud.storage.resource.StorageSubsystemCommandHandler;
import com.cloud.utils.Pair;
import com.cloud.vm.DiskProfile;
import com.cloud.vm.VirtualMachine;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.Marshalling;
import com.xensource.xenapi.Network;
import com.xensource.xenapi.PIF;
import com.xensource.xenapi.Pool;
import com.xensource.xenapi.Types.BadServerResponse;
import com.xensource.xenapi.Types.XenAPIException;
import com.xensource.xenapi.VM;
import com.xensource.xenapi.VMGuestMetrics;

@RunWith(MockitoJUnitRunner.class)
public class CitrixRequestWrapperTest {

    @Mock
    private XenServerResourceBase xenServerResourceBase;
    @Mock
    private RebootAnswer rebootAnswer;
    @Mock
    private CreateAnswer createAnswer;

    @Test
    public void testWrapperInstance() {
        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);
    }

    @Test
    public void testUnknownCommand() {
        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        try {
            wrapper.execute(new NotAValidCommand(), xenServerResourceBase);
        } catch (final Exception e) {
            assertTrue(e instanceof NullPointerException);
        }
    }

    @Test
    public void testExecuteRebootRouterCommand() {
        final RebootRouterCommand rebootRouterCommand = new RebootRouterCommand("Test", "127.0.0.1");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(rebootRouterCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(2)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testExecuteCreateCommand() {
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);
        final DiskProfile diskProfile = Mockito.mock(DiskProfile.class);
        final CreateCommand createCommand = new CreateCommand(diskProfile, "", poolVO, false);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(createCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testCheckConsoleProxyLoadCommand() {
        final CheckConsoleProxyLoadCommand consoleProxyCommand = new CheckConsoleProxyLoadCommand();

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(consoleProxyCommand, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testWatchConsoleProxyLoadCommand() {
        final WatchConsoleProxyLoadCommand watchConsoleProxyCommand = new WatchConsoleProxyLoadCommand(0, 0, "", "", 0);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(watchConsoleProxyCommand, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testReadyCommand() {
        final ReadyCommand readyCommand = new ReadyCommand();

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(readyCommand, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testGetHostStatsCommand() {
        final GetHostStatsCommand statsCommand = new GetHostStatsCommand(null, null, 0);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(statsCommand, xenServerResourceBase);

        assertTrue(answer.getResult());
    }

    @Test
    public void testGetVmStatsCommand() {
        final GetVmStatsCommand statsCommand = new GetVmStatsCommand(new ArrayList<String>(), null, null);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(statsCommand, xenServerResourceBase);

        assertTrue(answer.getResult());
    }

    @Test
    public void testGetVmDiskStatsCommand() {
        final GetVmDiskStatsCommand diskStatsCommand = new GetVmDiskStatsCommand(new ArrayList<String>(), null, null);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(diskStatsCommand, xenServerResourceBase);

        assertTrue(answer.getResult());
    }

    @Test
    public void testCheckHealthCommand() {
        final CheckHealthCommand checkHealthCommand = new CheckHealthCommand();

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(checkHealthCommand, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testStopCommand() {
        final StopCommand stopCommand = new StopCommand("Test", false, false);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(stopCommand, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testRebootCommand() {
        final RebootCommand rebootCommand = new RebootCommand("Test", true);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(rebootCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testCheckVirtualMachineCommand() {
        final CheckVirtualMachineCommand virtualMachineCommand = new CheckVirtualMachineCommand("Test");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(virtualMachineCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testPrepareForMigrationCommand() {
        final VirtualMachineTO machineTO = Mockito.mock(VirtualMachineTO.class);
        final PrepareForMigrationCommand prepareCommand = new PrepareForMigrationCommand(machineTO);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(prepareCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testMigrateCommand() {
        final VirtualMachineTO machineTO = Mockito.mock(VirtualMachineTO.class);
        final MigrateCommand migrateCommand = new MigrateCommand("Test", "127.0.0.1", false, machineTO, false);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(migrateCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testDestroyCommand() {

        final VMTemplateStorageResourceAssoc templateStorage = Mockito.mock(VMTemplateStorageResourceAssoc.class);
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);

        final DestroyCommand destroyCommand = new DestroyCommand(poolVO, templateStorage);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(destroyCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testCreateStoragePoolCommand() {
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final CreateStoragePoolCommand createStorageCommand = new CreateStoragePoolCommand(false, poolVO);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        final Answer answer = wrapper.execute(createStorageCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testModifyStoragePoolCommand() {
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final ModifyStoragePoolCommand modifyStorageCommand = new ModifyStoragePoolCommand(false, poolVO);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        final Answer answer = wrapper.execute(modifyStorageCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testDeleteStoragePoolCommand() {
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final DeleteStoragePoolCommand deleteStorageCommand = new DeleteStoragePoolCommand(poolVO);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        final Answer answer = wrapper.execute(deleteStorageCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testResizeVolumeCommand() {
        final StorageFilerTO pool = Mockito.mock(StorageFilerTO.class);

        final ResizeVolumeCommand resizeCommand = new ResizeVolumeCommand("Test", pool, 1l, 3l, false, "Tests-1");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(resizeCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testAttachIsoCommand() {
        final AttachIsoCommand attachCommand = new AttachIsoCommand("Test", "/", true);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(attachCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testUpgradeSnapshotCommand() {
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);

        final UpgradeSnapshotCommand upgradeSnapshotCommand = new UpgradeSnapshotCommand(poolVO, "http", 1l, 1l, 1l, 1l, 1l, "/", "58c5778b-7dd1-47cc-a7b5-f768541bf278", "Test",
                        "2.1");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(upgradeSnapshotCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testUpgradeSnapshotCommandNo21() {
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);

        final UpgradeSnapshotCommand upgradeSnapshotCommand = new UpgradeSnapshotCommand(poolVO, "http", 1l, 1l, 1l, 1l, 1l, "/", "58c5778b-7dd1-47cc-a7b5-f768541bf278", "Test",
                        "3.1");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(upgradeSnapshotCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(0)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testGetStorageStatsCommand() {
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);
        final DataStoreTO store = Mockito.mock(DataStoreTO.class);

        final GetStorageStatsCommand storageStatsCommand = new GetStorageStatsCommand(store);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        final Answer answer = wrapper.execute(storageStatsCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testPrimaryStorageDownloadCommand() {
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);
        final StoragePoolVO poolVO = Mockito.mock(StoragePoolVO.class);

        final PrimaryStorageDownloadCommand storageDownloadCommand = new PrimaryStorageDownloadCommand("Test", "http://127.0.0.1", ImageFormat.VHD, 1l, poolVO, 200);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        final Answer answer = wrapper.execute(storageDownloadCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testGetVncPortCommand() {
        final GetVncPortCommand vncPortCommand = new GetVncPortCommand(1l, "Test");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(vncPortCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testSetupCommand() {
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);
        final HostEnvironment env = Mockito.mock(HostEnvironment.class);

        final SetupCommand setupCommand = new SetupCommand(env);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        final Answer answer = wrapper.execute(setupCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testMaintainCommand() {
        // This test needs further work.

        final String uuid = "befc4dcd-f5c6-4015-8791-3c18622b7c7f";

        final Connection conn = Mockito.mock(Connection.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);
        final XmlRpcClient client = Mockito.mock(XmlRpcClient.class);

        // final Host.Record hr = PowerMockito.mock(Host.Record.class);
        // final Host host = PowerMockito.mock(Host.class);

        final MaintainCommand maintainCommand = new MaintainCommand();

        final Map<String, Object> map = new Hashtable<String, Object>();
        map.put("Value", "Xen");

        final Map<String, Object> spiedMap = spy(map);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);
        when(xenServerHost.getUuid()).thenReturn(uuid);
        when(conn.getSessionReference()).thenReturn("befc4dcd");

        try {
            final Object[] params = { Marshalling.toXMLRPC("befc4dcd"), Marshalling.toXMLRPC(uuid) };
            when(client.execute("host.get_by_uuid", new Object[] { "befc4dcd", uuid })).thenReturn(spiedMap);
            PowerMockito.when(conn, "dispatch", "host.get_by_uuid", params).thenReturn(spiedMap);
        } catch (final Exception e) {
            fail(e.getMessage());
        }

        // try {
        // PowerMockito.mockStatic(Host.class);
        // //BDDMockito.given(Host.getByUuid(conn,
        // xenServerHost.getUuid())).willReturn(host);
        // PowerMockito.when(Host.getByUuid(conn,
        // xenServerHost.getUuid())).thenReturn(host);
        // PowerMockito.verifyStatic(times(1));
        // } catch (final BadServerResponse e) {
        // fail(e.getMessage());
        // } catch (final XenAPIException e) {
        // fail(e.getMessage());
        // } catch (final XmlRpcException e) {
        // fail(e.getMessage());
        // }
        //
        // PowerMockito.mockStatic(Types.class);
        // PowerMockito.when(Types.toHostRecord(spiedMap)).thenReturn(hr);
        // PowerMockito.verifyStatic(times(1));
        //
        // try {
        // PowerMockito.mockStatic(Host.Record.class);
        // when(host.getRecord(conn)).thenReturn(hr);
        // verify(host, times(1)).getRecord(conn);
        // } catch (final BadServerResponse e) {
        // fail(e.getMessage());
        // } catch (final XenAPIException e) {
        // fail(e.getMessage());
        // } catch (final XmlRpcException e) {
        // fail(e.getMessage());
        // }

        final Answer answer = wrapper.execute(maintainCommand, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testPingTestCommandHostIp() {
        final PingTestCommand pingTestCommand = new PingTestCommand("127.0.0.1");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(pingTestCommand, xenServerResourceBase);
        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testPingTestCommandRouterPvtIps() {
        final PingTestCommand pingTestCommand = new PingTestCommand("127.0.0.1", "127.0.0.1");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(pingTestCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testCheckOnHostCommand() {
        final com.cloud.host.Host host = Mockito.mock(com.cloud.host.Host.class);
        final CheckOnHostCommand onHostCommand = new CheckOnHostCommand(host);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(onHostCommand, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testModifySshKeysCommand() {
        final ModifySshKeysCommand sshKeysCommand = new ModifySshKeysCommand("", "");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(sshKeysCommand, xenServerResourceBase);

        assertTrue(answer.getResult());
    }

    @Test
    public void testStartCommand() {
        final VirtualMachineTO vm = Mockito.mock(VirtualMachineTO.class);
        final com.cloud.host.Host host = Mockito.mock(com.cloud.host.Host.class);

        final StartCommand startCommand = new StartCommand(vm, host, false);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(startCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testOvsSetTagAndFlowCommand() {
        final Network network = Mockito.mock(Network.class);
        final Connection conn = Mockito.mock(Connection.class);

        final OvsSetTagAndFlowCommand tagAndFlowCommand = new OvsSetTagAndFlowCommand("Test", "tag", "vlan://1", "123", 1l);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.setupvSwitchNetwork(conn)).thenReturn(network);
        try {
            when(network.getBridge(conn)).thenReturn("br0");
        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(tagAndFlowCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerResourceBase, times(1)).setupvSwitchNetwork(conn);
        verify(xenServerResourceBase, times(1)).setIsOvs(true);

        assertFalse(answer.getResult());
    }

    @Test
    public void testCheckSshCommand() {
        final CheckSshCommand sshCommand = new CheckSshCommand("Test", "127.0.0.1", 22);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(sshCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testSecurityGroupRulesCommand() {
        final Connection conn = Mockito.mock(Connection.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final String guestIp = "127.0.0.1";
        final String guestMac = "00:00:00:00";
        final String vmName = "Test";
        final Long vmId = 1l;
        final String signature = "signature";
        final Long seqNum = 1l;
        final IpPortAndProto[] ingressRuleSet = new IpPortAndProto[]{Mockito.mock(IpPortAndProto.class)};
        final IpPortAndProto[] egressRuleSet = new IpPortAndProto[]{Mockito.mock(IpPortAndProto.class)};
        final List<String> secIps = new Vector<String>();

        final SecurityGroupRulesCmd sshCommand = new SecurityGroupRulesCmd(guestIp, guestMac, vmName, vmId, signature, seqNum, ingressRuleSet, egressRuleSet, secIps);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        final Answer answer = wrapper.execute(sshCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testOvsFetchInterfaceCommand() {
        final String label = "[abc]";
        final String uuid = "befc4dcd-f5c6-4015-8791-3c18622b7c7f";

        final Connection conn = Mockito.mock(Connection.class);
        final XsLocalNetwork network = Mockito.mock(XsLocalNetwork.class);
        final Network network2 = Mockito.mock(Network.class);
        final PIF pif = Mockito.mock(PIF.class);
        final PIF.Record pifRec = Mockito.mock(PIF.Record.class);

        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final OvsFetchInterfaceCommand fetchInterCommand = new OvsFetchInterfaceCommand(label);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.isXcp()).thenReturn(true);
        when(xenServerResourceBase.getLabel()).thenReturn("[abc]");
        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);

        try {
            when(network.getNetwork()).thenReturn(network2);
            when(network.getPif(conn)).thenReturn(pif);
            when(network.getPif(conn)).thenReturn(pif);
            when(pif.getRecord(conn)).thenReturn(pifRec);
            when(network.getNetwork().getUuid(conn)).thenReturn(uuid);
            when(xenServerResourceBase.getNetworkByName(conn, label)).thenReturn(network);
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(fetchInterCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testOvsCreateGreTunnelCommand() {
        final String bridge = "gre";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final OvsCreateGreTunnelCommand createGreCommand = new OvsCreateGreTunnelCommand("127.0.0.1", "KEY", 1l, 2l);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);
        when(xenServerResourceBase.setupvSwitchNetwork(conn)).thenReturn(network);
        try {
            when(network.getBridge(conn)).thenReturn(bridge);
            when(
                            xenServerResourceBase.callHostPlugin(conn, "ovsgre", "ovs_create_gre", "bridge", bridge, "remoteIP", createGreCommand.getRemoteIp(), "greKey",
                                            createGreCommand.getKey(), "from", Long.toString(createGreCommand.getFrom()), "to", Long.toString(createGreCommand.getTo()))).thenReturn(
                            "1:2");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(createGreCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerResourceBase, times(1)).setIsOvs(true);

        assertTrue(answer.getResult());
    }

    @Test
    public void testOvsDeleteFlowCommandSuccess() {
        final String bridge = "gre";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsDeleteFlowCommand deleteFlowCommand = new OvsDeleteFlowCommand("Test");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.setupvSwitchNetwork(conn)).thenReturn(network);
        try {
            when(network.getBridge(conn)).thenReturn(bridge);
            when(xenServerResourceBase.callHostPlugin(conn, "ovsgre", "ovs_delete_flow", "bridge", bridge, "vmName", deleteFlowCommand.getVmName())).thenReturn("SUCCESS");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(deleteFlowCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerResourceBase, times(1)).setIsOvs(true);

        assertTrue(answer.getResult());
    }

    @Test
    public void testOvsDeleteFlowCommandFailure() {
        final String bridge = "gre";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsDeleteFlowCommand deleteFlowCommand = new OvsDeleteFlowCommand("Test");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.setupvSwitchNetwork(conn)).thenReturn(network);
        try {
            when(network.getBridge(conn)).thenReturn(bridge);
            when(xenServerResourceBase.callHostPlugin(conn, "ovsgre", "ovs_delete_flow", "bridge", bridge, "vmName", deleteFlowCommand.getVmName())).thenReturn("FAILED");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(deleteFlowCommand, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerResourceBase, times(1)).setIsOvs(true);

        assertFalse(answer.getResult());
    }

    @Test
    public void testOvsVpcPhysicalTopologyConfigCommand() {
        final String bridge = "gre";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsVpcPhysicalTopologyConfigCommand.Host[] hosts = new OvsVpcPhysicalTopologyConfigCommand.Host[0];
        final OvsVpcPhysicalTopologyConfigCommand.Tier[] tiers = new OvsVpcPhysicalTopologyConfigCommand.Tier[0];
        final OvsVpcPhysicalTopologyConfigCommand.Vm[] vms = new OvsVpcPhysicalTopologyConfigCommand.Vm[0];

        final OvsVpcPhysicalTopologyConfigCommand physicalTopology = new OvsVpcPhysicalTopologyConfigCommand(hosts, tiers, vms, "10.0.0.1/24");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, physicalTopology.getBridgeName())).thenReturn(network);
            when(network.getBridge(conn)).thenReturn(bridge);

            when(
                            xenServerResourceBase.callHostPlugin(conn, "ovstunnel", "configure_ovs_bridge_for_network_topology", "bridge", bridge, "config",
                                            physicalTopology.getVpcConfigInJson(), "host-id", ((Long) physicalTopology.getHostId()).toString(), "seq-no", Long.toString(1))).thenReturn(
                            "SUCCESS");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(physicalTopology, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testOvsVpcRoutingPolicyConfigCommand() {
        final String bridge = "gre";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsVpcRoutingPolicyConfigCommand.Acl[] acls = new OvsVpcRoutingPolicyConfigCommand.Acl[0];
        final OvsVpcRoutingPolicyConfigCommand.Tier[] tiers = new OvsVpcRoutingPolicyConfigCommand.Tier[0];

        final OvsVpcRoutingPolicyConfigCommand routingPolicy = new OvsVpcRoutingPolicyConfigCommand("v1", "10.0.0.1/24", acls, tiers);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, routingPolicy.getBridgeName())).thenReturn(network);
            when(network.getBridge(conn)).thenReturn(bridge);

            when(
                            xenServerResourceBase.callHostPlugin(conn, "ovstunnel", "configure_ovs_bridge_for_routing_policies", "bridge", bridge, "host-id",
                                            ((Long) routingPolicy.getHostId()).toString(), "config", routingPolicy.getVpcConfigInJson(), "seq-no", Long.toString(1))).thenReturn(
                            "SUCCESS");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(routingPolicy, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testCleanupNetworkRulesCmd() {
        final Connection conn = Mockito.mock(Connection.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final CleanupNetworkRulesCmd cleanupNets = new CleanupNetworkRulesCmd(20);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.canBridgeFirewall()).thenReturn(true);
        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);
        when(xenServerResourceBase.getVMInstanceName()).thenReturn("VM");
        when(xenServerResourceBase.callHostPlugin(conn, "vmops", "cleanup_rules", "instance", xenServerResourceBase.getVMInstanceName())).thenReturn("1");

        final Answer answer = wrapper.execute(cleanupNets, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testCleanupNetworkRulesCmdLTZ() {
        final Connection conn = Mockito.mock(Connection.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final CleanupNetworkRulesCmd cleanupNets = new CleanupNetworkRulesCmd(20);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.canBridgeFirewall()).thenReturn(true);
        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);
        when(xenServerResourceBase.getVMInstanceName()).thenReturn("VM");
        when(xenServerResourceBase.callHostPlugin(conn, "vmops", "cleanup_rules", "instance", xenServerResourceBase.getVMInstanceName())).thenReturn("-1");

        final Answer answer = wrapper.execute(cleanupNets, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerHost, times(1)).getIp();

        assertFalse(answer.getResult());
        assertEquals(answer.getDetails(), "-1");
    }

    @Test
    public void testCleanupNetworkRulesCmdNullDetails() {
        final CleanupNetworkRulesCmd cleanupNets = new CleanupNetworkRulesCmd(20);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.canBridgeFirewall()).thenReturn(false);
        final Answer answer = wrapper.execute(cleanupNets, xenServerResourceBase);

        assertTrue(answer.getResult());
        assertNull(answer.getDetails());
    }

    @Test
    public void testNetworkRulesSystemVmCommand() {
        final Connection conn = Mockito.mock(Connection.class);

        final NetworkRulesSystemVmCommand netRules = new NetworkRulesSystemVmCommand("Test", VirtualMachine.Type.User);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);

        final Answer answer = wrapper.execute(netRules, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testNetworkRulesSystemVmCommandNonUser() {
        final Connection conn = Mockito.mock(Connection.class);

        final NetworkRulesSystemVmCommand netRules = new NetworkRulesSystemVmCommand("Test", VirtualMachine.Type.DomainRouter);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.callHostPlugin(conn, "vmops", "default_network_rules_systemvm", "vmName", netRules.getVmName())).thenReturn("true");

        final Answer answer = wrapper.execute(netRules, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testNetworkRulesSystemVmCommandNonUserFalse() {
        final Connection conn = Mockito.mock(Connection.class);

        final NetworkRulesSystemVmCommand netRules = new NetworkRulesSystemVmCommand("Test", VirtualMachine.Type.DomainRouter);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.callHostPlugin(conn, "vmops", "default_network_rules_systemvm", "vmName", netRules.getVmName())).thenReturn("false");

        final Answer answer = wrapper.execute(netRules, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testOvsCreateTunnelCommandSuccess() {
        final String bridge = "tunnel";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsCreateTunnelCommand createTunnel = new OvsCreateTunnelCommand("127.0.0.1", 1, 1l, 2l, 1l, "127.0.1.1", "net01", "cd84c713-f448-48c9-ba25-e6740d4a9003");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, createTunnel.getNetworkName())).thenReturn(network);
            when(network.getBridge(conn)).thenReturn(bridge);

            when(xenServerResourceBase.callHostPlugin(conn, "ovstunnel", "create_tunnel", "bridge", bridge, "remote_ip", createTunnel.getRemoteIp(),
                            "key", createTunnel.getKey().toString(), "from",
                            createTunnel.getFrom().toString(), "to", createTunnel.getTo().toString(), "cloudstack-network-id",
                            createTunnel.getNetworkUuid())).thenReturn("SUCCESS:0");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(createTunnel, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerResourceBase, times(1)).configureTunnelNetwork(conn, createTunnel.getNetworkId(), createTunnel.getFrom(), createTunnel.getNetworkName());

        assertTrue(answer.getResult());
    }

    @Test
    public void testOvsCreateTunnelCommandFail() {
        final String bridge = "tunnel";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsCreateTunnelCommand createTunnel = new OvsCreateTunnelCommand("127.0.0.1", 1, 1l, 2l, 1l, "127.0.1.1", "net01", "cd84c713-f448-48c9-ba25-e6740d4a9003");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, createTunnel.getNetworkName())).thenReturn(network);
            when(network.getBridge(conn)).thenReturn(bridge);

            when(xenServerResourceBase.callHostPlugin(conn, "ovstunnel", "create_tunnel", "bridge", bridge, "remote_ip", createTunnel.getRemoteIp(),
                            "key", createTunnel.getKey().toString(), "from",
                            createTunnel.getFrom().toString(), "to", createTunnel.getTo().toString(), "cloudstack-network-id",
                            createTunnel.getNetworkUuid())).thenReturn("FAIL:1");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(createTunnel, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerResourceBase, times(1)).configureTunnelNetwork(conn, createTunnel.getNetworkId(), createTunnel.getFrom(), createTunnel.getNetworkName());

        assertFalse(answer.getResult());
    }

    @Test
    public void testOvsCreateTunnelCommandNoNet() {
        final Connection conn = Mockito.mock(Connection.class);

        final OvsCreateTunnelCommand createTunnel = new OvsCreateTunnelCommand("127.0.0.1", 1, 1l, 2l, 1l, "127.0.1.1", "net01", "cd84c713-f448-48c9-ba25-e6740d4a9003");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, createTunnel.getNetworkName())).thenReturn(null);

        final Answer answer = wrapper.execute(createTunnel, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testOvsSetupBridgeCommand() {
        final Connection conn = Mockito.mock(Connection.class);

        final OvsSetupBridgeCommand setupBridge = new OvsSetupBridgeCommand("Test", 1l, 1l);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);

        final Answer answer = wrapper.execute(setupBridge, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        verify(xenServerResourceBase, times(1)).findOrCreateTunnelNetwork(conn, setupBridge.getBridgeName());
        verify(xenServerResourceBase, times(1)).configureTunnelNetwork(conn, setupBridge.getNetworkId(), setupBridge.getHostId(), setupBridge.getBridgeName());

        assertTrue(answer.getResult());
    }

    @Test
    public void testOvsDestroyBridgeCommand() {
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsDestroyBridgeCommand destroyBridge = new OvsDestroyBridgeCommand(1l, "bridge", 1l);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, destroyBridge.getBridgeName())).thenReturn(network);

        final Answer answer = wrapper.execute(destroyBridge, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();
        try {
            verify(xenServerResourceBase, times(1)).cleanUpTmpDomVif(conn, network);
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }
        verify(xenServerResourceBase, times(1)).destroyTunnelNetwork(conn, network, destroyBridge.getHostId());

        assertTrue(answer.getResult());
    }

    @Test
    public void testOvsDestroyTunnelCommandSuccess() {
        final String bridge = "tunnel";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsDestroyTunnelCommand destroyTunnel = new OvsDestroyTunnelCommand(1l, "net01", "port11");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, destroyTunnel.getBridgeName())).thenReturn(network);
            when(network.getBridge(conn)).thenReturn(bridge);

            when(xenServerResourceBase.callHostPlugin(conn, "ovstunnel", "destroy_tunnel", "bridge", bridge, "in_port", destroyTunnel.getInPortName())).thenReturn("SUCCESS");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(destroyTunnel, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testOvsDestroyTunnelCommandFailed() {
        final String bridge = "tunnel";
        final Connection conn = Mockito.mock(Connection.class);
        final Network network = Mockito.mock(Network.class);

        final OvsDestroyTunnelCommand destroyTunnel = new OvsDestroyTunnelCommand(1l, "net01", "port11");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.findOrCreateTunnelNetwork(conn, destroyTunnel.getBridgeName())).thenReturn(network);
            when(network.getBridge(conn)).thenReturn(bridge);

            when(xenServerResourceBase.callHostPlugin(conn, "ovstunnel", "destroy_tunnel", "bridge", bridge, "in_port", destroyTunnel.getInPortName())).thenReturn("FAILED");

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(destroyTunnel, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateHostPasswordCommand() {
        final Pair<Boolean, String> result = Mockito.mock(Pair.class);

        final UpdateHostPasswordCommand updatePwd = new UpdateHostPasswordCommand("test", "123", "127.0.0.1");

        when(xenServerResourceBase.getPwdFromQueue()).thenReturn("password");

        final String hostIp = updatePwd.getHostIp();
        final String username = updatePwd.getUsername();
        final String hostPasswd = xenServerResourceBase.getPwdFromQueue();
        final String newPassword = updatePwd.getNewPassword();

        final StringBuilder cmdLine = new StringBuilder();
        cmdLine.append(XenServerHelper.SCRIPT_CMD_PATH).append(VRScripts.UPDATE_HOST_PASSWD).append(' ').append(username).append(' ').append(newPassword);

        when(XenServerHelper.buildCommandLine(XenServerHelper.SCRIPT_CMD_PATH, VRScripts.UPDATE_HOST_PASSWD, username, newPassword)).thenReturn(
                        cmdLine.toString());

        try {
            when(XenServerHelper.executeSshWrapper(hostIp, 22, username, null, hostPasswd, cmdLine.toString())).thenReturn(result);
            when(result.first()).thenReturn(true);
            when(result.second()).thenReturn("");
        } catch (final Exception e) {
            fail(e.getMessage());
        }

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(updatePwd, xenServerResourceBase);

        verify(xenServerResourceBase, times(2)).getPwdFromQueue();
        XenServerHelper.buildCommandLine(XenServerHelper.SCRIPT_CMD_PATH, VRScripts.UPDATE_HOST_PASSWD, username, newPassword);
        try {
            XenServerHelper.executeSshWrapper(hostIp, 22, username, null, hostPasswd, cmdLine.toString());
        } catch (final Exception e) {
            fail(e.getMessage());
        }
        verify(result, times(1)).first();
        verify(result, times(1)).second();

        assertTrue(answer.getResult());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateHostPasswordCommandFail() {
        final Pair<Boolean, String> result = Mockito.mock(Pair.class);

        final UpdateHostPasswordCommand updatePwd = new UpdateHostPasswordCommand("test", "123", "127.0.0.1");

        when(xenServerResourceBase.getPwdFromQueue()).thenReturn("password");

        final String hostIp = updatePwd.getHostIp();
        final String username = updatePwd.getUsername();
        final String hostPasswd = xenServerResourceBase.getPwdFromQueue();
        final String newPassword = updatePwd.getNewPassword();

        final StringBuilder cmdLine = new StringBuilder();
        cmdLine.append(XenServerHelper.SCRIPT_CMD_PATH).append(VRScripts.UPDATE_HOST_PASSWD).append(' ').append(username).append(' ').append(newPassword);

        when(XenServerHelper.buildCommandLine(XenServerHelper.SCRIPT_CMD_PATH, VRScripts.UPDATE_HOST_PASSWD, username, newPassword)).thenReturn(
                        cmdLine.toString());

        try {
            when(XenServerHelper.executeSshWrapper(hostIp, 22, username, null, hostPasswd, cmdLine.toString())).thenReturn(result);
            when(result.first()).thenReturn(false);
            when(result.second()).thenReturn("");
        } catch (final Exception e) {
            fail(e.getMessage());
        }

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(updatePwd, xenServerResourceBase);

        verify(xenServerResourceBase, times(2)).getPwdFromQueue();
        XenServerHelper.buildCommandLine(XenServerHelper.SCRIPT_CMD_PATH, VRScripts.UPDATE_HOST_PASSWD, username, newPassword);
        try {
            XenServerHelper.executeSshWrapper(hostIp, 22, username, null, hostPasswd, cmdLine.toString());
        } catch (final Exception e) {
            fail(e.getMessage());
        }
        verify(result, times(1)).first();
        verify(result, times(1)).second();

        assertFalse(answer.getResult());
    }

    @Test
    public void testUpdateHostPasswordCommandException() {

        final UpdateHostPasswordCommand updatePwd = new UpdateHostPasswordCommand("test", "123", "127.0.0.1");

        when(xenServerResourceBase.getPwdFromQueue()).thenReturn("password");

        final String hostIp = updatePwd.getHostIp();
        final String username = updatePwd.getUsername();
        final String hostPasswd = xenServerResourceBase.getPwdFromQueue();
        final String newPassword = updatePwd.getNewPassword();

        final StringBuilder cmdLine = new StringBuilder();
        cmdLine.append(XenServerHelper.SCRIPT_CMD_PATH).append(VRScripts.UPDATE_HOST_PASSWD).append(' ').append(username).append(' ').append(newPassword);

        when(XenServerHelper.buildCommandLine(XenServerHelper.SCRIPT_CMD_PATH, VRScripts.UPDATE_HOST_PASSWD, username, newPassword)).thenReturn(
                        cmdLine.toString());

        try {
            when(XenServerHelper.executeSshWrapper(hostIp, 22, username, null, hostPasswd, cmdLine.toString())).thenThrow(new Exception("testing failure"));
        } catch (final Exception e) {
            fail(e.getMessage());
        }

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(updatePwd, xenServerResourceBase);

        verify(xenServerResourceBase, times(2)).getPwdFromQueue();
        XenServerHelper.buildCommandLine(XenServerHelper.SCRIPT_CMD_PATH, VRScripts.UPDATE_HOST_PASSWD, username, newPassword);
        try {
            XenServerHelper.executeSshWrapper(hostIp, 22, username, null, hostPasswd, cmdLine.toString());
        } catch (final Exception e) {
            fail(e.getMessage());
        }

        assertFalse(answer.getResult());
    }

    @Test
    public void testClusterVMMetaDataSyncCommand() {
        final String uuid = "6172d8b7-ba10-4a70-93f9-ecaf41f51d53";

        final Connection conn = Mockito.mock(Connection.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);

        final Pool pool = PowerMockito.mock(Pool.class);
        final Pool.Record poolr = Mockito.mock(Pool.Record.class);
        final Host.Record hostr = Mockito.mock(Host.Record.class);
        final Host master = Mockito.mock(Host.class);

        final ClusterVMMetaDataSyncCommand vmDataSync = new ClusterVMMetaDataSyncCommand(10, 1l);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);
            when(xenServerResourceBase.getHost().getUuid()).thenReturn(uuid);

            PowerMockito.mockStatic(Pool.Record.class);

            when(pool.getRecord(conn)).thenReturn(poolr);
            poolr.master = master;
            when(poolr.master.getRecord(conn)).thenReturn(hostr);
            hostr.uuid = uuid;

        } catch (final BadServerResponse e) {
            fail(e.getMessage());
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(vmDataSync, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testCheckNetworkCommandSuccess() {
        final List<PhysicalNetworkSetupInfo> setupInfos = new ArrayList<PhysicalNetworkSetupInfo>();

        final CheckNetworkCommand checkNet = new CheckNetworkCommand(setupInfos);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(checkNet, xenServerResourceBase);

        assertTrue(answer.getResult());
    }

    @Test
    public void testCheckNetworkCommandFailure() {
        final PhysicalNetworkSetupInfo info = new PhysicalNetworkSetupInfo();

        final List<PhysicalNetworkSetupInfo> setupInfos = new ArrayList<PhysicalNetworkSetupInfo>();
        setupInfos.add(info);

        final CheckNetworkCommand checkNet = new CheckNetworkCommand(setupInfos);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(checkNet, xenServerResourceBase);

        assertFalse(answer.getResult());
    }

    @Test
    public void testPlugNicCommand() {
        final NicTO nicTO = Mockito.mock(NicTO.class);
        final Connection conn = Mockito.mock(Connection.class);

        final PlugNicCommand plugNic = new PlugNicCommand(nicTO, "Test", VirtualMachine.Type.User);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);

        final Answer answer = wrapper.execute(plugNic, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testUnPlugNicCommand() {
        final NicTO nicTO = Mockito.mock(NicTO.class);
        final Connection conn = Mockito.mock(Connection.class);

        final UnPlugNicCommand unplugNic = new UnPlugNicCommand(nicTO, "Test");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);

        final Answer answer = wrapper.execute(unplugNic, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testCreateVMSnapshotCommand() {
        final Connection conn = Mockito.mock(Connection.class);

        final VMSnapshotTO snapshotTO = Mockito.mock(VMSnapshotTO.class);
        final List<VolumeObjectTO> volumeTOs = new ArrayList<VolumeObjectTO>();

        final CreateVMSnapshotCommand vmSnapshot = new CreateVMSnapshotCommand("Test", "uuid", snapshotTO, volumeTOs, "Debian");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);

        final Answer answer = wrapper.execute(vmSnapshot, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testDeleteVMSnapshotCommand() {
        final Connection conn = Mockito.mock(Connection.class);

        final VMSnapshotTO snapshotTO = Mockito.mock(VMSnapshotTO.class);
        final List<VolumeObjectTO> volumeTOs = new ArrayList<VolumeObjectTO>();

        final DeleteVMSnapshotCommand vmSnapshot = new DeleteVMSnapshotCommand("Test", snapshotTO, volumeTOs, "Debian");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);

        final Answer answer = wrapper.execute(vmSnapshot, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testRevertToVMSnapshotCommand() {
        final Connection conn = Mockito.mock(Connection.class);

        final VMSnapshotTO snapshotTO = Mockito.mock(VMSnapshotTO.class);
        final List<VolumeObjectTO> volumeTOs = new ArrayList<VolumeObjectTO>();

        final RevertToVMSnapshotCommand vmSnapshot = new RevertToVMSnapshotCommand("Test", "uuid", snapshotTO, volumeTOs, "Debian");

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);

        final Answer answer = wrapper.execute(vmSnapshot, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testNetworkRulesVmSecondaryIpCommandSuccess() {
        final Connection conn = Mockito.mock(Connection.class);

        final NetworkRulesVmSecondaryIpCommand rulesVm = new NetworkRulesVmSecondaryIpCommand("Test", VirtualMachine.Type.User);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.callHostPlugin(conn, "vmops", "network_rules_vmSecondaryIp", "vmName", rulesVm.getVmName(), "vmMac", rulesVm.getVmMac(),
                        "vmSecIp", rulesVm.getVmSecIp(), "action", rulesVm.getAction())).thenReturn("true");

        final Answer answer = wrapper.execute(rulesVm, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testNetworkRulesVmSecondaryIpCommandFailure() {
        final Connection conn = Mockito.mock(Connection.class);

        final NetworkRulesVmSecondaryIpCommand rulesVm = new NetworkRulesVmSecondaryIpCommand("Test", VirtualMachine.Type.User);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.callHostPlugin(conn, "vmops", "network_rules_vmSecondaryIp", "vmName", rulesVm.getVmName(), "vmMac", rulesVm.getVmMac(),
                        "vmSecIp", rulesVm.getVmSecIp(), "action", rulesVm.getAction())).thenReturn("false");

        final Answer answer = wrapper.execute(rulesVm, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testScaleVmCommand() {
        final String uuid = "6172d8b7-ba10-4a70-93f9-ecaf41f51d53";

        final VirtualMachineTO machineTO = Mockito.mock(VirtualMachineTO.class);
        final Connection conn = Mockito.mock(Connection.class);
        final XenServerHost xenServerHost = Mockito.mock(XenServerHost.class);
        final Host host = Mockito.mock(Host.class);

        final ScaleVmCommand scaleVm = new ScaleVmCommand(machineTO);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getHost()).thenReturn(xenServerHost);
        when(xenServerResourceBase.getHost().getUuid()).thenReturn(uuid);

        try {
            when(xenServerResourceBase.isDmcEnabled(conn, host)).thenReturn(true);
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        final Answer answer = wrapper.execute(scaleVm, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testPvlanSetupCommandDhcpSuccess() {
        final String label = "net";

        final Connection conn = Mockito.mock(Connection.class);
        final XsLocalNetwork network = Mockito.mock(XsLocalNetwork.class);
        final Network network2 = Mockito.mock(Network.class);

        final PvlanSetupCommand lanSetup = PvlanSetupCommand.createDhcpSetup("add", URI.create("http://127.0.0.1"), "tag", "dhcp", "0:0:0:0:0:0", "127.0.0.1");

        final String primaryPvlan = lanSetup.getPrimary();
        final String isolatedPvlan = lanSetup.getIsolated();
        final String op = lanSetup.getOp();
        final String dhcpName = lanSetup.getDhcpName();
        final String dhcpMac = lanSetup.getDhcpMac();
        final String dhcpIp = lanSetup.getDhcpIp();

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.getNativeNetworkForTraffic(conn, TrafficType.Guest, "tag")).thenReturn(network);
            when(network.getNetwork()).thenReturn(network2);
            when(network2.getNameLabel(conn)).thenReturn(label);
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        when(xenServerResourceBase.callHostPlugin(conn, "ovs-pvlan", "setup-pvlan-dhcp", "op", op, "nw-label", label, "primary-pvlan", primaryPvlan, "isolated-pvlan",
                        isolatedPvlan, "dhcp-name", dhcpName, "dhcp-ip", dhcpIp, "dhcp-mac", dhcpMac)).thenReturn("true");

        final Answer answer = wrapper.execute(lanSetup, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testPvlanSetupCommandDhcpFailure() {
        final String label = "net";

        final Connection conn = Mockito.mock(Connection.class);
        final XsLocalNetwork network = Mockito.mock(XsLocalNetwork.class);
        final Network network2 = Mockito.mock(Network.class);

        final PvlanSetupCommand lanSetup = PvlanSetupCommand.createDhcpSetup("add", URI.create("http://127.0.0.1"), "tag", "dhcp", "0:0:0:0:0:0", "127.0.0.1");

        final String primaryPvlan = lanSetup.getPrimary();
        final String isolatedPvlan = lanSetup.getIsolated();
        final String op = lanSetup.getOp();
        final String dhcpName = lanSetup.getDhcpName();
        final String dhcpMac = lanSetup.getDhcpMac();
        final String dhcpIp = lanSetup.getDhcpIp();

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.getNativeNetworkForTraffic(conn, TrafficType.Guest, "tag")).thenReturn(network);
            when(network.getNetwork()).thenReturn(network2);
            when(network2.getNameLabel(conn)).thenReturn(label);
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        when(xenServerResourceBase.callHostPlugin(conn, "ovs-pvlan", "setup-pvlan-dhcp", "op", op, "nw-label", label, "primary-pvlan", primaryPvlan, "isolated-pvlan",
                        isolatedPvlan, "dhcp-name", dhcpName, "dhcp-ip", dhcpIp, "dhcp-mac", dhcpMac)).thenReturn("false");

        final Answer answer = wrapper.execute(lanSetup, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testPvlanSetupCommandVmSuccess() {
        final String label = "net";

        final Connection conn = Mockito.mock(Connection.class);
        final XsLocalNetwork network = Mockito.mock(XsLocalNetwork.class);
        final Network network2 = Mockito.mock(Network.class);

        final PvlanSetupCommand lanSetup = PvlanSetupCommand.createVmSetup("add", URI.create("http://127.0.0.1"), "tag", "0:0:0:0:0:0");

        final String primaryPvlan = lanSetup.getPrimary();
        final String isolatedPvlan = lanSetup.getIsolated();
        final String op = lanSetup.getOp();
        final String vmMac = lanSetup.getVmMac();

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.getNativeNetworkForTraffic(conn, TrafficType.Guest, "tag")).thenReturn(network);
            when(network.getNetwork()).thenReturn(network2);
            when(network2.getNameLabel(conn)).thenReturn(label);
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        when(xenServerResourceBase.callHostPlugin(conn, "ovs-pvlan", "setup-pvlan-vm", "op", op, "nw-label", label, "primary-pvlan", primaryPvlan, "isolated-pvlan",
                        isolatedPvlan, "vm-mac", vmMac)).thenReturn("true");

        final Answer answer = wrapper.execute(lanSetup, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testPvlanSetupCommandVmFailure() {
        final String label = "net";

        final Connection conn = Mockito.mock(Connection.class);
        final XsLocalNetwork network = Mockito.mock(XsLocalNetwork.class);
        final Network network2 = Mockito.mock(Network.class);

        final PvlanSetupCommand lanSetup = PvlanSetupCommand.createVmSetup("add", URI.create("http://127.0.0.1"), "tag", "0:0:0:0:0:0");

        final String primaryPvlan = lanSetup.getPrimary();
        final String isolatedPvlan = lanSetup.getIsolated();
        final String op = lanSetup.getOp();
        final String vmMac = lanSetup.getVmMac();

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        try {
            when(xenServerResourceBase.getNativeNetworkForTraffic(conn, TrafficType.Guest, "tag")).thenReturn(network);
            when(network.getNetwork()).thenReturn(network2);
            when(network2.getNameLabel(conn)).thenReturn(label);
        } catch (final XenAPIException e) {
            fail(e.getMessage());
        } catch (final XmlRpcException e) {
            fail(e.getMessage());
        }

        when(xenServerResourceBase.callHostPlugin(conn, "ovs-pvlan", "setup-pvlan-vm", "op", op, "nw-label", label, "primary-pvlan", primaryPvlan, "isolated-pvlan",
                        isolatedPvlan, "vm-mac", vmMac)).thenReturn("false");

        final Answer answer = wrapper.execute(lanSetup, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testPerformanceMonitorCommandSuccess() {
        final Connection conn = Mockito.mock(Connection.class);

        final PerformanceMonitorCommand performanceMonitor = new PerformanceMonitorCommand(new Hashtable<String, String>(), 200);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getPerfMon(conn, performanceMonitor.getParams(), performanceMonitor.getWait())).thenReturn("performance");

        final Answer answer = wrapper.execute(performanceMonitor, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

    @Test
    public void testPerformanceMonitorCommandFailure() {
        final Connection conn = Mockito.mock(Connection.class);

        final PerformanceMonitorCommand performanceMonitor = new PerformanceMonitorCommand(new Hashtable<String, String>(), 200);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getPerfMon(conn, performanceMonitor.getParams(), performanceMonitor.getWait())).thenReturn(null);

        final Answer answer = wrapper.execute(performanceMonitor, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertFalse(answer.getResult());
    }

    @Test
    public void testIpAssocVpcCommand() {
        final VirtualRoutingResource routingResource = Mockito.mock(VirtualRoutingResource.class);
        final IpAddressTO[] ips = new IpAddressTO[0];

        final IpAssocVpcCommand ipAssociation = new IpAssocVpcCommand(ips);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getVirtualRoutingResource()).thenReturn(routingResource);

        final Answer answer = wrapper.execute(ipAssociation, xenServerResourceBase);

        verify(routingResource, times(1)).executeRequest(ipAssociation);

        // Requires more testing, but the VirtualResourceRouting is quite big.
        assertNull(answer);
    }

    @Test
    public void testIpAssocCommand() {
        final VirtualRoutingResource routingResource = Mockito.mock(VirtualRoutingResource.class);
        final IpAddressTO[] ips = new IpAddressTO[0];

        final IpAssocCommand ipAssociation = new IpAssocCommand(ips);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getVirtualRoutingResource()).thenReturn(routingResource);

        final Answer answer = wrapper.execute(ipAssociation, xenServerResourceBase);

        verify(routingResource, times(1)).executeRequest(ipAssociation);

        // Requires more testing, but the VirtualResourceRouting is quite big.
        assertNull(answer);
    }

    @Test
    public void testStorageSubSystemCommand() {
        final DiskTO disk = Mockito.mock(DiskTO.class);
        final String vmName = "Test";
        final AttachCommand command = new AttachCommand(disk, vmName);

        final StorageSubsystemCommandHandler handler = Mockito.mock(StorageSubsystemCommandHandler.class);
        when(xenServerResourceBase.getStorageHandler()).thenReturn(handler);

        when(handler.handleStorageCommands(command)).thenReturn(new AttachAnswer(disk));

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        final Answer answer = wrapper.execute(command, xenServerResourceBase);
        assertTrue(answer.getResult());
    }

    @Test
    public void testGetVmIpAddressCommand() throws XenAPIException, XmlRpcException {

        final Connection conn = Mockito.mock(Connection.class);
        final VM vm = Mockito.mock(VM.class);
        final VMGuestMetrics mtr = Mockito.mock(VMGuestMetrics.class);
        final VMGuestMetrics.Record rec = Mockito.mock(VMGuestMetrics.Record.class);

        final Map<String, String> vmIpsMap = new HashMap<>();
        vmIpsMap.put("Test", "127.0.0.1");
        rec.networks = vmIpsMap;

        final GetVmIpAddressCommand getVmIpAddrCmd = new GetVmIpAddressCommand("Test", "127.0.0.1/24", false);

        final CitrixRequestWrapper wrapper = CitrixRequestWrapper.getInstance();
        assertNotNull(wrapper);

        when(xenServerResourceBase.getConnection()).thenReturn(conn);
        when(xenServerResourceBase.getVM(conn, getVmIpAddrCmd.getVmName())).thenReturn(vm);
        when(vm.getGuestMetrics(conn)).thenReturn(mtr);
        when(mtr.getRecord(conn)).thenReturn(rec);

        final Answer answer = wrapper.execute(getVmIpAddrCmd, xenServerResourceBase);

        verify(xenServerResourceBase, times(1)).getConnection();

        assertTrue(answer.getResult());
    }

}

class NotAValidCommand extends Command {

    @Override
    public boolean executeInSequence() {
        return false;
    }

}
