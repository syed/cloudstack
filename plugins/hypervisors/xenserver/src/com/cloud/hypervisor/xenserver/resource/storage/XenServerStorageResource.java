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

package com.cloud.hypervisor.xenserver.resource.storage;

import com.cloud.agent.api.StartupStorageCommand;
import com.cloud.agent.api.StoragePoolInfo;
import com.cloud.agent.api.to.DataStoreTO;
import com.cloud.agent.api.to.DataTO;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.agent.api.to.NfsTO;
import com.cloud.hypervisor.xenserver.resource.common.XenServerHelper;
import com.cloud.hypervisor.xenserver.resource.common.XsHost;
import com.cloud.storage.Storage;
import com.cloud.storage.Volume;
import com.cloud.template.VirtualMachineTemplate;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.ssh.SSHCmdHelper;
import com.trilead.ssh2.SCPClient;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.Host;
import com.xensource.xenapi.PBD;
import com.xensource.xenapi.SR;
import com.xensource.xenapi.Task;
import com.xensource.xenapi.Types;
import com.xensource.xenapi.Types.BadServerResponse;
import com.xensource.xenapi.Types.XenAPIException;
import com.xensource.xenapi.VBD;
import com.xensource.xenapi.VDI;
import com.xensource.xenapi.VM;
import com.xensource.xenapi.XenAPIObject;
import org.apache.cloudstack.storage.to.TemplateObjectTO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.xmlrpc.XmlRpcException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static com.cloud.hypervisor.xenserver.resource.common.XenServerHelper.isRefNull;
import static java.nio.charset.Charset.defaultCharset;

public class XenServerStorageResource {

    private static final Logger s_logger = Logger.getLogger(XenServerStorageResource.class);

    protected String _configDriveSRName = "ConfigDriveISOs";
    protected String _configDriveIsopath = "/opt/xensource/packages/configdrive_iso/";
    protected String _attachIsoDeviceNum = "3";
    protected String _username;
    protected Queue<String> _password = new LinkedList<String>();
    protected XsHost host;
    private long dcId;

    public XenServerStorageResource(XsHost host, long dcId) {
        this.host = host;
        this.dcId = dcId;
    }



    public enum SRType {
        EXT, FILE, ISCSI, ISO, LVM, LVMOHBA, LVMOISCSI,
        /**
         * used for resigning metadata (like SR UUID and VDI UUID when a
         * particular storage manager is installed on a XenServer host (for back-end snapshots to work))
         */
        RELVMOISCSI, NFS;
    }

    protected boolean checkSR(final Connection conn, final SR sr) {
        try {
            final SR.Record srr = sr.getRecord(conn);
            final Set<PBD> pbds = sr.getPBDs(conn);
            if (pbds.size() == 0) {
                final String msg = "There is no PBDs for this SR: " + srr.nameLabel + " on host:" + this.host.getUuid();
                s_logger.warn(msg);
                return false;
            }
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Checking " + srr.nameLabel + " or SR " + srr.uuid + " on " + this.host);
            }
            if (srr.shared) {
                if (SRType.NFS.equals(srr.type)) {
                    final Map<String, String> smConfig = srr.smConfig;
                    if (!smConfig.containsKey("nosubdir")) {
                        smConfig.put("nosubdir", "true");
                        sr.setSmConfig(conn, smConfig);
                    }
                }

                final Host host = Host.getByUuid(conn, this.host.getUuid());
                boolean found = false;
                for (final PBD pbd : pbds) {
                    final PBD.Record pbdr = pbd.getRecord(conn);
                    if (host.equals(pbdr.host)) {
                        if (!pbdr.currentlyAttached) {
                            pbdPlug(conn, pbd, pbdr.uuid);
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    final PBD.Record pbdr = srr.PBDs.iterator().next().getRecord(conn);
                    pbdr.host = host;
                    pbdr.uuid = "";
                    final PBD pbd = PBD.create(conn, pbdr);
                    pbdPlug(conn, pbd, pbd.getUuid(conn));
                }
            } else {
                for (final PBD pbd : pbds) {
                    final PBD.Record pbdr = pbd.getRecord(conn);
                    if (!pbdr.currentlyAttached) {
                        pbdPlug(conn, pbd, pbdr.uuid);
                    }
                }
            }

        } catch (final Exception e) {
            final String msg = "checkSR failed host:" + host + " due to " + e.toString();
            s_logger.warn(msg, e);
            return false;
        }
        return true;
    }

    public void cleanupTemplateSR(final Connection conn) {
        Set<PBD> pbds = null;
        try {
            final Host host = Host.getByUuid(conn, this.host.getUuid());
            pbds = host.getPBDs(conn);
        } catch (final XenAPIException e) {
            s_logger.warn("Unable to get the SRs " + e.toString(), e);
            throw new CloudRuntimeException("Unable to get SRs " + e.toString(), e);
        } catch (final Exception e) {
            throw new CloudRuntimeException("Unable to get SRs " + e.getMessage(), e);
        }
        for (final PBD pbd : pbds) {
            SR sr = null;
            SR.Record srRec = null;
            try {
                sr = pbd.getSR(conn);
                srRec = sr.getRecord(conn);
            } catch (final Exception e) {
                s_logger.warn("pbd.getSR get Exception due to ", e);
                continue;
            }
            final String type = srRec.type;
            if (srRec.shared) {
                continue;
            }
            if (SRType.NFS.equals(type) || SRType.ISO.equals(type) && srRec.nameDescription.contains("template")) {
                try {
                    pbd.unplug(conn);
                    pbd.destroy(conn);
                    sr.forget(conn);
                } catch (final Exception e) {
                    s_logger.warn("forget SR catch Exception due to ", e);
                }
            }
        }
    }

    protected VDI cloudVDIcopy(final Connection conn, final VDI vdi, final SR sr, int wait) throws Exception {
        Task task = null;
        if (wait == 0) {
            wait = 2 * 60 * 60;
        }
        try {
            task = vdi.copyAsync(conn, sr);
            // poll every 1 seconds , timeout after 2 hours
            XenServerHelper.waitForTask(conn, task, 1000, (long) wait * 1000);
            XenServerHelper.checkForSuccess(conn, task);
            final VDI dvdi = Types.toVDI(task, conn);
            return dvdi;
        } finally {
            if (task != null) {
                try {
                    task.destroy(conn);
                } catch (final Exception e) {
                    s_logger.debug("unable to destroy task(" + task.toString() + ") on host(" + host.getUuid() + ") due to " + e.toString());
                }
            }
        }
    }

    public String copyVhdFromSecondaryStorage(final Connection conn, final String mountpoint, final String sruuid, final int wait) {
        final String nameLabel = "cloud-" + UUID.randomUUID().toString();
        final String results = XenServerHelper.callHostPluginAsync(conn, "vmopspremium", "copy_vhd_from_secondarystorage", wait, host, "mountpoint", mountpoint, "sruuid", sruuid, "namelabel",
                nameLabel);
        String errMsg = null;
        if (results == null || results.isEmpty()) {
            errMsg = "copy_vhd_from_secondarystorage return null";
        } else {
            final String[] tmp = results.split("#");
            final String status = tmp[0];
            if (status.equals("0")) {
                return tmp[1];
            } else {
                errMsg = tmp[1];
            }
        }
        final String source = mountpoint.substring(mountpoint.lastIndexOf('/') + 1);
        if (killCopyProcess(conn, source)) {
            destroyVDIbyNameLabel(conn, nameLabel);
        }
        s_logger.warn(errMsg);
        throw new CloudRuntimeException(errMsg);
    }

    protected SR createIsoSRbyURI(final Connection conn, final URI uri, final String vmName, final boolean shared) {
        try {
            final Map<String, String> deviceConfig = new HashMap<String, String>();
            String path = uri.getPath();
            path = path.replace("//", "/");
            deviceConfig.put("location", uri.getHost() + ":" + path);
            final Host host = Host.getByUuid(conn, this.host.getUuid());
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

    protected SR createNfsSRbyURI(final Connection conn, final URI uri, final boolean shared) {
        try {
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Creating a " + (shared ? "shared SR for " : "not shared SR for ") + uri);
            }

            final Map<String, String> deviceConfig = new HashMap<String, String>();
            String path = uri.getPath();
            path = path.replace("//", "/");
            deviceConfig.put("server", uri.getHost());
            deviceConfig.put("serverpath", path);
            final String name = UUID.nameUUIDFromBytes(new String(uri.getHost() + path).getBytes()).toString();
            if (!shared) {
                final Set<SR> srs = SR.getByNameLabel(conn, name);
                for (final SR sr : srs) {
                    final SR.Record record = sr.getRecord(conn);
                    if (SRType.NFS.equals(record.type) && record.contentType.equals("user") && !record.shared) {
                        removeSRSync(conn, sr);
                    }
                }
            }

            final Host host = Host.getByUuid(conn, this.host.getUuid());
            final Map<String, String> smConfig = new HashMap<String, String>();
            smConfig.put("nosubdir", "true");
            final SR sr = SR.create(conn, host, deviceConfig, new Long(0), name, uri.getHost() + uri.getPath(), SRType.NFS.toString(), "user", shared, smConfig);

            if (!checkSR(conn, sr)) {
                throw new Exception("no attached PBD");
            }
            if (s_logger.isDebugEnabled()) {
                s_logger.debug(logX(sr, "Created a SR; UUID is " + sr.getUuid(conn) + " device config is " + deviceConfig));
            }
            sr.scan(conn);
            return sr;
        } catch (final XenAPIException e) {
            final String msg = "Can not create second storage SR mountpoint: " + uri.getHost() + uri.getPath() + " due to " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        } catch (final Exception e) {
            final String msg = "Can not create second storage SR mountpoint: " + uri.getHost() + uri.getPath() + " due to " + e.getMessage();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        }
    }

    public VBD createPatchVbd(final Connection conn, final String vmName, final VM vm) throws XmlRpcException, XenAPIException {

        if (this.host.getSystemvmisouuid() == null) {
            final Set<SR> srs = SR.getByNameLabel(conn, "XenServer Tools");
            if (srs.size() != 1) {
                throw new CloudRuntimeException("There are " + srs.size() + " SRs with name XenServer Tools");
            }
            final SR sr = srs.iterator().next();
            sr.scan(conn);

            final SR.Record srr = sr.getRecord(conn);

            if (host.getSystemvmisouuid() == null) {
                for (final VDI vdi : srr.VDIs) {
                    final VDI.Record vdir = vdi.getRecord(conn);
                    if (vdir.nameLabel.contains("systemvm.iso")) {
                        host.setSystemvmisouuid(vdir.uuid);
                        break;
                    }
                }
            }
            if (host.getSystemvmisouuid() == null) {
                throw new CloudRuntimeException("can not find systemvmiso");
            }
        }

        final VBD.Record cdromVBDR = new VBD.Record();
        cdromVBDR.VM = vm;
        cdromVBDR.empty = true;
        cdromVBDR.bootable = false;
        cdromVBDR.userdevice = "3";
        cdromVBDR.mode = Types.VbdMode.RO;
        cdromVBDR.type = Types.VbdType.CD;
        final VBD cdromVBD = VBD.create(conn, cdromVBDR);
        cdromVBD.insert(conn, VDI.getByUuid(conn, host.getSystemvmisouuid()));

        return cdromVBD;
    }

    public boolean createSecondaryStorageFolder(final Connection conn, final String remoteMountPath, final String newFolder) {
        final String result = XenServerHelper.callHostPlugin(conn, "vmopsSnapshot", "create_secondary_storage_folder", host, "remoteMountPath", remoteMountPath, "newFolder", newFolder);
        return result != null;
    }

    String createTemplateFromSnapshot(final Connection conn, final String templatePath, final String snapshotPath, final int wait) {
        final String tmpltLocalDir = UUID.randomUUID().toString();
        final String results = XenServerHelper.callHostPluginAsync(conn, "vmopspremium", "create_privatetemplate_from_snapshot", wait, host, "templatePath", templatePath, "snapshotPath", snapshotPath,
                "tmpltLocalDir", tmpltLocalDir);
        String errMsg = null;
        if (results == null || results.isEmpty()) {
            errMsg = "create_privatetemplate_from_snapshot return null";
        } else {
            final String[] tmp = results.split("#");
            final String status = tmp[0];
            if (status.equals("0")) {
                return results;
            } else {
                errMsg = "create_privatetemplate_from_snapshot failed due to " + tmp[1];
            }
        }
        final String source = "cloud_mount/" + tmpltLocalDir;
        killCopyProcess(conn, source);
        s_logger.warn(errMsg);
        throw new CloudRuntimeException(errMsg);
    }

    public VBD createVbd(final Connection conn, final DiskTO volume, final String vmName, final VM vm, final VirtualMachineTemplate.BootloaderType bootLoaderType, VDI vdi) throws XmlRpcException,
            XenAPIException {
        final Volume.Type type = volume.getType();

        if (vdi == null) {
            vdi = mount(conn, vmName, volume);
        }

        if (vdi != null) {
            if ("detached".equals(vdi.getNameLabel(conn))) {
                vdi.setNameLabel(conn, vmName + "-DATA");
            }

            final Map<String, String> smConfig = vdi.getSmConfig(conn);
            for (final String key : smConfig.keySet()) {
                if (key.startsWith("host_")) {
                    vdi.removeFromSmConfig(conn, key);
                    break;
                }
            }
        }
        final VBD.Record vbdr = new VBD.Record();
        vbdr.VM = vm;
        if (vdi != null) {
            vbdr.VDI = vdi;
        } else {
            vbdr.empty = true;
        }
        if (type == Volume.Type.ROOT && bootLoaderType == VirtualMachineTemplate.BootloaderType.PyGrub) {
            vbdr.bootable = true;
        } else if (type == Volume.Type.ISO && bootLoaderType == VirtualMachineTemplate.BootloaderType.CD) {
            vbdr.bootable = true;
        }

        if (volume.getType() == Volume.Type.ISO) {
            vbdr.mode = Types.VbdMode.RO;
            vbdr.type = Types.VbdType.CD;
            vbdr.userdevice = "3";
        } else {
            vbdr.mode = Types.VbdMode.RW;
            vbdr.type = Types.VbdType.DISK;
            vbdr.unpluggable = (volume.getType() == Volume.Type.ROOT) ? false : true;
            vbdr.userdevice = "autodetect";
            final Long deviceId = volume.getDiskSeq();
            if (deviceId != null && !isDeviceUsed(conn, vm, deviceId)) {
                vbdr.userdevice = deviceId.toString();
            }
        }
        final VBD vbd = VBD.create(conn, vbdr);

        if (s_logger.isDebugEnabled()) {
            s_logger.debug("VBD " + vbd.getUuid(conn) + " created for " + volume);
        }

        return vbd;
    }

    public VDI createVdi(final Connection conn, final SR sr, final String vdiNameLabel, final Long volumeSize) throws XenAPIException, XmlRpcException {

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

    public boolean deleteSecondaryStorageFolder(final Connection conn, final String remoteMountPath, final String folder) {
        final String details = XenServerHelper.callHostPlugin(conn, "vmopsSnapshot", "delete_secondary_storage_folder", host, "remoteMountPath", remoteMountPath, "folder", folder);
        return details != null && details.equals("1");
    }

    protected String deleteSnapshotBackup(final Connection conn, final Long dcId, final Long accountId, final Long volumeId, final String secondaryStorageMountPath,
                                          final String backupUUID) {

        // If anybody modifies the formatting below again, I'll skin them
        final String result = XenServerHelper.callHostPlugin(conn, "vmopsSnapshot", "deleteSnapshotBackup", host, "backupUUID", backupUUID, "dcId", dcId.toString(), "accountId", accountId.toString(),
                "volumeId", volumeId.toString(), "secondaryStorageMountPath", secondaryStorageMountPath);

        return result;
    }

    public void destroyPatchVbd(final Connection conn, final String vmName) throws XmlRpcException, XenAPIException {
        try {
            if (!vmName.startsWith("r-") && !vmName.startsWith("s-") && !vmName.startsWith("v-")) {
                return;
            }
            final Set<VM> vms = VM.getByNameLabel(conn, vmName);
            for (final VM vm : vms) {
                final Set<VBD> vbds = vm.getVBDs(conn);
                for (final VBD vbd : vbds) {
                    if (vbd.getType(conn) == Types.VbdType.CD) {
                        vbd.eject(conn);
                        vbd.destroy(conn);
                        break;
                    }
                }
            }
        } catch (final Exception e) {
            s_logger.debug("Cannot destory CD-ROM device for VM " + vmName + " due to " + e.toString(), e);
        }
    }


    void destroyVDIbyNameLabel(final Connection conn, final String nameLabel) {
        try {
            final Set<VDI> vdis = VDI.getByNameLabel(conn, nameLabel);
            if (vdis.size() != 1) {
                s_logger.warn("destoryVDIbyNameLabel failed due to there are " + vdis.size() + " VDIs with name " + nameLabel);
                return;
            }
            for (final VDI vdi : vdis) {
                try {
                    vdi.destroy(conn);
                } catch (final Exception e) {
                    final String msg = "Failed to destroy VDI : " + nameLabel + "due to " + e.toString() + "\n Force deleting VDI using system 'rm' command";
                    s_logger.warn(msg);
                    try {
                        final String srUUID = vdi.getSR(conn).getUuid(conn);
                        final String vdiUUID = vdi.getUuid(conn);
                        final String vdifile = "/var/run/sr-mount/" + srUUID + "/" + vdiUUID + ".vhd";
                        XenServerHelper.callHostPluginAsync(conn, "vmopspremium", "remove_corrupt_vdi", 10, host, "vdifile", vdifile);
                    } catch (final Exception e2) {
                        s_logger.warn(e2);
                    }
                }
            }
        } catch (final Exception e) {
        }
    }

    public SR getIscsiSR(final Connection conn, final String srNameLabel, final String target, String path, final String chapInitiatorUsername,
                         final String chapInitiatorPassword, final boolean ignoreIntroduceException) {
        return getIscsiSR(conn, srNameLabel, target, path, chapInitiatorUsername, chapInitiatorPassword, false, ignoreIntroduceException);
    }

    public SR getIscsiSR(final Connection conn, final String srNameLabel, final String target, String path, final String chapInitiatorUsername,
                         final String chapInitiatorPassword, final boolean resignature, final boolean ignoreIntroduceException) {
        synchronized (srNameLabel.intern()) {
            final Map<String, String> deviceConfig = new HashMap<String, String>();
            try {
                if (path.endsWith("/")) {
                    path = path.substring(0, path.length() - 1);
                }

                final String tmp[] = path.split("/");
                if (tmp.length != 3) {
                    final String msg = "Wrong iscsi path " + path + " it should be /targetIQN/LUN";
                    s_logger.warn(msg);
                    throw new CloudRuntimeException(msg);
                }
                final String targetiqn = tmp[1].trim();
                final String lunid = tmp[2].trim();
                String scsiid = "";

                final Set<SR> srs = SR.getByNameLabel(conn, srNameLabel);
                for (final SR sr : srs) {
                    if (!SRType.LVMOISCSI.equals(sr.getType(conn))) {
                        continue;
                    }
                    final Set<PBD> pbds = sr.getPBDs(conn);
                    if (pbds.isEmpty()) {
                        continue;
                    }
                    final PBD pbd = pbds.iterator().next();
                    final Map<String, String> dc = pbd.getDeviceConfig(conn);
                    if (dc == null) {
                        continue;
                    }
                    if (dc.get("target") == null) {
                        continue;
                    }
                    if (dc.get("targetIQN") == null) {
                        continue;
                    }
                    if (dc.get("lunid") == null) {
                        continue;
                    }
                    if (target.equals(dc.get("target")) && targetiqn.equals(dc.get("targetIQN")) && lunid.equals(dc.get("lunid"))) {
                        throw new CloudRuntimeException("There is a SR using the same configuration target:" + dc.get("target") + ",  targetIQN:" + dc.get("targetIQN")
                                + ", lunid:" + dc.get("lunid") + " for pool " + srNameLabel + "on host:" + this.host.getUuid());
                    }
                }
                deviceConfig.put("target", target);
                deviceConfig.put("targetIQN", targetiqn);

                if (StringUtils.isNotBlank(chapInitiatorUsername) && StringUtils.isNotBlank(chapInitiatorPassword)) {
                    deviceConfig.put("chapuser", chapInitiatorUsername);
                    deviceConfig.put("chappassword", chapInitiatorPassword);
                }

                final Host host = Host.getByUuid(conn, this.host.getUuid());
                final Map<String, String> smConfig = new HashMap<String, String>();
                final String type = SRType.LVMOISCSI.toString();
                SR sr = null;
                try {
                    sr = SR.create(conn, host, deviceConfig, new Long(0), srNameLabel, srNameLabel, type, "user", true, smConfig);
                } catch (final XenAPIException e) {
                    final String errmsg = e.toString();
                    if (errmsg.contains("SR_BACKEND_FAILURE_107")) {
                        final String lun[] = errmsg.split("<LUN>");
                        boolean found = false;
                        for (int i = 1; i < lun.length; i++) {
                            final int blunindex = lun[i].indexOf("<LUNid>") + 7;
                            final int elunindex = lun[i].indexOf("</LUNid>");
                            String ilun = lun[i].substring(blunindex, elunindex);
                            ilun = ilun.trim();
                            if (ilun.equals(lunid)) {
                                final int bscsiindex = lun[i].indexOf("<SCSIid>") + 8;
                                final int escsiindex = lun[i].indexOf("</SCSIid>");
                                scsiid = lun[i].substring(bscsiindex, escsiindex);
                                scsiid = scsiid.trim();
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            final String msg = "can not find LUN " + lunid + " in " + errmsg;
                            s_logger.warn(msg);
                            throw new CloudRuntimeException(msg);
                        }
                    } else {
                        final String msg = "Unable to create Iscsi SR  " + deviceConfig + " due to  " + e.toString();
                        s_logger.warn(msg, e);
                        throw new CloudRuntimeException(msg, e);
                    }
                }

                deviceConfig.put("SCSIid", scsiid);

                String result = SR.probe(conn, host, deviceConfig, type, smConfig);

                String pooluuid = null;

                if (result.indexOf("<UUID>") != -1) {
                    pooluuid = result.substring(result.indexOf("<UUID>") + 6, result.indexOf("</UUID>")).trim();
                }

                if (pooluuid == null || pooluuid.length() != 36) {
                    sr = SR.create(conn, host, deviceConfig, new Long(0), srNameLabel, srNameLabel, type, "user", true, smConfig);
                }
                else {
                    if (resignature) {
                        try {
                            SR.create(conn, host, deviceConfig, new Long(0), srNameLabel, srNameLabel, SRType.RELVMOISCSI.toString(), "user", true, smConfig);

                            // The successful outcome of SR.create (right above) is to throw an exception of type XenAPIException (with expected
                            // toString() text) after resigning the metadata (we indicated to perform a resign by passing in SRType.RELVMOISCSI.toString()).
                            // That being the case, if this CloudRuntimeException statement is executed, there appears to have been some kind
                            // of failure in the execution of the above SR.create (resign) method.
                            throw new CloudRuntimeException("Problem resigning the metadata");
                        }
                        catch (XenAPIException ex) {
                            String msg = ex.toString();

                            if (!msg.contains("successfully resigned")) {
                                throw ex;
                            }

                            result = SR.probe(conn, host, deviceConfig, type, smConfig);

                            pooluuid = null;

                            if (result.indexOf("<UUID>") != -1) {
                                pooluuid = result.substring(result.indexOf("<UUID>") + 6, result.indexOf("</UUID>")).trim();
                            }

                            if (pooluuid == null || pooluuid.length() != 36) {
                                throw new CloudRuntimeException("Non-existent or invalid SR UUID");
                            }
                        }
                    }

                    try {
                        sr = SR.introduce(conn, pooluuid, srNameLabel, srNameLabel, type, "user", true, smConfig);
                    } catch (final XenAPIException ex) {
                        if (ignoreIntroduceException) {
                            return sr;
                        }

                        throw ex;
                    }

                    final Set<Host> setHosts = Host.getAll(conn);

                    if (setHosts == null) {
                        final String msg = "Unable to create iSCSI SR " + deviceConfig + " due to hosts not available.";

                        s_logger.warn(msg);

                        throw new CloudRuntimeException(msg);
                    }

                    for (final Host currentHost : setHosts) {
                        final PBD.Record rec = new PBD.Record();

                        rec.deviceConfig = deviceConfig;
                        rec.host = currentHost;
                        rec.SR = sr;

                        final PBD pbd = PBD.create(conn, rec);

                        pbd.plug(conn);
                    }
                }

                sr.scan(conn);

                return sr;
            } catch (final XenAPIException e) {
                final String msg = "Unable to create Iscsi SR  " + deviceConfig + " due to  " + e.toString();
                s_logger.warn(msg, e);
                throw new CloudRuntimeException(msg, e);
            } catch (final Exception e) {
                final String msg = "Unable to create Iscsi SR  " + deviceConfig + " due to  " + e.getMessage();
                s_logger.warn(msg, e);
                throw new CloudRuntimeException(msg, e);
            }
        }
    }

    public SR getISOSRbyVmName(final Connection conn, final String vmName) {
        try {
            final Set<SR> srs = SR.getByNameLabel(conn, vmName + "-ISO");
            if (srs.size() == 0) {
                return null;
            } else if (srs.size() == 1) {
                return srs.iterator().next();
            } else {
                final String msg = "getIsoSRbyVmName failed due to there are more than 1 SR having same Label";
                s_logger.warn(msg);
            }
        } catch (final XenAPIException e) {
            final String msg = "getIsoSRbyVmName failed due to " + e.toString();
            s_logger.warn(msg, e);
        } catch (final Exception e) {
            final String msg = "getIsoSRbyVmName failed due to " + e.getMessage();
            s_logger.warn(msg, e);
        }
        return null;
    }

    public VDI getIsoVDIByURL(final Connection conn, final String vmName, final String isoURL) {
        SR isoSR = null;
        String mountpoint = null;
        if (isoURL.startsWith("xs-tools")) {
            try {
                final Set<VDI> vdis = VDI.getByNameLabel(conn, isoURL);
                if (vdis.isEmpty()) {
                    throw new CloudRuntimeException("Could not find ISO with URL: " + isoURL);
                }
                return vdis.iterator().next();

            } catch (final XenAPIException e) {
                throw new CloudRuntimeException("Unable to get pv iso: " + isoURL + " due to " + e.toString());
            } catch (final Exception e) {
                throw new CloudRuntimeException("Unable to get pv iso: " + isoURL + " due to " + e.toString());
            }
        }

        final int index = isoURL.lastIndexOf("/");
        mountpoint = isoURL.substring(0, index);

        URI uri;
        try {
            uri = new URI(mountpoint);
        } catch (final URISyntaxException e) {
            throw new CloudRuntimeException("isoURL is wrong: " + isoURL);
        }
        isoSR = getISOSRbyVmName(conn, vmName);
        if (isoSR == null) {
            isoSR = createIsoSRbyURI(conn, uri, vmName, false);
        }

        final String isoName = isoURL.substring(index + 1);

        final VDI isoVDI = getVDIbyLocationandSR(conn, isoName, isoSR);

        if (isoVDI != null) {
            return isoVDI;
        } else {
            throw new CloudRuntimeException("Could not find ISO with URL: " + isoURL);
        }
    }

    protected SR getLocalEXTSR(final Connection conn) {
        try {
            final Map<SR, SR.Record> map = SR.getAllRecords(conn);
            if (map != null && !map.isEmpty()) {
                for (final Map.Entry<SR, SR.Record> entry : map.entrySet()) {
                    final SR.Record srRec = entry.getValue();
                    if (SRType.FILE.equals(srRec.type) || SRType.EXT.equals(srRec.type)) {
                        final Set<PBD> pbds = srRec.PBDs;
                        if (pbds == null) {
                            continue;
                        }
                        for (final PBD pbd : pbds) {
                            final Host host = pbd.getHost(conn);
                            if (!isRefNull(host) && host.getUuid(conn).equals(this.host.getUuid())) {
                                if (!pbd.getCurrentlyAttached(conn)) {
                                    pbd.plug(conn);
                                }
                                final SR sr = entry.getKey();
                                sr.scan(conn);
                                return sr;
                            }
                        }
                    }
                }
            }
        } catch (final XenAPIException e) {
            final String msg = "Unable to get local EXTSR in host:" + this.host.getUuid() + e.toString();
            s_logger.warn(msg);
        } catch (final XmlRpcException e) {
            final String msg = "Unable to get local EXTSR in host:" + this.host.getUuid() + e.getCause();
            s_logger.warn(msg);
        }
        return null;
    }

    public StartupStorageCommand initializeLocalSR(final Connection conn) {
        final SR lvmsr = getLocalLVMSR(conn);
        if (lvmsr != null) {
            try {
                this.host.setLocalSRuuid(lvmsr.getUuid(conn));

                final String lvmuuid = lvmsr.getUuid(conn);
                final long cap = lvmsr.getPhysicalSize(conn);
                if (cap > 0) {
                    final long avail = cap - lvmsr.getPhysicalUtilisation(conn);
                    lvmsr.setNameLabel(conn, lvmuuid);
                    final String name = "Cloud Stack Local LVM Storage Pool for " + this.host.getUuid();
                    lvmsr.setNameDescription(conn, name);
                    final Host host = Host.getByUuid(conn, this.host.getUuid());
                    final String address = host.getAddress(conn);
                    final StoragePoolInfo pInfo = new StoragePoolInfo(lvmuuid, address, SRType.LVM.toString(), SRType.LVM.toString(), Storage.StoragePoolType.LVM, cap, avail);
                    final StartupStorageCommand cmd = new StartupStorageCommand();
                    cmd.setPoolInfo(pInfo);
                    cmd.setGuid(this.host.getUuid());
                    cmd.setDataCenter(Long.toString(this.dcId));
                    cmd.setResourceType(Storage.StorageResourceType.STORAGE_POOL);
                    return cmd;
                }
            } catch (final XenAPIException e) {
                final String msg = "build local LVM info err in host:" + this.host.getUuid() + e.toString();
                s_logger.warn(msg);
            } catch (final XmlRpcException e) {
                final String msg = "build local LVM info err in host:" + this.host.getUuid() + e.getMessage();
                s_logger.warn(msg);
            }
        }

        final SR extsr = getLocalEXTSR(conn);
        if (extsr != null) {
            try {
                final String extuuid = extsr.getUuid(conn);
                this.host.setLocalSRuuid(extuuid);
                final long cap = extsr.getPhysicalSize(conn);
                if (cap > 0) {
                    final long avail = cap - extsr.getPhysicalUtilisation(conn);
                    extsr.setNameLabel(conn, extuuid);
                    final String name = "Cloud Stack Local EXT Storage Pool for " + this.host.getUuid();
                    extsr.setNameDescription(conn, name);
                    final Host host = Host.getByUuid(conn, this.host.getUuid());
                    final String address = host.getAddress(conn);
                    final StoragePoolInfo pInfo = new StoragePoolInfo(extuuid, address, SRType.EXT.toString(), SRType.EXT.toString(), Storage.StoragePoolType.EXT, cap, avail);
                    final StartupStorageCommand cmd = new StartupStorageCommand();
                    cmd.setPoolInfo(pInfo);
                    cmd.setGuid(this.host.getUuid());
                    cmd.setDataCenter(Long.toString(this.dcId));
                    cmd.setResourceType(Storage.StorageResourceType.STORAGE_POOL);
                    return cmd;
                }
            } catch (final XenAPIException e) {
                final String msg = "build local EXT info err in host:" + this.host.getUuid() + e.toString();
                s_logger.warn(msg);
            } catch (final XmlRpcException e) {
                final String msg = "build local EXT info err in host:" + this.host.getUuid() + e.getMessage();
                s_logger.warn(msg);
            }
        }
        return null;
    }

    protected SR getLocalLVMSR(final Connection conn) {
        try {
            final Map<SR, SR.Record> map = SR.getAllRecords(conn);
            if (map != null && !map.isEmpty()) {
                for (final Map.Entry<SR, SR.Record> entry : map.entrySet()) {
                    final SR.Record srRec = entry.getValue();
                    if (SRType.LVM.equals(srRec.type)) {
                        final Set<PBD> pbds = srRec.PBDs;
                        if (pbds == null) {
                            continue;
                        }
                        for (final PBD pbd : pbds) {
                            final Host host = pbd.getHost(conn);
                            if (!isRefNull(host) && host.getUuid(conn).equals(this.host.getUuid())) {
                                if (!pbd.getCurrentlyAttached(conn)) {
                                    pbd.plug(conn);
                                }
                                final SR sr = entry.getKey();
                                sr.scan(conn);
                                return sr;
                            }
                        }
                    }
                }
            }
        } catch (final XenAPIException e) {
            final String msg = "Unable to get local LVMSR in host:" + this.host.getUuid() + e.toString();
            s_logger.warn(msg);
        } catch (final XmlRpcException e) {
            final String msg = "Unable to get local LVMSR in host:" + this.host.getUuid() + e.getCause();
            s_logger.warn(msg);
        }
        return null;
    }

    public SR getNfsSR(final Connection conn, final String poolid, final String uuid, final String server, String serverpath, final String pooldesc) {
        final Map<String, String> deviceConfig = new HashMap<String, String>();
        try {
            serverpath = serverpath.replace("//", "/");
            final Set<SR> srs = SR.getAll(conn);
            if (srs != null && !srs.isEmpty()) {
                for (final SR sr : srs) {
                    if (!SRType.NFS.equals(sr.getType(conn))) {
                        continue;
                    }

                    final Set<PBD> pbds = sr.getPBDs(conn);
                    if (pbds.isEmpty()) {
                        continue;
                    }

                    final PBD pbd = pbds.iterator().next();

                    final Map<String, String> dc = pbd.getDeviceConfig(conn);

                    if (dc == null) {
                        continue;
                    }

                    if (dc.get("server") == null) {
                        continue;
                    }

                    if (dc.get("serverpath") == null) {
                        continue;
                    }

                    if (server.equals(dc.get("server")) && serverpath.equals(dc.get("serverpath"))) {
                        throw new CloudRuntimeException("There is a SR using the same configuration server:" + dc.get("server") + ", serverpath:" + dc.get("serverpath")
                                + " for pool " + uuid + " on host:" + this.host.getUuid());
                    }

                }
            }
            deviceConfig.put("server", server);
            deviceConfig.put("serverpath", serverpath);
            final Host host = Host.getByUuid(conn, this.host.getUuid());
            final Map<String, String> smConfig = new HashMap<String, String>();
            smConfig.put("nosubdir", "true");
            final SR sr = SR.create(conn, host, deviceConfig, new Long(0), uuid, poolid, SRType.NFS.toString(), "user", true, smConfig);
            sr.scan(conn);
            return sr;
        } catch (final XenAPIException e) {
            throw new CloudRuntimeException("Unable to create NFS SR " + pooldesc, e);
        } catch (final XmlRpcException e) {
            throw new CloudRuntimeException("Unable to create NFS SR " + pooldesc, e);
        }
    }


    protected SR getSRByNameLabelandHost(final Connection conn, final String name) throws BadServerResponse, XenAPIException, XmlRpcException {
        final Set<SR> srs = SR.getByNameLabel(conn, name);
        SR ressr = null;
        for (final SR sr : srs) {
            Set<PBD> pbds;
            pbds = sr.getPBDs(conn);
            for (final PBD pbd : pbds) {
                final PBD.Record pbdr = pbd.getRecord(conn);
                if (pbdr.host != null && pbdr.host.getUuid(conn).equals(host.getUuid())) {
                    if (!pbdr.currentlyAttached) {
                        pbd.plug(conn);
                    }
                    ressr = sr;
                    break;
                }
            }
        }
        return ressr;
    }

        public SR getStorageRepository(final Connection conn, final String srNameLabel) {
        Set<SR> srs;
        try {
            srs = SR.getByNameLabel(conn, srNameLabel);
        } catch (final XenAPIException e) {
            throw new CloudRuntimeException("Unable to get SR " + srNameLabel + " due to " + e.toString(), e);
        } catch (final Exception e) {
            throw new CloudRuntimeException("Unable to get SR " + srNameLabel + " due to " + e.getMessage(), e);
        }

        if (srs.size() > 1) {
            throw new CloudRuntimeException("More than one storage repository was found for pool with uuid: " + srNameLabel);
        } else if (srs.size() == 1) {
            final SR sr = srs.iterator().next();
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("SR retrieved for " + srNameLabel);
            }

            if (checkSR(conn, sr)) {
                return sr;
            }
            throw new CloudRuntimeException("SR check failed for storage pool: " + srNameLabel + "on host:" + host.getUuid());
        } else {
            throw new CloudRuntimeException("Can not see storage pool: " + srNameLabel + " from on host:" + host.getUuid());
        }
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
        final String parentUuid = XenServerHelper.callHostPlugin(conn, "vmopsSnapshot", "getVhdParent", host, "primaryStorageSRUuid", primaryStorageSRUuid, "snapshotUuid", snapshotUuid, "isISCSI",
                isISCSI.toString());

        if (parentUuid == null || parentUuid.isEmpty() || parentUuid.equalsIgnoreCase("None")) {
            s_logger.debug("Unable to get parent of VHD " + snapshotUuid + " in SR " + primaryStorageSRUuid);
            // errString is already logged.
            return null;
        }
        return parentUuid;
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



    public void handleSrAndVdiDetach(final String iqn, final Connection conn) throws Exception {
        final SR sr = getStorageRepository(conn, iqn);

        removeSR(conn, sr);
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


    public boolean IsISCSI(final String type) {
        return SRType.LVMOHBA.equals(type) || SRType.LVMOISCSI.equals(type) || SRType.LVM.equals(type);
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


    protected VDI mount(final Connection conn, final Storage.StoragePoolType poolType, final String volumeFolder, final String volumePath) {
        return getVDIbyUuid(conn, volumePath);
    }

    protected VDI mount(final Connection conn, final String vmName, final DiskTO volume) throws XmlRpcException, XenAPIException {
        final DataTO data = volume.getData();
        final Volume.Type type = volume.getType();
        if (type == Volume.Type.ISO) {
            final TemplateObjectTO iso = (TemplateObjectTO) data;
            final DataStoreTO store = iso.getDataStore();

            if (store == null) {
                // It's a fake iso
                return null;
            }

            // corer case, xenserver pv driver iso
            final String templateName = iso.getName();
            if (templateName.startsWith("xs-tools")) {
                try {
                    final Set<VDI> vdis = VDI.getByNameLabel(conn, templateName);
                    if (vdis.isEmpty()) {
                        throw new CloudRuntimeException("Could not find ISO with URL: " + templateName);
                    }
                    return vdis.iterator().next();
                } catch (final XenAPIException e) {
                    throw new CloudRuntimeException("Unable to get pv iso: " + templateName + " due to " + e.toString());
                } catch (final Exception e) {
                    throw new CloudRuntimeException("Unable to get pv iso: " + templateName + " due to " + e.toString());
                }
            }

            if (!(store instanceof NfsTO)) {
                throw new CloudRuntimeException("only support mount iso on nfs");
            }
            final NfsTO nfsStore = (NfsTO) store;
            final String isoPath = nfsStore.getUrl() + File.separator + iso.getPath();
            final int index = isoPath.lastIndexOf("/");

            final String mountpoint = isoPath.substring(0, index);
            URI uri;
            try {
                uri = new URI(mountpoint);
            } catch (final URISyntaxException e) {
                throw new CloudRuntimeException("Incorrect uri " + mountpoint, e);
            }
            final SR isoSr = createIsoSRbyURI(conn, uri, vmName, false);

            final String isoname = isoPath.substring(index + 1);

            final VDI isoVdi = getVDIbyLocationandSR(conn, isoname, isoSr);

            if (isoVdi == null) {
                throw new CloudRuntimeException("Unable to find ISO " + isoPath);
            }
            return isoVdi;
        } else {
            final VolumeObjectTO vol = (VolumeObjectTO) data;
            return VDI.getByUuid(conn, vol.getPath());
        }
    }


    private void pbdPlug(final Connection conn, final PBD pbd, final String uuid) {
        try {
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Plugging in PBD " + uuid + " for " + host);
            }
            pbd.plug(conn);
        } catch (final Exception e) {
            final String msg = "PBD " + uuid + " is not attached! and PBD plug failed due to " + e.toString() + ". Please check this PBD in " + host;
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg);
        }
    }

        public void prepareISO(final Connection conn, final String vmName,  List<String[]> vmDataList, String configDriveLabel) throws XmlRpcException, XenAPIException {

        final Set<VM> vms = VM.getByNameLabel(conn, vmName);
        if (vms == null || vms.size() != 1) {
            throw new CloudRuntimeException("There are " + (vms == null ? "0" : vms.size()) + " VMs named " + vmName);
        }
        final VM vm = vms.iterator().next();

        if (vmDataList != null) {
            // create SR
            SR sr =  createLocalIsoSR(conn, _configDriveSRName + host.getIp());

            // 1. create vm data files
            createVmdataFiles(vmName, vmDataList, configDriveLabel);

            // 2. copy config drive iso to host
            copyConfigDriveIsoToHost(conn, sr, vmName);
        }

        final Set<VBD> vbds = vm.getVBDs(conn);
        for (final VBD vbd : vbds) {
            final VBD.Record vbdr = vbd.getRecord(conn);
            if (vbdr.type == Types.VbdType.CD && vbdr.empty == false && vbdr.userdevice.equals(_attachIsoDeviceNum)) {
                final VDI vdi = vbdr.VDI;
                final SR sr = vdi.getSR(conn);
                final Set<PBD> pbds = sr.getPBDs(conn);
                if (pbds == null) {
                    throw new CloudRuntimeException("There is no pbd for sr " + sr);
                }
                for (final PBD pbd : pbds) {
                    final PBD.Record pbdr = pbd.getRecord(conn);
                    if (pbdr.host.getUuid(conn).equals(host.getUuid())) {
                        return;
                    }
                }
                sr.setShared(conn, true);
                final Host host = Host.getByUuid(conn, this.host.getUuid());
                final PBD.Record pbdr = pbds.iterator().next().getRecord(conn);
                pbdr.host = host;
                pbdr.uuid = "";
                final PBD pbd = PBD.create(conn, pbdr);
                pbdPlug(conn, pbd, pbd.getUuid(conn));
                break;
            }
        }
    }

    // The idea here is to see if the DiskTO in question is from managed storage and does not yet have an SR.
    // If no SR, create it and create a VDI in it.
    public VDI prepareManagedDisk(final Connection conn, final DiskTO disk, final long vmId, final String vmName) throws Exception {
        final Map<String, String> details = disk.getDetails();

        if (details == null) {
            return null;
        }

        final boolean isManaged = new Boolean(details.get(DiskTO.MANAGED)).booleanValue();

        if (!isManaged) {
            return null;
        }

        final String iqn = details.get(DiskTO.IQN);

        final Set<SR> srNameLabels = SR.getByNameLabel(conn, iqn);

        if (srNameLabels.size() != 0) {
            return null;
        }

        final String vdiNameLabel = Volume.Type.ROOT.equals(disk.getType()) ? ("ROOT-" + vmId) : (vmName + "-DATA");

        return prepareManagedStorage(conn, details, null, vdiNameLabel);
    }

    protected SR prepareManagedSr(final Connection conn, final Map<String, String> details) {
        final String iScsiName = details.get(DiskTO.IQN);
        final String storageHost = details.get(DiskTO.STORAGE_HOST);
        final String chapInitiatorUsername = details.get(DiskTO.CHAP_INITIATOR_USERNAME);
        final String chapInitiatorSecret = details.get(DiskTO.CHAP_INITIATOR_SECRET);
        final String mountpoint = details.get(DiskTO.MOUNT_POINT);
        final String protocoltype = details.get(DiskTO.PROTOCOL_TYPE);

        if (Storage.StoragePoolType.NetworkFilesystem.toString().equalsIgnoreCase(protocoltype)) {
            final String poolid = storageHost + ":" + mountpoint;
            final String namelable = mountpoint;
            final String volumedesc = storageHost + ":" + mountpoint;

            return getNfsSR(conn, poolid, namelable, storageHost, mountpoint, volumedesc);
        } else {
            return getIscsiSR(conn, iScsiName, storageHost, iScsiName, chapInitiatorUsername, chapInitiatorSecret, true);
        }
    }

    protected VDI prepareManagedStorage(final Connection conn, final Map<String, String> details, final String path, final String vdiNameLabel) throws Exception {
        final SR sr = prepareManagedSr(conn, details);

        VDI vdi = getVDIbyUuid(conn, path, false);
        final Long volumeSize = Long.parseLong(details.get(DiskTO.VOLUME_SIZE));

        Set<VDI> vdisInSr = sr.getVDIs(conn);

        // If a VDI already exists in the SR (in case we cloned from a template cache), use that.
        if (vdisInSr.size() == 1) {
            vdi = vdisInSr.iterator().next();
        }

        if (vdi == null) {
            vdi = createVdi(conn, sr, vdiNameLabel, volumeSize);
        } else {
            // If vdi is not null, it must have already been created, so check whether a resize of the volume was performed.
            // If true, resize the VDI to the volume size.

            s_logger.info("Checking for the resize of the datadisk");

            final long vdiVirtualSize = vdi.getVirtualSize(conn);

            if (vdiVirtualSize != volumeSize) {
                s_logger.info("Resizing the data disk (VDI) from vdiVirtualSize: " + vdiVirtualSize + " to volumeSize: " + volumeSize);

                try {
                    vdi.resize(conn, volumeSize);
                } catch (final Exception e) {
                    s_logger.warn("Unable to resize volume", e);
                }
            }

            // change the name-label in case of a cloned VDI
            if (!Objects.equals(vdi.getNameLabel(conn), vdiNameLabel)) {
                try {
                    vdi.setNameLabel(conn, vdiNameLabel);
                } catch (final Exception e) {
                    s_logger.warn("Unable to rename volume", e);
                }
            }
        }

        return vdi;
    }

        public void removeSR(final Connection conn, final SR sr) {
        if (sr == null) {
            return;
        }

        if (s_logger.isDebugEnabled()) {
            s_logger.debug(logX(sr, "Removing SR"));
        }

        for (int i = 0; i < 2; i++) {
            try {
                final Set<VDI> vdis = sr.getVDIs(conn);
                for (final VDI vdi : vdis) {
                    vdi.forget(conn);
                }

                Set<PBD> pbds = sr.getPBDs(conn);
                for (final PBD pbd : pbds) {
                    if (s_logger.isDebugEnabled()) {
                        s_logger.debug(logX(pbd, "Unplugging pbd"));
                    }

                    // if (pbd.getCurrentlyAttached(conn)) {
                    pbd.unplug(conn);
                    // }

                    pbd.destroy(conn);
                }

                pbds = sr.getPBDs(conn);

                if (pbds.size() == 0) {
                    if (s_logger.isDebugEnabled()) {
                        s_logger.debug(logX(sr, "Forgetting"));
                    }

                    sr.forget(conn);

                    return;
                }

                if (s_logger.isDebugEnabled()) {
                    s_logger.debug(logX(sr, "There is still one or more PBDs attached."));

                    if (s_logger.isTraceEnabled()) {
                        for (final PBD pbd : pbds) {
                            s_logger.trace(logX(pbd, " Still attached"));
                        }
                    }
                }
            } catch (final XenAPIException e) {
                s_logger.debug(logX(sr, "Catch XenAPIException: " + e.toString()));
            } catch (final XmlRpcException e) {
                s_logger.debug(logX(sr, "Catch Exception: " + e.getMessage()));
            }
        }

        s_logger.warn(logX(sr, "Unable to remove SR"));
    }

    protected String removeSRSync(final Connection conn, final SR sr) {
        if (sr == null) {
            return null;
        }
        if (s_logger.isDebugEnabled()) {
            s_logger.debug(logX(sr, "Removing SR"));
        }
        long waittime = 0;
        try {
            final Set<VDI> vdis = sr.getVDIs(conn);
            for (final VDI vdi : vdis) {
                final Map<java.lang.String, Types.VdiOperations> currentOperation = vdi.getCurrentOperations(conn);
                if (currentOperation == null || currentOperation.size() == 0) {
                    continue;
                }
                if (waittime >= 1800000) {
                    final String msg = "This template is being used, try late time";
                    s_logger.warn(msg);
                    return msg;
                }
                waittime += 30000;
                try {
                    Thread.sleep(30000);
                } catch (final InterruptedException ex) {
                }
            }
            removeSR(conn, sr);
            return null;
        } catch (final XenAPIException e) {
            s_logger.warn(logX(sr, "Unable to get current opertions " + e.toString()), e);
        } catch (final XmlRpcException e) {
            s_logger.warn(logX(sr, "Unable to get current opertions " + e.getMessage()), e);
        }
        final String msg = "Remove SR failed";
        s_logger.warn(msg);
        return msg;

    }

    public String revertToSnapshot(final Connection conn, final VM vmSnapshot, final String vmName, final String oldVmUuid, final Boolean snapshotMemory, final String hostUUID)
            throws XenAPIException, XmlRpcException {

        final String results = XenServerHelper.callHostPluginAsync(conn, "vmopsSnapshot", "revert_memory_snapshot", 10 * 60 * 1000, host, "snapshotUUID", vmSnapshot.getUuid(conn), "vmName", vmName,
                "oldVmUuid", oldVmUuid, "snapshotMemory", snapshotMemory.toString(), "hostUUID", hostUUID);
        String errMsg = null;
        if (results == null || results.isEmpty()) {
            errMsg = "revert_memory_snapshot return null";
        } else {
            if (results.equals("0")) {
                return results;
            } else {
                errMsg = "revert_memory_snapshot exception";
            }
        }
        s_logger.warn(errMsg);
        throw new CloudRuntimeException(errMsg);
    }

        protected void umount(final Connection conn, final VDI vdi) {

    }

    public void umountSnapshotDir(final Connection conn, final Long dcId) {
        try {
            XenServerHelper.callHostPlugin(conn, "vmopsSnapshot", "unmountSnapshotsDir", host, "dcId", dcId.toString());
        } catch (final Exception e) {
            s_logger.debug("Failed to umount snapshot dir", e);
        }
    }

    public String upgradeSnapshot(final Connection conn, final String templatePath, final String snapshotPath) {
        final String results = XenServerHelper.callHostPluginAsync(conn, "vmopspremium", "upgrade_snapshot", 2 * 60 * 60, host, "templatePath", templatePath, "snapshotPath", snapshotPath);

        if (results == null || results.isEmpty()) {
            final String msg = "upgrade_snapshot return null";
            s_logger.warn(msg);
            throw new CloudRuntimeException(msg);
        }
        final String[] tmp = results.split("#");
        final String status = tmp[0];
        if (status.equals("0")) {
            return results;
        } else {
            s_logger.warn(results);
            throw new CloudRuntimeException(results);
        }
    }


    public boolean createAndAttachConfigDriveIsoForVM(final Connection conn, final VM vm, final List<String[]> vmDataList, final String configDriveLabel) throws XenAPIException, XmlRpcException {

        final String vmName = vm.getNameLabel(conn);

        // create SR
        final SR sr =  createLocalIsoSR(conn, _configDriveSRName+host.getIp());
        if (sr == null) {
            s_logger.debug("Failed to create local SR for the config drive");
            return false;
        }

        s_logger.debug("Creating vm data files in config drive for vm "+vmName);
        // 1. create vm data files
        if (!createVmdataFiles(vmName, vmDataList, configDriveLabel)) {
            s_logger.debug("Failed to create vm data files in config drive for vm "+vmName);
            return false;
        }

        // 2. copy config drive iso to host
        if (!copyConfigDriveIsoToHost(conn, sr, vmName)) {
            return false;
        }

        // 3. attachIsoToVM
        if (!attachConfigDriveIsoToVm(conn, vm)) {
            return false;
        }

        return true;
    }

        public boolean copyConfigDriveIsoToHost(final Connection conn, final SR sr, final String vmName) {

        final String vmIso = "/tmp/"+vmName+"/configDrive/"+vmName+".iso";
        //scp file into the host
        final com.trilead.ssh2.Connection sshConnection = new com.trilead.ssh2.Connection(host.getIp(), 22);

        try {
            sshConnection.connect(null, 60000, 60000);
            if (!sshConnection.authenticateWithPassword(_username, _password.peek())) {
                throw new CloudRuntimeException("Unable to authenticate");
            }

            s_logger.debug("scp config drive iso file "+vmIso +" to host " + host.getIp() +" path "+_configDriveIsopath);
            final SCPClient scp = new SCPClient(sshConnection);
            final String p = "0755";

            scp.put(vmIso, _configDriveIsopath, p);
            sr.scan(conn);
            s_logger.debug("copied config drive iso to host " + host);
        } catch (final IOException e) {
            s_logger.debug("failed to copy configdrive iso " + vmIso + " to host " + host, e);
            return false;
        } catch (final XmlRpcException e) {
            s_logger.debug("Failed to scan config drive iso SR "+ _configDriveSRName+host.getIp() + " in host "+ this.host, e);
            return false;
        } finally {
            sshConnection.close();
            //clean up the config drive files

            final String configDir = "/tmp/"+vmName;
            try {
                deleteLocalFolder(configDir);
                s_logger.debug("Successfully cleaned up config drive directory " + configDir
                        + " after copying it to host ");
            } catch (final Exception e) {
                s_logger.debug("Failed to delete config drive folder :" + configDir + " for VM " + vmName + " "
                        + e.getMessage());
            }
        }

        return true;
    }

    public boolean attachConfigDriveIsoToVm(final Connection conn, final VM vm) throws XenAPIException, XmlRpcException {

        final String vmName = vm.getNameLabel(conn);
        final String isoURL = _configDriveIsopath + vmName+".iso";
        VDI srVdi;

        //1. find the vdi of the iso
        //2. find the vbd for the vdi
        //3. attach iso to vm

        try {
            final Set<VDI> vdis = VDI.getByNameLabel(conn, vmName+".iso");
            if (vdis.isEmpty()) {
                throw new CloudRuntimeException("Could not find ISO with URL: " + isoURL);
            }
            srVdi =  vdis.iterator().next();

        } catch (final XenAPIException e) {
            s_logger.debug("Unable to get config drive iso: " + isoURL + " due to " + e.toString());
            return false;
        } catch (final Exception e) {
            s_logger.debug("Unable to get config drive iso: " + isoURL + " due to " + e.toString());
            return false;
        }

        VBD isoVBD = null;

        // Find the VM's CD-ROM VBD
        final Set<VBD> vbds = vm.getVBDs(conn);
        for (final VBD vbd : vbds) {
            final Types.VbdType type = vbd.getType(conn);

            final VBD.Record vbdr = vbd.getRecord(conn);

            // if the device exists then attach it
            if (!vbdr.userdevice.equals(_attachIsoDeviceNum) && type == Types.VbdType.CD) {
                isoVBD = vbd;
                break;
            }
        }

        if (isoVBD == null) {
            //create vbd
            final VBD.Record cfgDriveVbdr = new VBD.Record();
            cfgDriveVbdr.VM = vm;
            cfgDriveVbdr.empty = true;
            cfgDriveVbdr.bootable = false;
            cfgDriveVbdr.userdevice = "autodetect";
            cfgDriveVbdr.mode = Types.VbdMode.RO;
            cfgDriveVbdr.type = Types.VbdType.CD;
            final VBD cfgDriveVBD = VBD.create(conn, cfgDriveVbdr);
            isoVBD = cfgDriveVBD;

            s_logger.debug("Created CD-ROM VBD for VM: " + vm);
        }

        if (isoVBD != null) {
            // If an ISO is already inserted, eject it
            if (isoVBD.getEmpty(conn) == false) {
                isoVBD.eject(conn);
            }

            try {
                // Insert the new ISO
                isoVBD.insert(conn, srVdi);
                s_logger.debug("Attached config drive iso to vm " + vmName);
            }catch (final XmlRpcException ex) {
                s_logger.debug("Failed to attach config drive iso to vm " + vmName);
                return false;
            }
        }

        return true;
    }

    public SR createLocalIsoSR(final Connection conn, final String srName) throws XenAPIException, XmlRpcException {

        // if config drive sr already exists then return
        SR sr = getSRByNameLabelandHost(conn, _configDriveSRName+host.getIp());

        if (sr != null) {
            s_logger.debug("Config drive SR already exist, returing it");
            return sr;
        }

        try{
            final Map<String, String> deviceConfig = new HashMap<String, String>();

            final com.trilead.ssh2.Connection sshConnection = new com.trilead.ssh2.Connection(host.getIp(), 22);
            try {
                sshConnection.connect(null, 60000, 60000);
                if (!sshConnection.authenticateWithPassword(_username, _password.peek())) {
                    throw new CloudRuntimeException("Unable to authenticate");
                }

                final String cmd = "mkdir -p " + _configDriveIsopath;
                if (!SSHCmdHelper.sshExecuteCmd(sshConnection, cmd)) {
                    throw new CloudRuntimeException("Cannot create directory configdrive_iso on XenServer hosts");
                }
            } catch (final IOException e) {
                throw new CloudRuntimeException("Unable to create iso folder", e);
            } finally {
                sshConnection.close();
            }
            s_logger.debug("Created the config drive SR " + srName +" folder path "+ _configDriveIsopath);

            deviceConfig.put("location",  _configDriveIsopath);
            deviceConfig.put("legacy_mode", "true");
            final Host host = Host.getByUuid(conn, this.host.getUuid());
            final String type = SRType.ISO.toString();
            sr = SR.create(conn, host, deviceConfig, new Long(0),  _configDriveIsopath, "iso", type, "iso", false, new HashMap<String, String>());

            sr.setNameLabel(conn, srName);
            sr.setNameDescription(conn, deviceConfig.get("location"));

            sr.scan(conn);
            s_logger.debug("Config drive ISO SR at the path " + _configDriveIsopath  +" got created in host " + this.host);
            return sr;
        } catch (final XenAPIException e) {
            final String msg = "createLocalIsoSR failed! mountpoint " + e.toString();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        } catch (final Exception e) {
            final String msg = "createLocalIsoSR failed! mountpoint:  due to " + e.getMessage();
            s_logger.warn(msg, e);
            throw new CloudRuntimeException(msg, e);
        }

    }

    public void deleteLocalFolder(final String directory) throws Exception {
        if (directory == null || directory.isEmpty()) {
            final String msg = "Invalid directory path (null/empty) detected. Cannot delete specified directory.";
            s_logger.debug(msg);
            throw new Exception(msg);
        }

        try {
            FileUtils.deleteDirectory(new File(directory));
        } catch (final IOException e) {
            // IOException here means failure to delete. Not swallowing it here to
            // let the caller handle with appropriate contextual log message.
            throw e;
        }
    }

    protected SR getSRByNameLabel(Connection conn, String name) throws BadServerResponse, XenAPIException, XmlRpcException {
        Set<SR> srs = SR.getByNameLabel(conn, name);
        SR ressr = null;
        for (SR sr : srs) {
            Set<PBD> pbds;
            pbds = sr.getPBDs(conn);
            for (PBD pbd : pbds) {
                PBD.Record pbdr = pbd.getRecord(conn);
                if (pbdr.host != null) {
                    ressr = sr;
                    break;
                }
            }
        }
        return ressr;
    }


    public boolean attachConfigDriveToMigratedVm(Connection conn, String vmName, String ipAddr) {

        // attach the config drive in destination host

        try {
            s_logger.debug("Attaching config drive iso device for the VM "+ vmName + " In host "+ ipAddr);
            Set<VM> vms = VM.getByNameLabel(conn, vmName);

            SR sr = getSRByNameLabel(conn, _configDriveSRName + ipAddr);
            //Here you will find only two vdis with the <vmname>.iso.
            //one is from source host and second from dest host
            Set<VDI> vdis = VDI.getByNameLabel(conn, vmName + ".iso");
            if (vdis.isEmpty()) {
                s_logger.debug("Could not find config drive ISO: " + vmName);
                return false;
            }

            VDI configdriveVdi = null;
            for (VDI vdi : vdis) {
                SR vdiSr = vdi.getSR(conn);
                if (vdiSr.getUuid(conn).equals(sr.getUuid(conn))) {
                    //get this vdi to attach to vbd
                    configdriveVdi = vdi;
                    s_logger.debug("VDI for the config drive ISO  " + vdi);
                } else {
                    // delete the vdi in source host so that the <vmname>.iso file is get removed
                    s_logger.debug("Removing the source host VDI for the config drive ISO  " + vdi);
                    vdi.destroy(conn);
                }
            }

            if (configdriveVdi == null) {
                s_logger.debug("Config drive ISO VDI is not found ");
                return false;
            }

            for (VM vm : vms) {

                //create vbd
                VBD.Record cfgDriveVbdr = new VBD.Record();
                cfgDriveVbdr.VM = vm;
                cfgDriveVbdr.empty = true;
                cfgDriveVbdr.bootable = false;
                cfgDriveVbdr.userdevice = "autodetect";
                cfgDriveVbdr.mode = Types.VbdMode.RO;
                cfgDriveVbdr.type = Types.VbdType.CD;

                VBD cfgDriveVBD = VBD.create(conn, cfgDriveVbdr);

                s_logger.debug("Inserting vbd " + configdriveVdi);
                cfgDriveVBD.insert(conn, configdriveVdi);
                break;

            }

            return true;

        } catch (BadServerResponse e) {
            s_logger.warn("Failed to attach config drive ISO to the VM  "+ vmName + " In host " + ipAddr + " due to a bad server response.", e);
            return false;
        } catch (XenAPIException e) {
            s_logger.warn("Failed to attach config drive ISO to the VM  "+ vmName + " In host " + ipAddr + " due to a xapi problem.", e);
            return false;
        } catch (XmlRpcException e) {
            s_logger.warn("Failed to attach config drive ISO to the VM  "+ vmName + " In host " + ipAddr + " due to a problem in a remote call.", e);
            return false;
        }

    }

    boolean killCopyProcess(final Connection conn, final String nameLabel) {
        final String results = XenServerHelper.callHostPluginAsync(conn, "vmops", "kill_copy_process", 60, host, "namelabel", nameLabel);
        String errMsg = null;
        if (results == null || results.equals("false")) {
            errMsg = "kill_copy_process failed";
            s_logger.warn(errMsg);
            return false;
        } else {
            return true;
        }
    }

    public boolean createVmdataFiles(final String vmName, final List<String[]> vmDataList, final String configDriveLabel) {

        // add vm iso to the isolibrary
        final String isoPath = "/tmp/"+vmName+"/configDrive/";
        final String configDriveName = "cloudstack/";

        //create folder for the VM
        //Remove the folder before creating it.

        try {
            deleteLocalFolder("/tmp/"+isoPath);
        } catch (final IOException e) {
            s_logger.debug("Failed to delete the exiting config drive for vm "+vmName+ " "+ e.getMessage());
        } catch (final Exception e) {
            s_logger.debug("Failed to delete the exiting config drive for vm "+vmName+ " "+ e.getMessage());
        }


        if (vmDataList != null) {
            for (final String[] item : vmDataList) {
                final String dataType = item[0];
                final String fileName = item[1];
                final String content = item[2];

                // create file with content in folder

                if (dataType != null && !dataType.isEmpty()) {
                    //create folder
                    final String  folder = isoPath+configDriveName+dataType;
                    if (folder != null && !folder.isEmpty()) {
                        final File dir = new File(folder);
                        final boolean result = true;

                        try {
                            if (!dir.exists()) {
                                dir.mkdirs();
                            }
                        }catch (final SecurityException ex) {
                            s_logger.debug("Failed to create dir "+ ex.getMessage());
                            return false;
                        }

                        if (result && content != null && !content.isEmpty()) {
                            File file = new File(folder+"/"+fileName+".txt");
                            try (OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(file.getAbsoluteFile()),"UTF-8");
                                 BufferedWriter bw = new BufferedWriter(fw);
                                ) {
                                bw.write(content);
                                s_logger.debug("created file: "+ file + " in folder:"+folder);
                            } catch (final IOException ex) {
                                s_logger.debug("Failed to create file "+ ex.getMessage());
                                return false;
                            }
                        }
                    }
                }
            }
            s_logger.debug("Created the vm data in "+ isoPath);
        }

        String s = null;
        try {

            final String cmd =  "mkisofs -iso-level 3 -V "+ configDriveLabel +" -o "+ isoPath+vmName +".iso " + isoPath;
            final Process p = Runtime.getRuntime().exec(cmd);

            final BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream(), defaultCharset()));

            final BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(p.getErrorStream(), defaultCharset()));

            // read the output from the command
            while ((s = stdInput.readLine()) != null) {
                s_logger.debug(s);
            }

            // read any errors from the attempted command
            while ((s = stdError.readLine()) != null) {
                s_logger.debug(s);
            }
            s_logger.debug(" Created config drive ISO using the command " + cmd +" in the host "+ this.host.getIp());
        } catch (final IOException e) {
            s_logger.debug(e.getMessage());
            return false;
        }

        return true;
    }

    protected String logX(final XenAPIObject obj, final String msg) {
        return new StringBuilder("Host ").append(this.host.getIp()).append(" ").append(obj.toWireString()).append(": ").append(msg).toString();
    }
}
