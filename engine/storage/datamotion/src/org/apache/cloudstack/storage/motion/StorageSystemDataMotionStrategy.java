/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cloudstack.storage.motion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import org.apache.cloudstack.engine.subsystem.api.storage.ChapInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataMotionStrategy;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreCapabilities;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreManager;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.StrategyPriority;
import org.apache.cloudstack.engine.subsystem.api.storage.TemplateInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService;
import org.apache.cloudstack.engine.subsystem.api.storage.ObjectInDataStoreStateMachine.Event;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService.VolumeApiResult;
import org.apache.cloudstack.framework.async.AsyncCallFuture;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.framework.config.dao.ConfigurationDao;
import org.apache.cloudstack.storage.command.CopyCommand;
import org.apache.cloudstack.storage.command.CopyCmdAnswer;
import org.apache.cloudstack.storage.command.ResignatureAnswer;
import org.apache.cloudstack.storage.command.ResignatureCommand;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;

import com.cloud.agent.AgentManager;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.agent.api.to.VirtualMachineTO;
import com.cloud.configuration.Config;
import com.cloud.host.DetailVO;
import com.cloud.host.Host;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.host.dao.HostDetailsDao;
import com.cloud.hypervisor.Hypervisor.HypervisorType;
import com.cloud.org.Cluster;
import com.cloud.org.Grouping.AllocationState;
import com.cloud.resource.ResourceState;
import com.cloud.server.ManagementService;
import com.cloud.storage.DataStoreRole;
import com.cloud.storage.DiskOfferingVO;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.VolumeDetailVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.Storage.ImageFormat;
import com.cloud.storage.dao.DiskOfferingDao;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.SnapshotDetailsDao;
import com.cloud.storage.dao.SnapshotDetailsVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.dao.VolumeDetailsDao;
import com.cloud.utils.NumbersUtil;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.VirtualMachineManager;

@Component
public class StorageSystemDataMotionStrategy implements DataMotionStrategy {
    private static final Logger s_logger = Logger.getLogger(StorageSystemDataMotionStrategy.class);

    @Inject private AgentManager _agentMgr;
    @Inject private ConfigurationDao _configDao;
    @Inject private DataStoreManager _dataStoreMgr;
    @Inject private DiskOfferingDao _diskOfferingDao;
    @Inject private HostDao _hostDao;
    @Inject private HostDetailsDao _hostDetailsDao;
    @Inject private ManagementService _mgr;
    @Inject private PrimaryDataStoreDao _storagePoolDao;
    @Inject private SnapshotDao _snapshotDao;
    @Inject private SnapshotDetailsDao _snapshotDetailsDao;
    @Inject private VolumeDao _volumeDao;
    @Inject private VolumeDataFactory _volumeDataFactory;
    @Inject private VolumeDetailsDao _volumeDetailsDao;
    @Inject private VolumeService _volumeService;

    @Override
    public StrategyPriority canHandle(DataObject srcData, DataObject destData) {
        if (srcData instanceof SnapshotInfo) {
            if (canHandle(srcData.getDataStore()) || canHandle(destData.getDataStore())) {
                return StrategyPriority.HIGHEST;
            }
        }

        return StrategyPriority.CANT_HANDLE;
    }

    private boolean canHandle(DataStore dataStore) {
        if (dataStore.getRole() == DataStoreRole.Primary) {
            Map<String, String> mapCapabilities = dataStore.getDriver().getCapabilities();

            if (mapCapabilities != null) {
                String value = mapCapabilities.get(DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT.toString());
                Boolean supportsStorageSystemSnapshots = new Boolean(value);

                if (supportsStorageSystemSnapshots) {
                    s_logger.info("Using 'StorageSystemDataMotionStrategy'");

                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public StrategyPriority canHandle(Map<VolumeInfo, DataStore> volumeMap, Host srcHost, Host destHost) {
        return StrategyPriority.CANT_HANDLE;
    }

    @Override
    public Void copyAsync(DataObject srcData, DataObject destData, Host destHost, AsyncCompletionCallback<CopyCommandResult> callback) {
        if (srcData instanceof SnapshotInfo) {
            SnapshotInfo snapshotInfo = (SnapshotInfo)srcData;

            validate(snapshotInfo);

            boolean canHandleSrc = canHandle(srcData.getDataStore());

            if (canHandleSrc && destData instanceof TemplateInfo &&
                    (destData.getDataStore().getRole() == DataStoreRole.Image || destData.getDataStore().getRole() == DataStoreRole.ImageCache)) {
                return handleCreateTemplateFromSnapshot(snapshotInfo, (TemplateInfo)destData, callback);
            }

            if (destData instanceof VolumeInfo) {
                VolumeInfo volumeInfo = (VolumeInfo)destData;

                boolean canHandleDest = canHandle(destData.getDataStore());

                if (canHandleSrc && canHandleDest) {
                    if (snapshotInfo.getDataStore().getId() == volumeInfo.getDataStore().getId()) {
                        return handleCreateVolumeFromSnapshotBothOnStorageSystem(snapshotInfo, volumeInfo, callback);
                    }
                    else {
                        throw new UnsupportedOperationException("This operation is not supported (DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT " +
                                "not supported by source or destination storage plug-in).");
                    }
                }

                if (canHandleSrc) {
                    throw new UnsupportedOperationException("This operation is not supported (DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT " +
                            "not supported by destination storage plug-in).");
                }

                if (canHandleDest) {
                    throw new UnsupportedOperationException("This operation is not supported (DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT " +
                            "not supported by source storage plug-in).");
                }
            }
        }

        throw new UnsupportedOperationException("This operation is not supported.");
    }

    private void validate(SnapshotInfo snapshotInfo) {
        long volumeId = snapshotInfo.getVolumeId();

        VolumeVO volumeVO = _volumeDao.findByIdIncludingRemoved(volumeId);

        if (volumeVO.getFormat() != ImageFormat.VHD) {
            throw new CloudRuntimeException("Only the " + ImageFormat.VHD.toString() + " image type is currently supported.");
        }
    }

    private boolean usingBackendSnapshotFor(SnapshotInfo snapshotInfo) {
        String property = getProperty(snapshotInfo.getId(), "takeSnapshot");

        return Boolean.parseBoolean(property);
    }

    private Void handleCreateTemplateFromSnapshot(SnapshotInfo snapshotInfo, TemplateInfo templateInfo, AsyncCompletionCallback<CopyCommandResult> callback) {
        try {
            snapshotInfo.processEvent(Event.CopyingRequested);
        }
        catch (Exception ex) {
            throw new CloudRuntimeException("This snapshot is not currently in a state where it can be used to create a template.");
        }

        HostVO hostVO = getXenServerHost(snapshotInfo);

        boolean usingBackendSnapshot = usingBackendSnapshotFor(snapshotInfo);
        boolean computeClusterSupportsResign = computeClusterSupportsResign(hostVO.getClusterId());

        if (usingBackendSnapshot && !computeClusterSupportsResign) {
            throw new CloudRuntimeException("Unable to locate an applicable host with which to perform a resignature operation");
        }

        try {
            if (usingBackendSnapshot) {
                createVolumeFromSnapshot(hostVO, snapshotInfo);
            }

            DataStore srcDataStore = snapshotInfo.getDataStore();

            String value = _configDao.getValue(Config.PrimaryStorageDownloadWait.toString());
            int primaryStorageDownloadWait = NumbersUtil.parseInt(value, Integer.parseInt(Config.PrimaryStorageDownloadWait.getDefaultValue()));
            CopyCommand copyCommand = new CopyCommand(snapshotInfo.getTO(), templateInfo.getTO(), primaryStorageDownloadWait, VirtualMachineManager.ExecuteInSequence.value());

            String errMsg = null;

            CopyCmdAnswer copyCmdAnswer = null;

            try {
                _volumeService.grantAccess(snapshotInfo, hostVO, srcDataStore);

                Map<String, String> srcDetails = getSnapshotDetails(snapshotInfo);

                copyCommand.setOptions(srcDetails);

                copyCmdAnswer = (CopyCmdAnswer)_agentMgr.send(hostVO.getId(), copyCommand);
            }
            catch (Exception ex) {
                throw new CloudRuntimeException(ex.getMessage());
            }
            finally {
                try {
                    _volumeService.revokeAccess(snapshotInfo, hostVO, srcDataStore);
                }
                catch (Exception ex) {
                    s_logger.debug(ex.getMessage(), ex);
                }

                if (copyCmdAnswer == null || !copyCmdAnswer.getResult()) {
                    if (copyCmdAnswer != null && copyCmdAnswer.getDetails() != null && !copyCmdAnswer.getDetails().isEmpty()) {
                        errMsg = copyCmdAnswer.getDetails();
                    }
                    else {
                        errMsg = "Unable to perform host-side operation";
                    }
                }

                try {
                    if (errMsg == null) {
                        snapshotInfo.processEvent(Event.OperationSuccessed);
                    }
                    else {
                        snapshotInfo.processEvent(Event.OperationFailed);
                    }
                }
                catch (Exception ex) {
                    s_logger.debug(ex.getMessage(), ex);
                }
            }

            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

            result.setResult(errMsg);

            callback.complete(result);
        }
        finally {
            if (usingBackendSnapshot) {
                deleteVolumeFromSnapshot(snapshotInfo);
            }
        }

        return null;
    }

    private Void handleCreateVolumeFromSnapshotBothOnStorageSystem(SnapshotInfo snapshotInfo, VolumeInfo volumeInfo, AsyncCompletionCallback<CopyCommandResult> callback) {
        HostVO hostVO = getXenServerHost(snapshotInfo);

        boolean usingBackendSnapshot = usingBackendSnapshotFor(snapshotInfo);
        boolean computeClusterSupportsResign = computeClusterSupportsResign(hostVO.getClusterId());

        if (usingBackendSnapshot && !computeClusterSupportsResign) {
            throw new CloudRuntimeException("Unable to locate an applicable host with which to perform a resignature operation");
        }

        boolean canStorageSystemCreateVolumeFromVolume = canStorageSystemCreateVolumeFromVolume(snapshotInfo);
        boolean useCloning = usingBackendSnapshot || (canStorageSystemCreateVolumeFromVolume && computeClusterSupportsResign);

        try {
            VolumeDetailVO volumeDetail = null;

            if (useCloning) {
                volumeDetail = new VolumeDetailVO(volumeInfo.getId(),
                    "cloneOf",
                    String.valueOf(snapshotInfo.getId()),
                    false);

                volumeDetail = _volumeDetailsDao.persist(volumeDetail);
            }

            // at this point, the snapshotInfo and volumeInfo should have the same disk offering ID (so either one should be OK to get a DiskOfferingVO instance)
            DiskOfferingVO diskOffering = _diskOfferingDao.findByIdIncludingRemoved(volumeInfo.getDiskOfferingId());
            SnapshotVO snapshot = _snapshotDao.findById(snapshotInfo.getId());

            // update the volume's hv_ss_reserve (hypervisor snapshot reserve) from a disk offering (used for managed storage)
            _volumeService.updateHypervisorSnapshotReserveForVolume(diskOffering, volumeInfo.getId(), snapshot.getHypervisorType());

            AsyncCallFuture<VolumeApiResult> future = _volumeService.createVolumeAsync(volumeInfo, volumeInfo.getDataStore());

            VolumeApiResult result = future.get();

            if (volumeDetail != null) {
                _volumeDetailsDao.remove(volumeDetail.getId());
            }

            if (result.isFailed()) {
                s_logger.debug("Failed to create a volume: " + result.getResult());

                throw new CloudRuntimeException(result.getResult());
            }
        }
        catch (Exception ex) {
            throw new CloudRuntimeException(ex.getMessage());
        }

        volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());

        volumeInfo.processEvent(Event.MigrationRequested);

        volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());

        final CopyCmdAnswer copyCmdAnswer;

        if (useCloning) {
            copyCmdAnswer = performResignature(volumeInfo, hostVO);
        }
        else {
            // asking for a XenServer host here so we don't always prefer to use XenServer hosts that support resigning
            // even when we don't need those hosts to do this kind of copy work
            hostVO = getXenServerHost(snapshotInfo.getDataCenterId(), false);

            copyCmdAnswer = performCopyOfVdi(volumeInfo, snapshotInfo, hostVO);
        }

        String errMsg = null;

        if (copyCmdAnswer == null || !copyCmdAnswer.getResult()) {
            if (copyCmdAnswer != null && copyCmdAnswer.getDetails() != null && !copyCmdAnswer.getDetails().isEmpty()) {
                errMsg = copyCmdAnswer.getDetails();
            }
            else {
                errMsg = "Unable to perform host-side operation";
            }
        }

        CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

        result.setResult(errMsg);

        callback.complete(result);

        return null;
    }

    // If the underlying storage system is making use of read-only snapshots, this gives the storage system the opportunity to
    // create a volume from the snapshot so that we can copy the VHD file that should be inside of the snapshot to secondary storage.
    //
    // The resultant volume must be writable because we need to resign the SR and the VDI that should be inside of it before we copy
    // the VHD file to secondary storage.
    //
    // If the storage system is using writable snapshots, then nothing need be done by that storage system here because we can just
    // resign the SR and the VDI that should be inside of the snapshot before copying the VHD file to secondary storage.
    private void createVolumeFromSnapshot(HostVO hostVO, SnapshotInfo snapshotInfo) {
        SnapshotDetailsVO snapshotDetails = handleSnapshotDetails(snapshotInfo.getId(), "tempVolume", "create");

        try {
            snapshotInfo.getDataStore().getDriver().createAsync(snapshotInfo.getDataStore(), snapshotInfo, null);
        }
        finally {
            _snapshotDetailsDao.remove(snapshotDetails.getId());
        }

        CopyCmdAnswer copyCmdAnswer = performResignature(snapshotInfo, hostVO);

        if (copyCmdAnswer == null || !copyCmdAnswer.getResult()) {
            if (copyCmdAnswer != null && copyCmdAnswer.getDetails() != null && !copyCmdAnswer.getDetails().isEmpty()) {
                throw new CloudRuntimeException(copyCmdAnswer.getDetails());
            }
            else {
                throw new CloudRuntimeException("Unable to perform host-side operation");
            }
        }
    }

    // If the underlying storage system needed to create a volume from a snapshot for createVolumeFromSnapshot(HostVO, SnapshotInfo), then
    // this is its opportunity to delete that temporary volume and restore properties in snapshot_details to the way they were before the
    // invocation of createVolumeFromSnapshot(HostVO, SnapshotInfo).
    private void deleteVolumeFromSnapshot(SnapshotInfo snapshotInfo) {
        SnapshotDetailsVO snapshotDetails = handleSnapshotDetails(snapshotInfo.getId(), "tempVolume", "delete");

        try {
            snapshotInfo.getDataStore().getDriver().createAsync(snapshotInfo.getDataStore(), snapshotInfo, null);
        }
        finally {
            _snapshotDetailsDao.remove(snapshotDetails.getId());
        }
    }

    private SnapshotDetailsVO handleSnapshotDetails(long csSnapshotId, String name, String value) {
        _snapshotDetailsDao.removeDetail(csSnapshotId, name);

        SnapshotDetailsVO snapshotDetails = new SnapshotDetailsVO(csSnapshotId, name, value, false);

        return _snapshotDetailsDao.persist(snapshotDetails);
    }

    private boolean canStorageSystemCreateVolumeFromVolume(SnapshotInfo snapshotInfo) {
        boolean supportsCloningVolumeFromVolume = false;

        DataStore dataStore = _dataStoreMgr.getDataStore(snapshotInfo.getDataStore().getId(), DataStoreRole.Primary);

        Map<String, String> mapCapabilities = dataStore.getDriver().getCapabilities();

        if (mapCapabilities != null) {
            String value = mapCapabilities.get(DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_VOLUME.toString());

            supportsCloningVolumeFromVolume = new Boolean(value);
        }

        return supportsCloningVolumeFromVolume;
    }

    private boolean computeClusterSupportsResign(long clusterId) {
        List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

        if (hosts == null) {
            return false;
        }

        for (HostVO host : hosts) {
            if (host == null) {
                return false;
            }

            DetailVO hostDetail = _hostDetailsDao.findDetail(host.getId(), "supportsResign");

            if (hostDetail == null) {
                return false;
            }

            String value = hostDetail.getValue();

            Boolean booleanValue = Boolean.valueOf(value);

            if (booleanValue == false) {
                return false;
            }
        }

        return true;
    }

    private String getProperty(long snapshotId, String property) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshotId, property);

        if (snapshotDetails != null) {
            return snapshotDetails.getValue();
        }

        return null;
    }

    private Map<String, String> getVolumeDetails(VolumeInfo volumeInfo) {
        Map<String, String> volumeDetails = new HashMap<String, String>();

        VolumeVO volumeVO = _volumeDao.findById(volumeInfo.getId());

        long storagePoolId = volumeVO.getPoolId();
        StoragePoolVO storagePoolVO = _storagePoolDao.findById(storagePoolId);

        volumeDetails.put(DiskTO.STORAGE_HOST, storagePoolVO.getHostAddress());
        volumeDetails.put(DiskTO.STORAGE_PORT, String.valueOf(storagePoolVO.getPort()));
        volumeDetails.put(DiskTO.IQN, volumeVO.get_iScsiName());

        ChapInfo chapInfo = _volumeService.getChapInfo(volumeInfo, volumeInfo.getDataStore());

        if (chapInfo != null) {
            volumeDetails.put(DiskTO.CHAP_INITIATOR_USERNAME, chapInfo.getInitiatorUsername());
            volumeDetails.put(DiskTO.CHAP_INITIATOR_SECRET, chapInfo.getInitiatorSecret());
            volumeDetails.put(DiskTO.CHAP_TARGET_USERNAME, chapInfo.getTargetUsername());
            volumeDetails.put(DiskTO.CHAP_TARGET_SECRET, chapInfo.getTargetSecret());
        }

        return volumeDetails;
    }

    private Map<String, String> getSnapshotDetails(SnapshotInfo snapshotInfo) {
        Map<String, String> snapshotDetails = new HashMap<String, String>();

        long storagePoolId = snapshotInfo.getDataStore().getId();
        StoragePoolVO storagePoolVO = _storagePoolDao.findById(storagePoolId);

        snapshotDetails.put(DiskTO.STORAGE_HOST, storagePoolVO.getHostAddress());
        snapshotDetails.put(DiskTO.STORAGE_PORT, String.valueOf(storagePoolVO.getPort()));

        long snapshotId = snapshotInfo.getId();

        snapshotDetails.put(DiskTO.IQN, getProperty(snapshotId, DiskTO.IQN));

        snapshotDetails.put(DiskTO.CHAP_INITIATOR_USERNAME, getProperty(snapshotId, DiskTO.CHAP_INITIATOR_USERNAME));
        snapshotDetails.put(DiskTO.CHAP_INITIATOR_SECRET, getProperty(snapshotId, DiskTO.CHAP_INITIATOR_SECRET));
        snapshotDetails.put(DiskTO.CHAP_TARGET_USERNAME, getProperty(snapshotId, DiskTO.CHAP_TARGET_USERNAME));
        snapshotDetails.put(DiskTO.CHAP_TARGET_SECRET, getProperty(snapshotId, DiskTO.CHAP_TARGET_SECRET));

        return snapshotDetails;
    }

    private HostVO getXenServerHost(SnapshotInfo snapshotInfo) {
        HostVO hostVO = getXenServerHost(snapshotInfo.getDataCenterId(), true);

        if (hostVO == null) {
            hostVO = getXenServerHost(snapshotInfo.getDataCenterId(), false);

            if (hostVO == null) {
                throw new CloudRuntimeException("Unable to locate an applicable host");
            }
        }

        return hostVO;
    }

    private HostVO getXenServerHost(Long zoneId, boolean computeClusterMustSupportResign) {
        if (zoneId == null) {
            throw new CloudRuntimeException("Zone ID cannot be null.");
        }

        List<? extends Cluster> clusters = _mgr.searchForClusters(zoneId, new Long(0), Long.MAX_VALUE, HypervisorType.XenServer.toString());

        if (clusters == null) {
            clusters = new ArrayList<>();
        }

        Collections.shuffle(clusters, new Random(System.nanoTime()));

        clusters:
        for (Cluster cluster : clusters) {
            if (cluster.getAllocationState() == AllocationState.Enabled) {
                List<HostVO> hosts = _hostDao.findByClusterId(cluster.getId());

                if (hosts != null) {
                    Collections.shuffle(hosts, new Random(System.nanoTime()));

                    for (HostVO host : hosts) {
                        if (host.getResourceState() == ResourceState.Enabled) {
                            if (computeClusterMustSupportResign) {
                                if (computeClusterSupportsResign(cluster.getId())) {
                                    return host;
                                }
                                else {
                                    // no other host in the cluster in question should be able to satisfy our requirements here, so move on to the next cluster
                                    continue clusters;
                                }
                            }
                            else {
                                return host;
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

    @Override
    public Void copyAsync(DataObject srcData, DataObject destData, AsyncCompletionCallback<CopyCommandResult> callback) {
        return copyAsync(srcData, destData, null, callback);
    }

    @Override
    public Void copyAsync(Map<VolumeInfo, DataStore> volumeMap, VirtualMachineTO vmTo, Host srcHost, Host destHost, AsyncCompletionCallback<CopyCommandResult> callback) {
        CopyCommandResult result = new CopyCommandResult(null, null);

        result.setResult("Unsupported operation requested for copying data.");

        callback.complete(result);

        return null;
    }

    private Map<String, String> getDetails(DataObject dataObj) {
        if (dataObj instanceof VolumeInfo) {
            return getVolumeDetails((VolumeInfo)dataObj);
        }
        else if (dataObj instanceof SnapshotInfo) {
            return getSnapshotDetails((SnapshotInfo)dataObj);
        }

        throw new CloudRuntimeException("'dataObj' must be of type 'VolumeInfo' or 'SnapshotInfo'.");
    }

    private CopyCmdAnswer performResignature(DataObject dataObj, HostVO hostVO) {
        long storagePoolId = dataObj.getDataStore().getId();
        DataStore dataStore = _dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);

        Map<String, String> details = getDetails(dataObj);

        ResignatureCommand command = new ResignatureCommand(details);

        ResignatureAnswer answer = null;

        try {
            _volumeService.grantAccess(dataObj, hostVO, dataStore);

            answer = (ResignatureAnswer)_agentMgr.send(hostVO.getId(), command);
        }
        catch (Exception ex) {
            throw new CloudRuntimeException(ex.getMessage());
        }
        finally {
            try {
                _volumeService.revokeAccess(dataObj, hostVO, dataStore);
            }
            catch (Exception ex) {
                s_logger.debug(ex.getMessage(), ex);
            }
        }

        if (answer == null || !answer.getResult()) {
            final String errMsg;

            if (answer != null && answer.getDetails() != null && !answer.getDetails().isEmpty()) {
                errMsg = answer.getDetails();
            }
            else {
                errMsg = "Unable to perform resignature operation";
            }

            throw new CloudRuntimeException(errMsg);
        }

        VolumeObjectTO newVolume = new VolumeObjectTO();

        newVolume.setSize(answer.getSize());
        newVolume.setPath(answer.getPath());
        newVolume.setFormat(answer.getFormat());

        return new CopyCmdAnswer(newVolume);
    }

    private CopyCmdAnswer performCopyOfVdi(VolumeInfo volumeInfo, SnapshotInfo snapshotInfo, HostVO hostVO) {
        String value = _configDao.getValue(Config.PrimaryStorageDownloadWait.toString());
        int primaryStorageDownloadWait = NumbersUtil.parseInt(value, Integer.parseInt(Config.PrimaryStorageDownloadWait.getDefaultValue()));
        CopyCommand copyCommand = new CopyCommand(snapshotInfo.getTO(), volumeInfo.getTO(), primaryStorageDownloadWait, VirtualMachineManager.ExecuteInSequence.value());

        CopyCmdAnswer copyCmdAnswer = null;

        try {
            _volumeService.grantAccess(snapshotInfo, hostVO, snapshotInfo.getDataStore());
            _volumeService.grantAccess(volumeInfo, hostVO, volumeInfo.getDataStore());

            Map<String, String> srcDetails = getSnapshotDetails(snapshotInfo);

            copyCommand.setOptions(srcDetails);

            Map<String, String> destDetails = getVolumeDetails(volumeInfo);

            copyCommand.setOptions2(destDetails);

            copyCmdAnswer = (CopyCmdAnswer)_agentMgr.send(hostVO.getId(), copyCommand);
        }
        catch (Exception ex) {
            throw new CloudRuntimeException(ex.getMessage());
        }
        finally {
            try {
                _volumeService.revokeAccess(snapshotInfo, hostVO, snapshotInfo.getDataStore());
            }
            catch (Exception ex) {
                s_logger.debug(ex.getMessage(), ex);
            }

            try {
                _volumeService.revokeAccess(volumeInfo, hostVO, volumeInfo.getDataStore());
            }
            catch (Exception ex) {
                s_logger.debug(ex.getMessage(), ex);
            }
        }

        return copyCmdAnswer;
    }
}
