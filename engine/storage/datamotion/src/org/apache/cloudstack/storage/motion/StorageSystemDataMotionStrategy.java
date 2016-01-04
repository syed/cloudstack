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
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService.VolumeApiResult;
import org.apache.cloudstack.framework.async.AsyncCallFuture;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.framework.config.dao.ConfigurationDao;
import org.apache.cloudstack.storage.command.CopyCmdAnswer;
import org.apache.cloudstack.storage.command.CopyCommand;
import org.apache.cloudstack.storage.command.ResignatureAnswer;
import org.apache.cloudstack.storage.command.ResignatureCommand;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.log4j.Logger;

import org.springframework.stereotype.Component;

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
import com.cloud.storage.Storage.ImageFormat;
import com.cloud.storage.VolumeDetailVO;
import com.cloud.storage.VolumeVO;
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
    public void copyAsync(DataObject srcData, DataObject destData, Host destHost, AsyncCompletionCallback<CopyCommandResult> callback) {
        if (srcData instanceof SnapshotInfo) {
            SnapshotInfo snapshotInfo = (SnapshotInfo)srcData;

            validate(snapshotInfo);

            boolean canHandleSrc = canHandle(srcData.getDataStore());

            if (canHandleSrc && destData instanceof TemplateInfo &&
                    (destData.getDataStore().getRole() == DataStoreRole.Image || destData.getDataStore().getRole() == DataStoreRole.ImageCache)) {
                handleCreateTemplateFromSnapshot(snapshotInfo, (TemplateInfo)destData, callback);
                return;
            }

            if (destData instanceof VolumeInfo) {
                VolumeInfo volumeInfo = (VolumeInfo)destData;
                boolean canHandleDest = canHandle(destData.getDataStore());

                if (canHandleSrc && canHandleDest) {
                    handleCreateVolumeFromSnapshotBothOnStorageSystem(snapshotInfo, volumeInfo, callback);
                    return;
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

    private boolean alreadyHaveCloneFor(SnapshotInfo snapshotInfo) {
        String property = getProperty(snapshotInfo.getId(), DiskTO.IQN);

        return property != null;
    }

    private Void handleCreateTemplateFromSnapshot(SnapshotInfo snapshotInfo, TemplateInfo templateInfo, AsyncCompletionCallback<CopyCommandResult> callback) {
        try {
            snapshotInfo.processEvent(Event.CopyingRequested);
        }
        catch (Exception ex) {
            throw new CloudRuntimeException("This snapshot is not currently in a state where it can be used to create a template.");
        }

        HostVO hostVO = null;

        if (alreadyHaveCloneFor(snapshotInfo)) {
            hostVO = getXenServerHost(snapshotInfo.getDataStore().getId(), false);

            if (hostVO == null) {
                throw new CloudRuntimeException("Unable to locate an applicable host");
            }
        }
        else {
            hostVO = getXenServerHost(snapshotInfo.getDataStore().getId(), true);

            createCloneFromSnapshot(hostVO, snapshotInfo);
        }

        DataStore srcDataStore = snapshotInfo.getDataStore();

        String value = _configDao.getValue(Config.PrimaryStorageDownloadWait.toString());
        int primaryStorageDownloadWait = NumbersUtil.parseInt(value, Integer.parseInt(Config.PrimaryStorageDownloadWait.getDefaultValue()));
        CopyCommand copyCommand = new CopyCommand(snapshotInfo.getTO(), templateInfo.getTO(), primaryStorageDownloadWait, VirtualMachineManager.ExecuteInSequence.value());

        String errMsg = null;

        CopyCmdAnswer copyCmdAnswer = null;

        try {
            _volumeService.grantAccess(snapshotInfo, hostVO, srcDataStore);

            Map<String, String> srcDetails = getSnapshotDetails(_storagePoolDao.findById(srcDataStore.getId()), snapshotInfo);

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

        return null;
    }

    private Void handleCreateVolumeFromSnapshotBothOnStorageSystem(SnapshotInfo snapshotInfo, VolumeInfo volumeInfo, AsyncCompletionCallback<CopyCommandResult> callback) {
        HostVO hostVO = getXenServerHost(snapshotInfo.getDataStore().getId(), true);

        if (!alreadyHaveCloneFor(snapshotInfo)) {
            createCloneFromSnapshot(hostVO, snapshotInfo);
        }

        if (hostVO == null) {
            hostVO = getXenServerHost(snapshotInfo.getDataStore().getId(), false);

            if (hostVO == null) {
                throw new CloudRuntimeException("Unable to locate an applicable host");
            }
        }

        boolean canComputeClusterHandleClonedVolume = canComputeClusterHandleClonedVolume(hostVO.getClusterId());
        boolean canStorageSystemCloneVolume = canStorageSystemCloneVolume(volumeInfo.getPoolId());

        try {
            // at this point, the snapshotInfo and volumeInfo should have the same disk offering ID (so either one should be OK to get a DiskOfferingVO instance)
            DiskOfferingVO diskOffering = _diskOfferingDao.findByIdIncludingRemoved(volumeInfo.getDiskOfferingId());
            SnapshotVO snapshot = _snapshotDao.findById(snapshotInfo.getId());

            VolumeDetailVO volumeDetail = null;

            if (canComputeClusterHandleClonedVolume && canStorageSystemCloneVolume) {
                volumeDetail = new VolumeDetailVO(volumeInfo.getId(),
                        "cloneOf",
                        String.valueOf(snapshotInfo.getId()),
                        false);

                volumeDetail = _volumeDetailsDao.persist(volumeDetail);
            }

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

        if (canComputeClusterHandleClonedVolume && canStorageSystemCloneVolume) {
            copyCmdAnswer = performResignature(volumeInfo, hostVO);
        }
        else {
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

    private void createCloneFromSnapshot(HostVO hostVO, SnapshotInfo snapshotInfo) {
        if (hostVO != null) {
            snapshotInfo.getDataStore().getDriver().createAsync(snapshotInfo.getDataStore(), snapshotInfo, null);

            CopyCmdAnswer copyCmdAnswer = performResignature(snapshotInfo, hostVO);

            if (copyCmdAnswer == null || !copyCmdAnswer.getResult()) {
                if (copyCmdAnswer != null && copyCmdAnswer.getDetails() != null && !copyCmdAnswer.getDetails().isEmpty()) {
                    throw new CloudRuntimeException(copyCmdAnswer.getDetails());
                }
                else {
                    throw new CloudRuntimeException("Unable to perform host-side operation");
                }
            }

            SnapshotDetailsVO snapshotDetail = new SnapshotDetailsVO(snapshotInfo.getId(),
                    DiskTO.PATH,
                    ((VolumeObjectTO)copyCmdAnswer.getNewData()).getPath(),
                    false);

            _snapshotDetailsDao.persist(snapshotDetail);
        }
        else {
            throw new CloudRuntimeException("Unable to locate an applicable host with which to perform a resignature operation");
        }
    }

    private boolean canComputeClusterHandleClonedVolume(long clusterId) {
        List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

        if (hosts == null) {
            return false;
        }

        for (HostVO host : hosts) {
            if (host == null) {
                return false;
            }

            DetailVO hostDetail = _hostDetailsDao.findDetail(host.getId(), "supportsClonedVolumes");

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

    private boolean canStorageSystemCloneVolume(long storagePoolId) {
        boolean supportsVolumeCloning = false;

        DataStore dataStore = _dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);

        Map<String, String> mapCapabilities = dataStore.getDriver().getCapabilities();

        if (mapCapabilities != null) {
            String value = mapCapabilities.get(DataStoreCapabilities.CAN_CLONE_VOLUME.toString());

            supportsVolumeCloning = new Boolean(value);
        }

        return supportsVolumeCloning;
    }

    private Map<String, String> getSnapshotDetails(StoragePoolVO storagePoolVO, SnapshotInfo snapshotInfo) {
        Map<String, String> details = new HashMap<String, String>();

        details.put(DiskTO.STORAGE_HOST, storagePoolVO.getHostAddress());
        details.put(DiskTO.STORAGE_PORT, String.valueOf(storagePoolVO.getPort()));

        long snapshotId = snapshotInfo.getId();

        details.put(DiskTO.IQN, getProperty(snapshotId, DiskTO.IQN));

        details.put(DiskTO.CHAP_INITIATOR_USERNAME, getProperty(snapshotId, DiskTO.CHAP_INITIATOR_USERNAME));
        details.put(DiskTO.CHAP_INITIATOR_SECRET, getProperty(snapshotId, DiskTO.CHAP_INITIATOR_SECRET));
        details.put(DiskTO.CHAP_TARGET_USERNAME, getProperty(snapshotId, DiskTO.CHAP_TARGET_USERNAME));
        details.put(DiskTO.CHAP_TARGET_SECRET, getProperty(snapshotId, DiskTO.CHAP_TARGET_SECRET));

        return details;
    }

    private String getProperty(long snapshotId, String property) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshotId, property);

        if (snapshotDetails != null) {
            return snapshotDetails.getValue();
        }

        return null;
    }

    private Map<String, String> getVolumeDetails(VolumeInfo volumeInfo) {
        Map<String, String> sourceDetails = new HashMap<String, String>();

        VolumeVO volumeVO = _volumeDao.findById(volumeInfo.getId());

        long storagePoolId = volumeVO.getPoolId();
        StoragePoolVO storagePoolVO = _storagePoolDao.findById(storagePoolId);

        sourceDetails.put(DiskTO.STORAGE_HOST, storagePoolVO.getHostAddress());
        sourceDetails.put(DiskTO.STORAGE_PORT, String.valueOf(storagePoolVO.getPort()));
        sourceDetails.put(DiskTO.IQN, volumeVO.get_iScsiName());

        ChapInfo chapInfo = _volumeService.getChapInfo(volumeInfo, volumeInfo.getDataStore());

        if (chapInfo != null) {
            sourceDetails.put(DiskTO.CHAP_INITIATOR_USERNAME, chapInfo.getInitiatorUsername());
            sourceDetails.put(DiskTO.CHAP_INITIATOR_SECRET, chapInfo.getInitiatorSecret());
            sourceDetails.put(DiskTO.CHAP_TARGET_USERNAME, chapInfo.getTargetUsername());
            sourceDetails.put(DiskTO.CHAP_TARGET_SECRET, chapInfo.getTargetSecret());
        }

        return sourceDetails;
    }

    private HostVO getXenServerHost(long dataStoreId, boolean computeClusterMustHandleClonedVolume) {
        StoragePoolVO storagePoolVO = _storagePoolDao.findById(dataStoreId);

        List<? extends Cluster> clusters = _mgr.searchForClusters(storagePoolVO.getDataCenterId(), new Long(0), Long.MAX_VALUE, HypervisorType.XenServer.toString());

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
                            if (computeClusterMustHandleClonedVolume) {
                                if (canComputeClusterHandleClonedVolume(cluster.getId())) {
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
    public void copyAsync(Map<VolumeInfo, DataStore> volumeMap, VirtualMachineTO vmTo, Host srcHost, Host destHost, AsyncCompletionCallback<CopyCommandResult> callback) {
        CopyCommandResult result = new CopyCommandResult(null, null);
        result.setResult("Unsupported operation requested for copying data.");
        callback.complete(result);
    }

    private CopyCmdAnswer performResignature(VolumeInfo volumeInfo, HostVO hostVO) {
        long storagePoolId = volumeInfo.getPoolId();
        DataStore dataStore = _dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);

        Map<String, String> details = getVolumeDetails(volumeInfo);

        ResignatureCommand command = new ResignatureCommand(details);

        ResignatureAnswer answer = null;

        try {
            _volumeService.grantAccess(volumeInfo, hostVO, dataStore);

            answer = (ResignatureAnswer)_agentMgr.send(hostVO.getId(), command);
        }
        catch (Exception ex) {
            throw new CloudRuntimeException(ex.getMessage());
        }
        finally {
            try {
                _volumeService.revokeAccess(volumeInfo, hostVO, dataStore);
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

    private CopyCmdAnswer performResignature(SnapshotInfo snapshotInfo, HostVO hostVO) {
        long storagePoolId = snapshotInfo.getDataStore().getId();
        DataStore dataStore = _dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);

        Map<String, String> details = getSnapshotDetails(_storagePoolDao.findById(storagePoolId), snapshotInfo);

        ResignatureCommand command = new ResignatureCommand(details);

        ResignatureAnswer answer = null;

        try {
            _volumeService.grantAccess(snapshotInfo, hostVO, dataStore);

            answer = (ResignatureAnswer)_agentMgr.send(hostVO.getId(), command);
        }
        catch (Exception ex) {
            throw new CloudRuntimeException(ex.getMessage());
        }
        finally {
            try {
                _volumeService.revokeAccess(snapshotInfo, hostVO, dataStore);
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

            Map<String, String> srcDetails = getSnapshotDetails(_storagePoolDao.findById(snapshotInfo.getDataStore().getId()), snapshotInfo);

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
