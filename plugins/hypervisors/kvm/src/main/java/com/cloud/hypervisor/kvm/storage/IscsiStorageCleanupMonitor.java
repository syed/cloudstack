package com.cloud.hypervisor.kvm.storage;

import com.cloud.hypervisor.kvm.resource.LibvirtConnection;
import com.cloud.hypervisor.kvm.resource.LibvirtDomainXMLParser;
import com.cloud.hypervisor.kvm.resource.LibvirtVMDef;
import org.apache.cloudstack.managed.context.ManagedContextRunnable;
import org.apache.log4j.Logger;
import org.libvirt.Connect;
import org.libvirt.Domain;
import org.libvirt.LibvirtException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IscsiStorageCleanupMonitor implements Runnable{
    private static final Logger s_logger = Logger.getLogger(IscsiStorageCleanupMonitor.class);
    private static final int CLEANUP_INTERVAL_SEC = 30; // check every X seconds
    private static final String ISCSI_PATH_PREFIX = "/dev/disk/by-path/ip-";
    private static final String KEYWORD_ISCSI = "iscsi";
    private static final String KEYWORD_IQN = "iqn";

    private IscsiAdmStorageAdaptor iscsiStorageAdaptor;

    private Map<String, Boolean> diskStatusMap;

    public IscsiStorageCleanupMonitor() {
        diskStatusMap = new HashMap<>();
        s_logger.debug("Initialize cleanup thread");
        iscsiStorageAdaptor = new IscsiAdmStorageAdaptor();
    }


    private class Monitor extends ManagedContextRunnable {

        @Override
        protected void runInContext() {
            //change the status of volumemap entries to false
            for (String diskPath : diskStatusMap.keySet()) {
                diskStatusMap.put(diskPath, false);
            }

            Connect conn = null;
            try {
                conn = LibvirtConnection.getConnection();

                int[] domains = conn.listDomains();
                s_logger.debug(String.format(" ********* FOUND %d DOMAINS ************", domains.length));
                for (int domId : domains) {
                    Domain dm = conn.domainLookupByID(domId);
                    final String domXml = dm.getXMLDesc(0);
                    final LibvirtDomainXMLParser parser = new LibvirtDomainXMLParser();
                    parser.parseDomainXML(domXml);
                    List<LibvirtVMDef.DiskDef> disks = parser.getDisks();

                    //populate the volume map. If an entry exists change the status to True
                    for (final LibvirtVMDef.DiskDef disk : disks) {
                        if (isIscsiDisk(disk)) {
                            diskStatusMap.put(disk.getDiskPath(), true);
                            s_logger.debug("Disk found by cleanup thread" + disk.getDiskPath());
                        }
                    }
                }

                // the ones where the state is false, they are stale. They may be actually
                // removed we go through each volume which is false, check iscsiadm,
                // if the volume still exisits, logout of that volume and remove it from the map
                for (String diskPath : diskStatusMap.keySet()) {
                    if (!diskStatusMap.get(diskPath)) {
                        if (Files.exists(Paths.get(diskPath))) {
                            try {
                                s_logger.info("Cleaning up disk " + diskPath);
                                iscsiStorageAdaptor.disconnectPhysicalDiskByPath(diskPath);
                            } catch (Exception e) {
                                s_logger.warn("[ignored] Error cleaning up " + diskPath, e);
                            }
                        }
                        diskStatusMap.remove(diskPath);
                    }
                }

            } catch (LibvirtException e) {
                s_logger.warn("[ignored] Error tryong to cleanup ", e);
            }
        }

        private boolean isIscsiDisk(LibvirtVMDef.DiskDef disk) {
            String path = disk.getDiskPath();
            return path.startsWith(ISCSI_PATH_PREFIX) && path.contains(KEYWORD_ISCSI) && path.contains(KEYWORD_IQN);
        }
    }

    @Override
    public void run() {
        while(true) {
            Thread monitorThread = new Thread(new Monitor());
            monitorThread.start();
            try {
                monitorThread.join();
            } catch (InterruptedException e) {
                s_logger.debug("[ignored] interupted joining monitor.");
            }

            try {
                Thread.sleep(CLEANUP_INTERVAL_SEC * 1000);
            } catch (InterruptedException e) {
                s_logger.debug("[ignored] interupted between heartbeats.");
            }
        }
    }
}
