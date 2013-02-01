package com.inmobi.databus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.databus.distcp.MergedStreamService;
import com.inmobi.databus.distcp.MirrorStreamService;
import com.inmobi.databus.local.LocalStreamService;
import com.inmobi.databus.purge.DataPurgerService;

public class TestDatabusInitialization {

  DatabusConfigParser configParser;
  DatabusConfig config;

  public void setUP(String filename) throws Exception {
    configParser = new DatabusConfigParser(filename);
    config = configParser.getConfig();
  }
  
  public void testServicesOnCluster(String confFile, String clusterName, 
      int numOfLocalServices, int numOfPurgerServices, int numofMergeServices, 
      int numOfMirrorServices)
          throws Exception {
    setUP(confFile);
    List<AbstractService> listOfServices = new ArrayList<AbstractService>();
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add(clusterName);
    Databus databus = new Databus(config, clustersToProcess);
    listOfServices.addAll(databus.init());

    Assert.assertEquals(numOfPurgerServices, getNumOfPurgerServices(
        listOfServices));
    Assert.assertEquals(numOfLocalServices, getNumOfLocalStreamServices(
        listOfServices));
    Assert.assertEquals(numofMergeServices, getNumOfMergeStreamServices(
        listOfServices));
    Assert.assertEquals(numOfMirrorServices, getNumOfMirrorStreamServices(
        listOfServices));  
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 1, Merge Stream services  - 4, 
   * Mirror Stream Service  - 0, DataPurgerService - 1
   */
  @Test
  public void testCluser1() throws Exception {
    testServicesOnCluster("test-combo-databus.xml", "testcluster1", 1, 1, 4, 0);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 1, Merge Stream services  - 4, 
   * Mirror Stream Service  - 1, DataPurgerService - 1
   */
  @Test
  public void testCluster2() throws Exception {
    testServicesOnCluster("test-combo-databus.xml", "testcluster2", 1, 1, 4, 1);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 1, Merge Stream services  - 4, 
   * Mirror Stream Service  - 1, DataPurgerService - 1
   */
  @Test
  public void testCluster3() throws Exception {
    testServicesOnCluster("test-combo-databus.xml", "testcluster3", 1, 1, 4, 1);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 0, Merge Stream services  - 4, 
   * Mirror Stream Service  - 0, DataPurgerService - 1
   */
  @Test
  public void testCluster4() throws Exception {
    testServicesOnCluster("test-combo-databus.xml", "testcluster4", 0, 1, 4, 0);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 1, Merge Stream services  - 4, 
   * Mirror Stream Service  - 0, DataPurgerService - 1
   */
  @Test
  public void testCluster5() throws Exception {
    testServicesOnCluster("test-combo-databus.xml", "testcluster5", 1, 1, 4, 0);
  }
  
  /*
   * testcluster1---- local stream service and purger service will be populated
   */
  @Test
  public void testLocalStreamService() throws Exception {
    testServicesOnCluster("test-lss-databus.xml", "testcluster1", 1, 1, 0, 0);
  }

  /*
   * testcluster1--- local, merge_cluster1_cluster1, merge_cluster2_cluster1
   *                 and purger services (4 services)
   * testcluster2--- merged and purger services  (2 services)
   */
  @Test
  public void testLocalMergeServices() throws Exception {
    testServicesOnCluster("test-mergedss-databus.xml", "testcluster1", 1, 1, 2, 
        0);
    testServicesOnCluster("test-mergedss-databus.xml", "testcluster2", 1, 1, 0, 
        0);
  }

  /*
   * testcluster1--- local, merge and purger services
   * testcluster2--- merge, mirror and purger services
   * testcluster3--- It is neither source nor destination of any stream, so
   *                 only purger service will be started
   */
  @Test
  public void testDatabusAllServices() throws Exception {
    testServicesOnCluster("test-merge-mirror-databus.xml", "testcluster1", 1, 1,
        1, 0);
    testServicesOnCluster("test-merge-mirror-databus.xml", "testcluster2", 0, 1,
        1, 1);
    testServicesOnCluster("test-merge-mirror-databus.xml", "testcluster3", 0, 1,
        0, 0);
  }

  private int getNumOfPurgerServices(List<AbstractService> listOfServices) {
    int numOfPurgerServices = 0;
    for (AbstractService service : listOfServices) {
      if (service instanceof DataPurgerService) {
        numOfPurgerServices++;
      }
    }
    return numOfPurgerServices;
  }

  protected int getNumOfLocalStreamServices(
      List<AbstractService> listOfServices) {
    int numOfLocalServices = 0;
    for (AbstractService service : listOfServices) {
      if (service instanceof LocalStreamService) {
        numOfLocalServices++;
      }
    }
    return numOfLocalServices;
  }

  protected int getNumOfMergeStreamServices(List<AbstractService> listOfServices) 
  {
    int numOfMergeServices = 0;
    for (AbstractService service : listOfServices) {
      if (service instanceof MergedStreamService) {
        numOfMergeServices++;
      }
    }
    return numOfMergeServices;
  }

  protected int getNumOfMirrorStreamServices(
      List<AbstractService> listOfServices) {
    int numOfMirrorServices = 0;
    for (AbstractService service : listOfServices) {
      if (service instanceof MirrorStreamService) {
        numOfMirrorServices++;
      }
    }
    return numOfMirrorServices;
  }
}