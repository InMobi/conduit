package com.inmobi.conduit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.inmobi.conduit.distcp.MergedStreamService;
import com.inmobi.conduit.distcp.MirrorStreamService;
import com.inmobi.conduit.local.LocalStreamService;
import com.inmobi.conduit.purge.DataPurgerService;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConduitInitialization {

  @BeforeMethod
  public void beforeTestMethod() {
    Conduit.setHCatEnabled(false);
  }

  public void setUP(String filename, Set<String> clustersToProcess, 
      List<AbstractService> listOfServices) throws Exception {
    ConduitConfigParser configParser = new ConduitConfigParser(filename);
    ConduitConfig config = configParser.getConfig();
    Conduit conduit = new Conduit(config, clustersToProcess);
    listOfServices.addAll(conduit.init());
  }
  
  public void testServicesOnCluster(String confFile, Set<String> clustersToProcess, 
      int numOfLocalServices, int numOfPurgerServices, int numofMergeServices, 
      int numOfMirrorServices)
          throws Exception {
    List<AbstractService> listOfServices = new ArrayList<AbstractService>();
    setUP(confFile, clustersToProcess, listOfServices);
    
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
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    testServicesOnCluster("test-combo-conduit.xml", clustersToProcess, 6, 1, 6,
        0);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 1, Merge Stream services  - 4, 
   * Mirror Stream Service  - 1, DataPurgerService - 1
   */
  @Test
  public void testCluster2() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster2");
    testServicesOnCluster("test-combo-conduit.xml", clustersToProcess, 6, 1, 4,
        3);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 1, Merge Stream services  - 4, 
   * Mirror Stream Service  - 1, DataPurgerService - 1
   */
  @Test
  public void testCluster3() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster3");
    testServicesOnCluster("test-combo-conduit.xml", clustersToProcess, 4, 1, 4,
        3);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 0, Merge Stream services  - 4, 
   * Mirror Stream Service  - 0, DataPurgerService - 1
   */
  @Test
  public void testCluster4() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster4");
    testServicesOnCluster("test-combo-conduit.xml", clustersToProcess, 0, 1, 4, 0);
  }
  
  /*
   * Expected O/P:
   * Local stream services  - 1, Merge Stream services  - 4, 
   * Mirror Stream Service  - 0, DataPurgerService - 1
   */
  @Test
  public void testCluster5() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster5");
    testServicesOnCluster("test-combo-conduit.xml", clustersToProcess, 4, 1, 4,
        0);
  }
  
  /*
   * Local stream service -- 4. 
   * Ex: testcluster1, testcluster2, testcluster3, testcluster5 are the sources 
   * of stream "stream21".
   * If the cluster is source of any stream then it will 
   * have local stream service. No local stream service for testcluster4 as it 
   * is not source of any stream.
   * 
   * DataPurger Service: 5
   * purger service for each cluster in the clusters to process set.
   *
   * Merge Service:  20 services
   * ---------------------------- 
   * tetscluster1: 4 merge services
   * Primary destination of a stream "stream21". testcluster1,
   * testcluster2,testcluster3,testcluster5 are the sources of "stream21".
   * So it will be having 4 merge services. 
   * 
   * testcluster2: 4 merge services
   * primary destination of "stream5_testcluster2" category. It has 4 sources.
   * 
   * testcluster3: 4 merge services
   * Primary destination of "stream5_ir1" category. It has 4 sources.
   * 
   * testcluster4:  4 merge services
   * primary destination of "stream5" category. All other clusters are sources.
   * 
   * testcluster5: 4 merge services
   * primary destination of "stream5_testcluster5" category. It has 4 sources.
   * 
   * --------------------
   * MirrorStream Service: 2 services.
   * ----------------------
   * testcluster2 and testcluster3 are the non primary destinations of the 
   * "stream3" category.
   */
  @Test
  public void testWithAllClusters() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    clustersToProcess.add("testcluster2");
    clustersToProcess.add("testcluster3");
    clustersToProcess.add("testcluster4");
    clustersToProcess.add("testcluster5");
    testServicesOnCluster("test-combo-conduit.xml", clustersToProcess, 20, 5,
        22, 6);
  }
  
  @Test
  public void testConfWithMultipleClusters() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    clustersToProcess.add("testcluster2");
    clustersToProcess.add("testcluster4");
    testServicesOnCluster("test-combo-conduit.xml", clustersToProcess, 12, 3,
        14, 3);
  }
  
  /*
   * testcluster1---- local stream service and purger service will be populated
   */
  @Test
  public void testLocalStreamService() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    testServicesOnCluster("test-lss-conduit.xml", clustersToProcess, 1, 1, 0, 0);
  }

  /*
   * testcluster1---- local stream service and purger service will be populated
   */
  @Test
  public void testDisabledLocalStreamService() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    testServicesOnCluster("test-lss-conduit.xml", clustersToProcess, 1, 1, 0, 0);
  }

  /*
   * testcluster1--- local, merge_cluster1_cluster1, merge_cluster2_cluster1
   *                 and purger services (4 services)
   */
  @Test
  public void testLocalMergeServices() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    testServicesOnCluster("test-mergedss-conduit.xml", clustersToProcess, 1, 1, 2,
        0);
    clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster2");
    testServicesOnCluster("test-mergedss-conduit.xml", clustersToProcess, 1, 1, 0,
        0);
  }
  
  /*
   * testcluster2--- merged and purger services  (2 services)
   */
  @Test
  public void testLocalMergeServicesWithMultipleClusters() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    clustersToProcess.add("testcluster2");
    testServicesOnCluster("test-xinclude-mergedss-conduit.xml",
        clustersToProcess, 2, 2, 2, 0);
  }

  /*
   * testcluster1--- local, merge and purger services
   * testcluster2--- merge, mirror and purger services
   * testcluster3--- It is neither source nor destination of any stream, so
   *                 only purger service will be started
   */
  @Test
  public void testConduitAllServices() throws Exception {
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    testServicesOnCluster("test-merge-mirror-conduit.xml", clustersToProcess, 1, 1,
        1, 0);
    clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster2");
    testServicesOnCluster("test-merge-mirror-conduit.xml", clustersToProcess, 0, 1,
        1, 1);
    clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster3");
    testServicesOnCluster("test-merge-mirror-conduit.xml", clustersToProcess, 0, 1,
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