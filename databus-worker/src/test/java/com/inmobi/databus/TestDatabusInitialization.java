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

  private List<AbstractService> listOfServices;

  public void setUP(String filename) throws Exception {
    configParser = new DatabusConfigParser(filename);
    config = configParser.getConfig();
  }

  /*
   * Unit test for init() method
   * It tests whether all services are correctly populated or not.
   *
   */
  @Test
  public void testDatabusInitialization() throws Exception {

    testLocalStreamService();

    testLocalMergeServices();

    testDatabusAllServices();
  }
  
  @Test
  public void testProdConf() throws Exception {
    setUP("test-prod-databus.xml");
    listOfServices = new ArrayList<AbstractService>();
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("ua2");
    clustersToProcess.add("uj1");
    clustersToProcess.add("lhr1");
    clustersToProcess.add("ua2_main");
    clustersToProcess.add("hkg1");
    Databus databus = new Databus(config, clustersToProcess);
    listOfServices.addAll(databus.init());
    System.out.println("total number of services running in the prod " + 
        listOfServices.size());
    Assert.assertEquals(listOfServices.size(), 31);
    
    //test local stream services
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(assertLocalStreamService());
    }
    Assert.assertFalse(assertLocalStreamService());
    
    //test Data purger services
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(assertDataPurgerService());
    }
    Assert.assertFalse(assertDataPurgerService());
    
    //test Merge stream services
    for (int i= 0; i < 20; i++) {
      Assert.assertTrue(assertMergedStreamService());
    }
    Assert.assertFalse(assertMergedStreamService());
    
    //test mirror stream services
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(assertMirrorStreamService());
    }
    Assert.assertFalse(assertMirrorStreamService());
    
    // list should be empty after removing all the services
    Assert.assertTrue(listOfServices.isEmpty());
  }

  /*
   * testcluster1---- local stream service and purger service will be populated
   */
  private void testLocalStreamService() throws Exception {
    setUP("test-lss-databus.xml");
    listOfServices = new ArrayList<AbstractService>();
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    Databus databus = new Databus(config, clustersToProcess);
    listOfServices.addAll(databus.init());

    Assert.assertEquals(listOfServices.size(), 2);
    // no merge stream service as primary destination is not available
    Assert.assertFalse(assertMergedStreamService());
    // no mirror stream service as non-primary destination is not available
    Assert.assertFalse(assertMirrorStreamService());
    Assert.assertTrue(assertLocalStreamService());
    Assert.assertTrue(assertDataPurgerService());
  }

  /*
   * testcluster1--- local, merge_cluster1_cluster2, merge_cluster2_cluster1
   *                 and purger services (4 services)
   * testcluster2--- merged and purger services  (2 services)
   */
  private void testLocalMergeServices() throws Exception {
    setUP("test-mergedss-databus.xml");
    listOfServices = new ArrayList<AbstractService>();
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    clustersToProcess.add("testcluster2");
    Databus databus = new Databus(config, clustersToProcess);
    listOfServices.addAll(databus.init());

    Assert.assertEquals(listOfServices.size(), 6);

    Assert.assertTrue(assertLocalStreamService());
    Assert.assertTrue(assertLocalStreamService());
    // no mirror stream service as non-primary destination is not available
    Assert.assertFalse(assertMirrorStreamService());
    Assert.assertTrue(assertMergedStreamService());
    Assert.assertTrue(assertMergedStreamService());
    Assert.assertTrue(assertDataPurgerService());
    Assert.assertTrue(assertDataPurgerService());
  }

  /*
   * testcluster1--- local, merge and purger services
   * testcluster2--- merge, mirror and purger services
   * testcluster3--- It is neither source nor destination of any stream, so
   *                 only purger service will be started
   */
  private void testDatabusAllServices() throws Exception {
    setUP("test-merge-mirror-databus.xml");
    listOfServices = new ArrayList<AbstractService>();
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster1");
    clustersToProcess.add("testcluster2");
    clustersToProcess.add("testcluster3");
    Databus databus = new Databus(config, clustersToProcess);
    listOfServices.addAll(databus.init());

    Assert.assertEquals(listOfServices.size(), 7);

    Assert.assertTrue(assertLocalStreamService());
    Assert.assertTrue(assertMergedStreamService());
    Assert.assertTrue(assertMergedStreamService());
    Assert.assertTrue(assertMirrorStreamService());
    Assert.assertFalse(assertMirrorStreamService());
    Assert.assertTrue(assertDataPurgerService());
    Assert.assertTrue(assertDataPurgerService());
    Assert.assertTrue(assertDataPurgerService());
  }

  private boolean assertDataPurgerService() {
    for (AbstractService service : listOfServices) {
      if (service instanceof DataPurgerService) {
        listOfServices.remove(service);
        return true;
      }
    }
    return false;
  }

  protected boolean assertLocalStreamService() {
    for (AbstractService service : listOfServices) {
      if (service instanceof LocalStreamService) {
        listOfServices.remove(service);
        return true;
      }
    }
    return false;
  }

  protected boolean assertMergedStreamService() {
    for (AbstractService service : listOfServices) {
      if (service instanceof MergedStreamService) {
        listOfServices.remove(service);
        return true;
      }
    }
    return false;
  }

  protected boolean assertMirrorStreamService() {
    for (AbstractService service : listOfServices) {
      if (service instanceof MirrorStreamService) {
        listOfServices.remove(service);
        return true;
      }
    }
    return false;
  }
}