package com.inmobi.conduit;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.inmobi.conduit.local.LocalStreamService;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class TimeToRunTest {
  
  private static Logger LOG = Logger.getLogger(TimeToRunTest.class);

  @Test
  public void getMSecondsTillNextRun() {
    
    Date date = new Date();
    SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss:SS");
    LOG.info("Test Date [" + format.format(date) + "]");
    try {
      LocalStreamService service = new LocalStreamService(null,
          ClusterTest.buildLocalCluster(), null, null, null);
      
      long mSecondsTillNextMin = service.getMSecondsTillNextRun(date.getTime());
      LOG.info("mSecondsTillNextMin = [" + mSecondsTillNextMin + "]");
      long roundedDate = date.getTime() + mSecondsTillNextMin;
      LOG.info("Rounded Date to next MIN [" + format.format(roundedDate) + "]");
      assert (roundedDate % 60000) == 0;
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
  }

}
