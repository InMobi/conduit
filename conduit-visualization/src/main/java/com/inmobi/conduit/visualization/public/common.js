var publisherSla, agentSla, vipSla, collectorSla, hdfsSla, localSla,
mergeSla, mirrorSla, percentileForSla, percentageForLoss, percentageForWarn,
lossWarnThresholdDiff;
var isGWTDevMode;
var qStream, qCluster, qstart, qend;

function setConfiguration(pSla, aSla, vSla, cSla, hSla, lSla, meSla, miSla,
percentileFrSla, percentageFrLoss, percentageFrWarn, lossWarnThreshold,
devMode) {
	publisherSla = pSla;
  agentSla = aSla;
  vipSla = vSla;
  collectorSla = cSla;
  hdfsSla = hSla;
  localSla = lSla;
  mergeSla = meSla;
  mirrorSla = miSla;
  percentageForLoss = percentageFrLoss;
  percentageForWarn = percentageFrWarn;
  percentileForSla = percentileFrSla;
  lossWarnThresholdDiff = lossWarnThreshold;
  isGWTDevMode = devMode;
}

function saveHistory(streamName, clusterName, selectedTabID, start, end) {
  if (start == undefined || end == undefined) {
    start = qstart;
    end = qend;
  }
  var History = window.History;
  if (History.enabled) {
    var selectedTab = selectedTabID.toString();
    var url = "?";
    if (isGWTDevMode) {
      url += "gwt.codesvr=127.0.0.1:9997&";
    }
    url += "qstart=" + start + "&qend=" + end + "&qstream=" + streamName +
    "&qcluster=" + clusterName + "&selectedTab=" + selectedTabID;

    History.pushState({
        qstream: streamName,
        qcluster: clusterName,
        selectedTab: selectedTab
      }, "Conduit Visualization", url);
  } else {
    console.log("History not enabled");
  }
  History.Adapter.bind(window, 'statechange', function () {
    loadGraph(History.getState()
      .data.qstream, History.getState()
      .data.qcluster, History.getState()
      .data.selectedTab);
  });
}