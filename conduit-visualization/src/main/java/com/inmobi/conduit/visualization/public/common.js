var publisherSla, agentSla, vipSla, collectorSla, hdfsSla, localSla,
mergeSla, mirrorSla, percentileForSla, percentageForLoss, percentageForWarn,
lossWarnThresholdDiff;
var isGWTDevMode;
var qStream, qCluster, qstart, qend, qSelectedTab, qView;
var hexcodeList = ["#FF9C42", "#DD75DD", "#C69C6E", "#FF86C2", "#F7977A", "#AE8886", "#FB6183", "#8E4804"];
var tierList = ["Publisher", "Agent", "VIP", "Collector", "HDFS", "Local", "Merge", "Mirror"];

String.prototype.equalsIgnoreCase = function (s) {
    return s.toLowerCase() == this.toLowerCase();
}

Array.prototype.contains = function (obj) {
  var i = this.length;
  while (i--) {
    if (this[i] == obj) {
      return true;
    }
  }
  return false;
}

function setConfiguration(pSla, aSla, vSla, cSla, hSla, lSla, meSla, miSla,
percentileFrSla, percentageFrLoss, percentageFrWarn, lossWarnThreshold,
devMode) {

	console.log("setting configuration");
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

function saveHistory(changeParams, streamName, clusterName, selectedTabID, viewId, start, end) {

  console.log("save history with stream:"+streamName+"cluster:"+clusterName+"start:"+start+" end:"+end);
	if (changeParams) {
		console.log("change parameters of url");
		qstart = start;
		qend = end;
		qStream = streamName;
		qCluster = clusterName;
		qSelectedTab = selectedTabID;
		qView = viewId;
	}
  var History = window.History;
  if (History.enabled) {
    var selectedTab = selectedTabID.toString();
    var url = "?";
    if (isGWTDevMode) {
      url += "gwt.codesvr=127.0.0.1:9997&";
    }
    url += "qstart=" + qstart + "&qend=" + qend + "&qstream=" + streamName +
    "&qcluster=" + clusterName + "&selectedTab=" + selectedTabID + "&qview="
    + qView;

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