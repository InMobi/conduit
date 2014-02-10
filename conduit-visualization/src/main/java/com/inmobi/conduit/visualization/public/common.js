var publisherSla, agentSla, vipSla, collectorSla, hdfsSla, localSla,
mergeSla, mirrorSla, percentileForSla, percentageForLoss, percentageForWarn,
lossWarnThresholdDiff;
var isGWTDevMode;
var qStream, qCluster, qstart, qend, qSelectedTab, qView, qTier;
var hexcodeList = ["#FF9C42", "#DD75DD", "#C69C6E", "#FF86C2", "#F7977A", "#AE8886", "#FB6183", "#8E4804"];
var tierList = ["Publisher", "Agent", "VIP", "Collector", "HDFS", "Local", "Merge", "Mirror"];
var isCountView = true;

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

function PercentileLatency(percentile, latency) {
  this.percentile = percentile;
  this.latency = latency;
}

function Point(x, y) {
  this.x = x;
  this.y = y;
}

function setConfiguration(pSla, aSla, vSla, cSla, hSla, lSla, meSla, miSla, percentileFrSla, percentageFrLoss, percentageFrWarn, lossWarnThreshold, devMode) {

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

/* selectedTabID, viewId, start, end are optional and are set only on changing
the query */
function saveHistory(changeParams, streamName, clusterName, tier,
selectedTabID, viewId, start, end) {

  console.log("save history with stream:"+streamName+"cluster:"+clusterName+"start:"+start+" end:"+end);
  if (changeParams) {
    console.log("change parameters of url");
    qstart = start;
    qend = end;
    qStream = streamName;
    qCluster = clusterName;
    qTier = tier;
    qSelectedTab = selectedTabID;
    qView = viewId;
  }
  if (selectedTabID == undefined) {
    selectedTabID = qSelectedTab;
  }
  if (tier == undefined) {
    tier = qTier;
  }
  var History = window.History;
  if (History.enabled) {
    var selectedTab = selectedTabID.toString();
    var url = "?";
    if (isGWTDevMode) {
      url += "gwt.codesvr=127.0.0.1:9997&";
    }
    url += "qstart=" + qstart + "&qend=" + qend + "&qstream=" + streamName +
    "&qcluster=" + clusterName + "&qtier=" + tier + "&selectedTab="
    + selectedTabID + "&qview="
    + qView;

    History.pushState({
        qstream: streamName,
        qcluster: clusterName,
        qtier: tier,
        selectedTab: selectedTabID
      }, "Conduit Visualization", url);
  } else {
    console.log("History not enabled");
  }
  History.Adapter.bind(window, 'statechange', function () {
    setCountLatencyView(History.getState().data.selectedTab);
    highlightTab();
    if (qView == 1) {
      loadGraph(History.getState().data.qstream, History.getState().data.qcluster);
    } else if (qView == 2) {
      highlightTierButton(History.getState().data.qtier);
      renderTimeLineForTierStreamCluster(History.getState().data.qtier, History.getState().data.qstream, History.getState().data.qcluster);
    } else {
      loadGraph(History.getState().data.qstream, History.getState().data.qcluster);
      highlightTierButton(History.getState().data.qtier);
      renderTimeLineForTierStreamCluster(History.getState().data.qtier, History.getState().data.qstream, History.getState().data.qcluster);
    }
  });
}

function saveHistoryAndReload(streamName, clusterName, tier, selectedTabID) {
  if (selectedTabID == undefined) {
    if (isCountView) {
      selectedTabID = 1;
    } else {
      selectedTabID = 2;
    }
  }
  saveHistory(false, streamName, clusterName, tier, selectedTabID);
  /*if (qView == 1) {
    loadGraph(streamName, clusterName);
  } else if (qView == 2) {
    selectAllButton();
    renderTimeLineForTierStreamCluster(tier, streamName, clusterName);
  } else {
    loadGraph(streamName, clusterName);
    selectAllButton();
    renderTimeLineForTierStreamCluster(tier, streamName, clusterName);
  }*/
}

function setCountLatencyView(selectedTabID) {
  if (parseInt(selectedTabID, 10) == 1) {
    isCountView = true;
  } else if (parseInt(selectedTabID, 10) == 2) {
    isCountView = false;
  }
}

function highlightTab() {
  if (isCountView) {
    document.getElementById("count").className = "active";
    document.getElementById("latency").className = "";
  } else {
    document.getElementById("count").className = "";
    document.getElementById("latency").className = "active";
  }
}

function tabSelected(selectedTabID) {
  setCountLatencyView(selectedTabID);
  highlightTab();
  clearSvgAndAddLoadSymbol();
  saveHistoryAndReload(qStream, qCluster, 'all', selectedTabID);
}