var percentileForSla, percentageForLoss, percentageForWarn,
  lossWarnThresholdDiff;
var isGWTDevMode;
var qStream, qCluster, qstart, qend, qSelectedTab, qView, qTier;
var isCountView = true;
var isReloadCompelete = true;
var isReloading = false;
var popupDiv = d3.select("#timelinePanel").append("div")
  .attr("class", "timelinetooltip")
  .attr("id", "popupDiv")
  .style("opacity", 0);
var tierColorMap = {};
tierColorMap["all"] = "#659CEF";
tierColorMap["publisher"] = "#FF9C42";
tierColorMap["agent"] = "#DD75DD";
tierColorMap["vip"] = "#C69C6E";
tierColorMap["collector"] = "#FF86C2";
tierColorMap["hdfs"] = "#F7977A";
tierColorMap["local"] = "#AE8886";
tierColorMap["merge"] = "#FB6183";
tierColorMap["mirror"] = "#8E4804";
var tierLatencySlaMap = {};
var currentStream, currentCluster;
var breadcrumbs = d3.select("#crumbs");

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

function isEmpty(obj) {
  for (var key in obj) {
    if (obj.hasOwnProperty(key))
      return false;
  }
  return true;
}

function PercentileLatency(percentile, latency) {
  this.percentile = percentile;
  this.latency = latency;
}

function Point(x, y) {
  this.x = x;
  this.y = y;
}

function setConfiguration(pSla, aSla, vSla, cSla, hSla, lSla, meSla, miSla,
  percentileFrSla, percentageFrLoss, percentageFrWarn, lossWarnThreshold,
  devMode) {
  console.log("setting configuration");
  tierLatencySlaMap["publisher"] = pSla;
  tierLatencySlaMap["agent"] = aSla;
  tierLatencySlaMap["vip"] = vSla;
  tierLatencySlaMap["collector"] = cSla;
  tierLatencySlaMap["hdfs"] = hSla;
  tierLatencySlaMap["local"] = lSla;
  tierLatencySlaMap["merge"] = meSla;
  tierLatencySlaMap["mirror"] = miSla;
  percentageForLoss = percentageFrLoss;
  percentageForWarn = percentageFrWarn;
  percentileForSla = percentileFrSla;
  lossWarnThresholdDiff = lossWarnThreshold;
  isGWTDevMode = devMode;
}

function resetBreadCrumbsList() {
  breadcrumbs.selectAll("li")
    .filter(function() {
      return this.id != "home";
    })
    .remove();
}

function appendBreadCrumbs(cluster, stream, tier) {
  resetBreadCrumbsList();
  if (!cluster.equalsIgnoreCase("all")) {
    breadcrumbs.append("li")
      .text(">");
    var listEl = breadcrumbs.append("li");
    listEl.append("a")
      .attr("id", cluster)
      .attr("xlink:href", "javascript:void(0)")
      .text(cluster)      
      .style("cursor", "pointer")
      .on("click", function(d) {
        saveHistoryAndReload("All", this.id, "All");
      });
  }
  if (!stream.equalsIgnoreCase("all")) {
    breadcrumbs.append("li")
      .text(">");
    var listEl = breadcrumbs.append("li");
    listEl.append("a")
      .attr("id", cluster+"-"+stream)
      .attr("xlink:href", "javascript:void(0)")
      .text(stream)
      .style("cursor", "pointer")
      .on("click", function(d) {
        var splits = this.id.split("-");
        saveHistoryAndReload(splits[1], splits[0], "All");
      });
  }
  if (!tier.equalsIgnoreCase("all")) {
    breadcrumbs.append("li")
      .text(">");
    var listEl = breadcrumbs.append("li");
    listEl.append("a")
      .attr("id", cluster+"-"+stream+"-"+tier)
      .attr("xlink:href", "javascript:void(0)")
      .text(tier);
  }
}

/* selectedTabID, viewId, start, end are optional and are set only on changing
the query */
function saveHistory(changeParams, streamName, clusterName, tier,
  selectedTabID, viewId, start, end) {
  console.log("save history with stream:" + streamName + "cluster:" +
    clusterName + "start:" + start + " end:" + end);
  currentStream = streamName;
  currentCluster = clusterName;
  resetBreadCrumbsList();
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
      "&qcluster=" + clusterName + "&qtier=" + tier + "&selectedTab=" +
      selectedTabID + "&qview=" + qView;
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
    if (!isReloading) {
      isReloadComplete = false;
    }
    if (!isReloadComplete) {
      setCountLatencyView(History.getState().data.selectedTab);
      highlightTab();
      appendBreadCrumbs(History.getState().data.qcluster, History.getState().data.qstream, History.getState().data.qtier);
      if (qView == 2) {
        loadGraph(History.getState().data.qstream, History.getState().data.qcluster);
      }
      highlightTierButton(History.getState().data.qtier);
      renderTimeLineForTierStreamCluster(History.getState().data.qtier,
          History.getState().data.qstream, History.getState().data.qcluster);
      isReloadComplete = true;
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
  isReloading = true;
  isReloadComplete = false;
  saveHistory(false, streamName, clusterName, tier, selectedTabID);
  if (!isReloadComplete) {
    appendBreadCrumbs(clusterName, streamName, tier);
    if (qView == 2) {
      loadGraph(streamName, clusterName);
    }
    highlightTierButton('all');
    renderTimeLineForTierStreamCluster(tier, streamName, clusterName);
    isReloadComplete = true;
  }
  isReloading = false;
}

function loadFullGraph() {
  saveHistoryAndReload(qStream, qCluster, 'all');  
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

function clearTopologyGraph() {
  d3.select("#graphsvg").remove();
}

function clearSummary() {
  document.getElementById("summaryPanel").innerHTML = "";
  document.getElementById("summaryPanel").style.backgroundColor = "#EBF4F8";
  document.getElementById("infoPanel").innerHTML = "";
  document.getElementById("infoPanel").style.backgroundColor = "#EBF4F8";
}

function clearTrendSVG() {
  d3.select("#trendsvg").remove();
  if (popupDiv != undefined) {
    popupDiv.transition()
      .duration(200)
      .style("opacity", 0);
  };
}

function clearTrendGraph() {
  clearTrendSVG();
  d3.select("#tierButtonPanel").remove();
}

function addLoadSymbol(id, height, width, svgid) {
  var svg = d3.select("#" + id)
    .append("svg:svg")
    .style("stroke", "gray")
    .attr("width", width)
    .attr("height", height)
    .style("background", "#EBF4F8")
    .attr("id", svgid)
    .append("svg:g")
  svg.append("svg:image")
    .attr("xlink:href", "Visualization/bar-ajax-loader.gif")
    .attr("x", width / 2 - 100)
    .attr("y", height / 2)
    .attr("width", "200")
    .attr("height", "40");
}

function clearAllAndAddLoadSymbol(viewId) {
  if (viewId == undefined) {
    viewId = qView;
    clearTrendSVG();
  } else {
    clearTrendGraph();
  }
  if (viewId == 2) {
    clearTopologyGraph();
    addLoadSymbol("topologyPanel", r * 5, r * 5, "graphsvg");
    clearSummary();
    addLoadSymbol("summaryPanel", 150, 350, "summarysvg");
  }
  addLoadSymbol("timelinePanel", 550, 1250, "trendsvg");
}

function tabSelected(selectedTabID) {
  setCountLatencyView(selectedTabID);
  highlightTab();
  clearAllAndAddLoadSymbol();
  saveHistoryAndReload(qStream, qCluster, 'all', selectedTabID);
}