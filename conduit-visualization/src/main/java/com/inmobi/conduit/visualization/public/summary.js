function setTierLatencyValues(pLatency, aLatency, cLatency, hLatency, lLatency, meLatency, miLatency) {
  publisherLatency = pLatency;
  agentLatency = aLatency;
  collectorLatency = cLatency;
  hdfsLatency = hLatency;
  localLatency = lLatency;
  mergeLatency = meLatency;
  mirrorLatency = miLatency;
  loadLatencySummary();
}

function addSummaryBox(treeList) {
  if (isCountView) {
    loadCountSummary(treeList);
  } else {
    loadLatencySummary();
  }
}

function loadLatencySummary() {
  document.getElementById("summaryPanel")
    .innerHTML = "";
  var div = document.createElement('div');
  var t = document.createElement('table');
  var currentRow = 0;
  t.insertRow(currentRow++)
    .insertCell(0)
    .innerHTML = "<b>Summary:</b>";
  div.appendChild(t);
  document.getElementById("summaryPanel")
    .appendChild(div);
  currentRow = addTierLatencyDetailsToSummary(div, currentRow, "Publisher",
    publisherSla, publisherLatency);
  currentRow = addTierLatencyDetailsToSummary(div, currentRow, "Agent",
    agentSla, agentLatency);
  currentRow = addTierLatencyDetailsToSummary(div, currentRow, "Collector",
    collectorSla, collectorLatency);
  currentRow = addTierLatencyDetailsToSummary(div, currentRow, "HDFS",
    hdfsSla, hdfsLatency);
  currentRow = addTierLatencyDetailsToSummary(div, currentRow, "Local",
    localSla, localLatency);
  currentRow = addTierLatencyDetailsToSummary(div, currentRow, "Merge",
    mergeSla, mergeLatency);
  currentRow = addTierLatencyDetailsToSummary(div, currentRow, "Mirror",
    mirrorSla, mirrorLatency);
}

function loadCountSummary(treeList) {
  var publisherCount = 0,
    agentReceivedCount = 0,
    agentSentCount = 0,
    collectorReceivedCount = 0,
    collectorSentCount = 0,
    hdfsCount = 0,
    localCount = 0,
    mergeCount = 0,
    mirrorCount = 0;
  treeList.forEach(function (cl) {
    cl.forEach(function (n) {
      switch (n.tier.toLowerCase()) {
      case 'publisher':
        publisherCount += parseInt(n.aggregatemessagesreceived, 10);
        break;
      case 'agent':
        agentReceivedCount += parseInt(n.aggregatemessagesreceived, 10);
        agentSentCount += parseInt(n.aggregatemessagesent, 10);
        break;
      case 'collector':
        collectorReceivedCount += parseInt(n.aggregatemessagesreceived, 10);
        collectorSentCount += parseInt(n.aggregatemessagesent, 10);
        break;
      case 'hdfs':
        hdfsCount += parseInt(n.aggregatemessagesreceived, 10);
        break;
      case 'local':
        localCount += parseInt(n.aggregatemessagesreceived, 10);
        break;
      case 'merge':
        mergeCount += parseInt(n.aggregatemessagesreceived, 10);
        break;
      case 'mirror':
        mirrorCount += parseInt(n.aggregatemessagesreceived, 10);
        break;
      }
    });
  });
  document.getElementById("summaryPanel")
    .innerHTML = "";
  var div = document.createElement('div');
  var t = document.createElement('table');
  var currentRow = 0;
  t.insertRow(currentRow++)
    .insertCell(0)
    .innerHTML = "<b>Summary:</b>";
  div.appendChild(t);
  document.getElementById("summaryPanel")
    .appendChild(div);

  currentRow = addTierCountDetailsToSummary(div, currentRow, "Publisher",
    publisherCount, 0, 0, false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Agent",
    agentReceivedCount, agentSentCount, publisherCount, false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Collector",
    collectorReceivedCount, collectorSentCount, agentSentCount, false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "HDFS",
    hdfsCount, 0, collectorSentCount, false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Local",
    localCount, 0, hdfsCount, false);

  var comparableLocalNum = getComparableCountValue("local", "merge", treeList);
  var comparableMergeNum = getComparableCountValue("merge", "mirror", treeList);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Merge",
    mergeCount, 0, comparableLocalNum, true);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Mirror",
    mirrorCount, 0, comparableMergeNum, true);
}

function getComparableCountValue(sourceTier, targetTier, treeList) {
  var count = -1;
  for (var i = 0; i < rootNodes.length; i++) {
    if (rootNodes[i].tier.equalsIgnoreCase(targetTier)) {
      if (count == -1) {
        count = 0;
      }
      var tnode = rootNodes[i];
      tnode.streamSourceList.forEach(function (topicList) {
        var topic = topicList.topic;
        topicList.source.forEach(function (cluster) {
          var isFound = false;
          for (var i = 0; i < rootNodes.length; i++) {
            if (rootNodes[i].tier.equalsIgnoreCase(sourceTier) && rootNodes[i].clusterName == cluster) {
              isFound = true;
              rootNodes[i].allreceivedtopicstats.forEach(function (stats) {
                if (stats.topic == topic) {
                  count += stats.messages;
                }
              });
            }
          }
          if (!isFound) {
            return -1;
          }
        });
      });
    }
  }
  return count;
}

function addTierLatencyDetailsToSummary(div, currentRow, tier, expectedLatency,  actualLatency) {
  var health;
  /*
    If expectedLatency = 1 and lossWarnThresholdDiff = 1 (minimum values),
    then
      status = healthy for latency <= 0(diff)
      status = warn for 0(diff) < latency <= 1(expected)
      status = unhealthy for latency > 1(expected)
      status = no data if no latency information is available
  */
  if (actualLatency == -1) {
    health = 4;
  } else if (actualLatency <= expectedLatency - lossWarnThresholdDiff) {
    health = 0;
  } else if (actualLatency <= expectedLatency && actualLatency >
  expectedLatency - lossWarnThresholdDiff) {
    health = 1;
  } else if (actualLatency > expectedLatency) {
    health = 2;
  }
  var id = "counthealthCell" + tier;
  var t = div.firstChild;
  var r, c, currentCol = 0;
  r = t.insertRow(currentRow++);
  c = r.insertCell(currentCol++);
  c.innerHTML = tier;
  c = r.insertCell(currentCol++);
  c.id = id;
  c.style.width = "15px";
  c.style.height = "15px";
  appendHealthStatusIndicator(id, health);
  if (health != 4) {
    c = r.insertCell(currentCol++);
    c.innerHTML = actualLatency + "(A)";
    c = r.insertCell(currentCol++);
    c.innerHTML = expectedLatency + "(E)";
  }
  return currentRow;
}

function appendHealthStatusIndicator(id, health) {
  /*
    Color indicators:
    Green - Healthy
    Yellow - Warn
    Red - Unhealthy
    Orange - Excess
    Blue - No Data
  */
  var svg = d3.select("#" + id)
    .append("svg:svg")
    .attr("width", 12)
    .attr("height", 12)
    .style("display", "block")
    .append("svg:g");
  var div = d3.select("body")
    .append("div")
    .attr("class", "healthtooltip")
    .style("opacity", 0);
  var circle = svg.append("svg:circle")
    .attr("r", 5)
    .attr("cx", 6)
    .attr("cy", 6)
    .style("display", "block")
    .style("cursor", "pointer")
    .on("mouseout", function () {
      div.transition()
        .duration(500)
        .style("opacity", 0);
    });
  var color, status;
  switch (health) {
  case 0:
    color = "#0f0";
    status = "Healthy";
    break;
  case 1:
    color = "#ff0";
    status = "Warn";
    break;
  case 2:
    color = "#f00";
    status = "Unhealthy";
    break;
  case 3:
    color = "#FFA500";
    status = "Excess";
    break;
  case 4:
    color = "#b4d7e8";
    status = "No Data";
    break;
  }
  circle.style("fill", color)
    .style("stroke", color)
    .on("mouseover", function () {
      div.transition()
        .duration(200)
        .style("opacity", 0.8)
        .style("background", color);
      div.html(status)
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 14) + "px");
    });
}

function addTierCountDetailsToSummary(div, currentRow, tier, received, sent,
  childCount) {
  var health;
  if (tier == 'Publisher' && received != 0) {
    health = 0;
  } else if (tier == 'Publisher' && received == 0) {
    health = 4;
  } else {
    health = getHealth(received, childCount);
  }
  var id = "counthealthCell" + tier;
  var t = div.firstChild;
  var r, c, currentCol = 0;
  r = t.insertRow(currentRow++);
  c = r.insertCell(currentCol++);
  c.innerHTML = tier;
  c = r.insertCell(currentCol++);
  c.id = id;
  c.style.width = "15px";
  c.style.height = "15px";
  if (received != 0) {
    c = r.insertCell(currentCol++);
    c.innerHTML = received + "(R)";
  }
  if (sent != 0) {
    c = r.insertCell(currentCol++);
    c.innerHTML = sent + "(S)";
  }
  appendHealthStatusIndicator(id, health);
  return currentRow;
}

function isWarn(count1, count2) {
  var marginAllowed = parseFloat(parseInt(percentageForWarn * count2) / 100);
  if (count1 < (count2 - marginAllowed))
    return true;
  else
    return false;
}

function getHealth(count1, count2) {
  if (count1 == 0) {
    return 4;
  } else if (count1 > count2) {
    return 3;
  } else if (isWarn(count1, count2) && !isLoss(count1, count2)) {
    return 1;
  } else if (isLoss(count1, count2)) {
    return 2;
  } else {
    return 0;
  }
}