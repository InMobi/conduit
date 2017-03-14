var tierLatencyMap = {};

function setTierLatencyValues(pLatency, aLatency, cLatency, hLatency, lLatency,
  meLatency, miLatency) {
  tierLatencyMap["publisher"] = pLatency;
  tierLatencyMap["agent"] = aLatency;
  tierLatencyMap["collector"] = cLatency;
  tierLatencyMap["hdfs"] = hLatency;
  tierLatencyMap["local"] = lLatency;
  tierLatencyMap["merge"] = meLatency;
  tierLatencyMap["mirror"] = miLatency;
  //loadLatencySummary();
}

function addSummaryBox() {
  if (isCountView) {
    loadCountSummary();
  } else {
    loadLatencySummary();
  }
}

function loadLatencySummary() {
  console.log("loading latency summary");
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
  for (var tier in tierLatencyMap) {
    addTierLatencyDetailsToSummary(div, currentRow, tier, tierLatencySlaMap[
      tier], tierLatencyMap[tier]);
    currentRow++;
  }
}

function calculateCount(n, totalCountMap) {
  switch (n.tier.toLowerCase()) {
    case 'publisher':
      if (totalCountMap['publisher'] == undefined) {
        totalCountMap['publisher'] = 0;
      }
      totalCountMap['publisher'] += n.aggregatemessagesreceived;
      break;
    case 'agent':
      if (totalCountMap['agent'] == undefined) {
        totalCountMap['agent'] = new Object();
      }
      if (totalCountMap['agent']['received'] == undefined) {
        totalCountMap['agent']['received'] = 0;
      }
      totalCountMap['agent']['received'] += n.aggregatemessagesreceived;
      if (totalCountMap['agent']['sent'] == undefined) {
        totalCountMap['agent']['sent'] = 0;
      }
      totalCountMap['agent']['sent'] += n.aggregatemessagesent;
      break;
    case 'collector':
      if (totalCountMap['collector'] == undefined) {
        totalCountMap['collector'] = new Object();
      }
      if (totalCountMap['collector']['received'] == undefined) {
        totalCountMap['collector']['received'] = 0;
      }
      totalCountMap['collector']['received'] += n.aggregatemessagesreceived;
      if (totalCountMap['collector']['sent'] == undefined) {
        totalCountMap['collector']['sent'] = 0;
      }
      totalCountMap['collector']['sent'] += n.aggregatemessagesent;
      break;
    case 'hdfs':
      if (totalCountMap['hdfs'] == undefined) {
        totalCountMap['hdfs'] = 0;
      }
      totalCountMap['hdfs'] += n.aggregatemessagesreceived;
      break;
    case 'local':
      if (totalCountMap['local'] == undefined) {
        totalCountMap['local'] = 0;
      }
      totalCountMap['local'] += n.aggregatemessagesreceived;
      break;
    case 'merge':
      if (totalCountMap['merge'] == undefined) {
        totalCountMap['merge'] = 0;
      }
      totalCountMap['merge'] += n.aggregatemessagesreceived;
      break;
    case 'mirror':
      if (totalCountMap['mirror'] == undefined) {
        totalCountMap['mirror'] = 0;
      }
      totalCountMap['mirror'] += n.aggregatemessagesreceived;
      break;
  }  
  n.children.forEach(function(c) {
    if (!isDummyNode(c)) {
      calculateCount(c, totalCountMap);
    }
  });
}

function loadCountSummary() {
  console.log("loading count summary");
  var totalCountMap = new Object();
  for (var c in rootNodesMap) {
    var tierMap = rootNodesMap[c];
    for (var t in tierMap) {
      var n = tierMap[t];
      calculateCount(n, totalCountMap);
    }
  }
  document.getElementById("summaryPanel").innerHTML = "";
  var div = document.createElement('div');
  var t = document.createElement('table');
  var currentRow = 0;
  t.insertRow(currentRow++)
    .insertCell(0)
    .innerHTML = "<b>Summary:</b>";
  div.appendChild(t);
  document.getElementById("summaryPanel").appendChild(div);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Publisher",
    totalCountMap['publisher'], 0, 0, false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Agent",
    totalCountMap['agent']['received'], totalCountMap['agent']['sent'], totalCountMap['publisher'], false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Collector",
    totalCountMap['collector']['received'], totalCountMap['collector']['sent'], totalCountMap['agent']['sent'], false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "HDFS",
    totalCountMap['hdfs'], 0, totalCountMap['collector']['sent'], false);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Local",
    totalCountMap['local'], 0, totalCountMap['hdfs'], false);
  var comparableLocalNum = getComparableCountValue("LOCAL", "MERGE");
  var comparableMergeNum = getComparableCountValue("MERGE", "MIRROR");
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Merge",
    totalCountMap['merge'], 0, comparableLocalNum, true);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Mirror",
    totalCountMap['mirror'], 0, comparableMergeNum, true);
}

function getComparableCountValue(sourceTier, targetTier) {
  // Return -1 if no nodes of targetTier or sourceTier are found in the rootNodesMap
  // else return corresponding number of messages sent by sourceTier
  var count = -1;
  for (var c in rootNodesMap) {
    var node = rootNodesMap[c][targetTier];
    if (node != undefined) {
      if (count == -1) {
        count = 0;
      }
      for (var t in node.topicSourceMap) {
        node.topicSourceMap[t].forEach(function(cluster) {
          var isFound = false;
          var sourceNode = rootNodesMap[cluster][sourceTier];
          if (sourceNode != undefined) {
            isFound = true;
            var num = sourceNode.topicReceivedMap[t];
            if (num != undefined) {
              count += num;
            }
          }
          if (!isFound) {
            return -1;
          }
        });
      }
    }
  }
  return count;
}

function addTierLatencyDetailsToSummary(div, currentRow, tier, expectedLatency,
  actualLatency) {
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