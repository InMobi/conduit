var jsonresponse;
var queryString;
var collectorIndex = 0;
var r = 180;
var fullTreeList = []; // Full list of Node objects grouped by respective cluster name
var hexcodeList = ["#FF9C42", "#DD75DD", "#C69C6E", "#FF86C2", "#F7977A", "#f96",
  "#ff0", "#ff0080"];
var agentSla, vipSla, collectorSla, hdfsSla, localSla, mergeSla, mirrorSla,
    percentileForSla, percentageForLoss;

function TopicStats(topic, messages, hostname) {
  this.topic = topic;
  this.messages = messages;
  this.hostname = hostname;
}

function PercentileLatency(percentile, latency) {
  this.percentile = percentile;
  this.latency = latency;
}

function TopicLatency(topic) {
  this.topic = topic;
  this.latencyList = [];
}

function Node(name, cluster, tier, aggregatemessagesreceived,
  aggregatemessagesent) {
  this.name = name;
  this.cluster = cluster;
  this.tier = tier;
  this.aggregatemessagesreceived = aggregatemessagesreceived;
  this.aggregatemessagesent = aggregatemessagesent;
  this.allreceivedtopicstats = [];
  this.allsenttopicstats = [];
  this.children = [];
  this.source = []; //for merge and mirror tier nodes
  this.overallLatency = [];
  this.allTopicsLatency = [];
}

function buildNodeList() {
  jsonresponse.nodes.forEach(function (n) {
    var node = new Node(n.name, n.cluster, n.tier, n.aggregatereceived, n.aggregatesent);
    n.receivedtopicStatsList.forEach(function (t) {
      if (t.hostname != undefined)
        node.allreceivedtopicstats.push(new TopicStats(t.topic, t.messages,
          t.hostname));
      else
        node.allreceivedtopicstats.push(new TopicStats(t.topic, t.messages));
    });
    n.senttopicStatsList.forEach(function (t) {
      if (t.hostname != undefined)
        node.allsenttopicstats.push(new TopicStats(t.topic, t.messages, t.hostname));
      else
        node.allsenttopicstats.push(new TopicStats(t.topic, t.messages));
    });
    if (n.tier == "merge" || n.tier == "mirror") {
      node.source = n.source;
    }
    n.overallLatency.forEach(function (l) {
      node.overallLatency.push(new PercentileLatency(l.percentile, l.latency));
    });
    n.topicLatency.forEach(function (t) {
      var topicLatency = new TopicLatency(t.topic);
      t.percentileLatencyList.forEach(function (l) {
        topicLatency.latencyList.push(new PercentileLatency(l.percentile,
          l.latency));
      });
      node.allTopicsLatency.push(topicLatency);
    });
    var isAdded = false;
    for (var j = 0; j < fullTreeList.length; j++) {
      var clusterList = fullTreeList[j];
      if (node.cluster == clusterList[0].cluster) {
        clusterList.push(node);
        isAdded = true;
        break;
      }
    }
    if (!isAdded) {
      var clusterList = [];
      clusterList.push(node);
      fullTreeList.push(clusterList);
    }
  });
}

function isLoss(parentCount, childCount) {
	var marginAllowed = percentageForLoss * childCount / 100;
	if ( parentCount < (childCount - marginAllowed))
		return true;
	else
		return false;
}

function highlightChildNodes(n) {
  if (n.tier.toLowerCase() == "hdfs") {
    n.children.forEach(function (c) {
      var aggregateparent = 0;
      var aggregatechild = 0;
      var link = d3.selectAll("path.link")
        .filter(function (d) {
          return d.source.cluster == n.cluster && d.source.name == n.name &&
            d.source.tier == n.tier && d.target.name == c.name;
        })
        .transition()
        .duration(100);
      if (c.allsenttopicstats.length == 0)
        link.style("fill", "#dedede")
          .style("stroke", "#dedede");
      else {
        n.allreceivedtopicstats.forEach(function (t) {
          if (t.hostname == c.name) {
            c.allsenttopicstats.forEach(function (ct) {
              if (ct.topic == t.topic) {
                aggregateparent += t.messages;
                aggregatechild += ct.messages;
              }
            });
          }
        });
        if (n.allreceivedtopicstats.length == 0)
          link.style("fill", "#dedede")
            .style("stroke", "#dedede");
        else if (isLoss(aggregateparent,aggregatechild))
          link.style("fill", "#ff0000")
            .style("stroke", "#ff0000");
        else
          link.style("fill", "#00ff00")
            .style("stroke", "#00ff00");
      }
    });
  } else {
    var totalaggregatechild = 0;
    var totalaggregateparent = 0;
    n.children.forEach(function (c) {
      totalaggregatechild += parseFloat(c.aggregatemessagesreceived);
    });
    if (n.tier.toLowerCase() == "vip") {
    	if (n.children.length == 0) {
	      var arr = d3.selectAll("g.node")
	        .filter(function (d) {
	          return d.tier == n.tier && d.cluster == n.cluster && (d.children.length >
	            0);
	        })
	        .data();
	      n = arr[0];    		
    	}
      n.children.forEach(function (c) {
        totalaggregatechild += parseFloat(c.aggregatemessagessent);
      });
    }
    if (n.tier.toLowerCase() == "collector") {
      var nodesInCluster = d3.selectAll("g.node")
        .filter(function (d) {
          return d.cluster == n.cluster;
        })
        .data();
      nodesInCluster.forEach(function (c) {
        if (c.tier.toLowerCase() == "collector")
          totalaggregateparent += parseFloat(c.aggregatemessagesreceived);
      });
    } else {
      totalaggregateparent = n.aggregatemessagesreceived;
    }
    n.children.forEach(function (c) {
      var currentLink = d3.selectAll("path.link")
        .filter(function (d) {
          return d.source.cluster == n.cluster && d.source.name == n.name &&
            d.source.tier == n.tier && d.target.name == c.name;
        })
        .transition()
        .duration(100);
      if (n.allreceivedtopicstats.length == 0)
        currentLink.style("fill", "#dedede").style("stroke", "#dedede");
      else if ((c.tier == "agent" || c.tier == "collector") && c.allsenttopicstats.length == 0)
        currentLink.style("fill", "#dedede").style("stroke", "#dedede");
      else if (c.allreceivedtopicstats.length == 0)
        currentLink.style("fill", "#dedede").style("stroke", "#dedede");
      else if (isLoss(totalaggregateparent,totalaggregatechild))
        currentLink.style("fill", "#ff0000").style("stroke", "#ff0000");
      else
        currentLink.style("fill", "#00ff00").style("stroke", "#00ff00");
    });
  }
  n.children.forEach(function (c) {
    highlightChildNodes(c);
  });
}

function nodeover(n, isCountView, clear) {
  if (clear) {
    d3.selectAll("path.link")
      .transition()
      .duration(100)
      .style("fill", "#dedede")
      .style("stroke", "#dedede");
  }
  
  if (n.tier == "merge" || n.tier == "mirror")
	  highlightBasedOnTier(n, isCountView); 
	else if(isCountView)
	  highlightChildNodes(n);
	else
		latencyhighlightChildNodes(n);
}

function latencyhighlightChildNodes(n) {
  if (n.children.length == 0 && n.tier.toLowerCase() == "vip") {
    var arr = d3.selectAll("g.node")
      .filter(function (d) {
        return d.tier == n.tier && d.cluster == n.cluster && (d.children.length >
          0);
      })
      .data();
    n = arr[0];
  }
  var graphnode = d3.selectAll("g.node")
    .select("circle")
    .filter(function (d) {
      return d.name == n.name && d.cluster == n.cluster && d.tier == n.tier;
    })
    .transition()
    .duration(100);
  var color = undefined;

  n.overallLatency.forEach(function (l) {
    if (l.percentile == percentileForSla) {
      var sla;
      switch (n.tier.toLowerCase()) {
      case 'agent':
        sla = agentSla;
        break;
      case 'vip':
        sla = vipSla;
        break;
      case 'collector':
        sla = collectorSla;
        break;
      case 'hdfs':
        sla = hdfsSla;
        break;
      case 'local':
        sla = localSla;
        break;
      case 'merge':
        sla = mergeSla;
        break;
      case 'mirror':
        sla = mirrorSla;
        break;
      }
      if (l.latency > sla)
        color = '#ff0000';
    }
  });
  if (color != undefined) {
    graphnode.style("fill", color);
  }

  n.children.forEach(function (c) {
    d3.selectAll("path.link")
      .filter(function (d) {
        return d.source.cluster == n.cluster && d.source.name == n.name &&
          d.source.tier == n.tier && d.target.name == c.name && n.allTopicsLatency
          .length > 0 && c.allTopicsLatency.length > 0;
      })
      .transition()
      .duration(100)
      .style("fill", "#ADD8E6")
      .style("stroke", "#ADD8E6");

    latencyhighlightChildNodes(c);
  });
}

function highlightBasedOnTier(n, isCountView) {
  d3.selectAll("g.node")
    .filter(function (d) {
      return d.cluster == n.cluster && d.name == n.name && d.tier == n.tier;
    })
    .select("circle")
    .transition()
    .duration(100)
    .attr("r", 6)
    .style("fill","#00ff00")
    .style("stroke", "#00ff00");
  if (n.tier == "merge") {
    n.source.forEach(function (s) {
      var arr = d3.selectAll("g.node")
        .filter(function (d) {
          return d.cluster == s && d.tier.toLowerCase() == "local";
        })
        .data();
      highlightChildNodes(arr[0], isCountView);
    });
  } else if (n.tier == "mirror") {
    n.source.forEach(function (s) {
      var arr = d3.selectAll("g.node")
        .filter(function (d) {
          return d.cluster == s && d.tier.toLowerCase() == "merge";
        })
        .data();
      highlightBasedOnTier(arr[0], isCountView);
    });
  }
}

function nodeclick(n) {
  document.getElementById("infoPanel")
    .innerHTML = "";
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Name";
  c = r.insertCell(1);
  c.innerHTML = n.name;
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Tier";
  c = r.insertCell(1);
  c.innerHTML = n.tier;
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Cluster";
  c = r.insertCell(1);
  c.innerHTML =
    "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('all', '" + n.cluster +
    "', true)\" style=\"cursor: pointer; cursor: hand;color:#00f;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px\">" +
    n.cluster + "</button>";
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Aggregate Recieved";
  c = r.insertCell(1);
  c.innerHTML = n.aggregatemessagesreceived;
  currentRow++;
  if (n.tier == "agent" || n.tier == "collector") {
    r = t.insertRow(currentRow);
    c = r.insertCell(0);
    c.innerHTML = "Aggregate Sent";
    c = r.insertCell(1);
    c.innerHTML = n.aggregatemessagesent;
    currentRow++;
    r = t.insertRow(currentRow);
    c = r.insertCell(0);
    c.innerHTML = "<b>Topic</b>";
    c = r.insertCell(1);
    c.innerHTML = "<b>Received</b>";
    c = r.insertCell(2);
    c.innerHTML = "<b>Sent</b>";
    currentRow++;
    n.allreceivedtopicstats.forEach(function (receivedStats) {
      var received = receivedStats.messages,
        sent;
      n.allsenttopicstats.forEach(function (sentStats) {
        if (receivedStats.topic == sentStats.topic) {
          sent = sentStats.messages;
        }
      });
      r = t.insertRow(currentRow);
      c = r.insertCell(0);
      c.innerHTML =
        "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" +
        receivedStats.topic + "', '" + n.cluster +
        "', true)\" style=\"cursor: pointer; cursor: hand;;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px\">" +
        receivedStats.topic + "</button>";
      if (sent != received) {
        c.firstChild.style.color = "#ff0000";
      } else {
        c.firstChild.style.color = "#00ff00";
      }
      c = r.insertCell(1);
      c.innerHTML = received;
      c = r.insertCell(2);
      c.innerHTML = sent;
      currentRow++;
    });
  } else {
    var cell = 0;
    r = t.insertRow(currentRow);
    if (n.tier.toLowerCase() == "hdfs") {
      c = r.insertCell(cell);
      c.innerHTML = "<b>Collector</b>";
      cell++;
    }
    c = r.insertCell(cell);
    c.innerHTML = "<b>Topic</b>";
    cell++;
    c = r.insertCell(cell);
    c.innerHTML = "<b>Received</b>";
    currentRow++;
    n.allreceivedtopicstats.forEach(function (topicstats) {
      r = t.insertRow(currentRow);
      var cell = 0;
      if (n.tier.toLowerCase() == "hdfs") {
        c = r.insertCell(cell);
        c.innerHTML = topicstats.hostname;
        cell++;
      }
      c = r.insertCell(cell);
      c.innerHTML =
        "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" +
        topicstats.topic + "', '" + n.cluster +
        "', true)\" style=\"cursor: pointer; cursor: hand;color:#00f;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px\">" +
        topicstats.topic + "</button>";
      cell++;
      c = r.insertCell(cell);
      c.innerHTML = topicstats.messages;
      currentRow++;
    });
  }
  document.getElementById("infoPanel")
    .appendChild(t);
}

function latencynodeclick(n) {
  console.log(n);
  document.getElementById("infoPanel")
    .innerHTML = "";
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Name";
  c = r.insertCell(1);
  c.innerHTML = n.name;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Tier";
  c = r.insertCell(1);
  c.innerHTML = n.tier;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Cluster";
  c = r.insertCell(1);
  c.innerHTML =
    "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('all', '" + n.cluster +
    "', false)\" style=\"cursor: pointer; cursor: hand;color:#00f;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px\">" +
    n.cluster + "</button>";
  var percentileSet = [];
  n.overallLatency.forEach(function (l) {
    percentileSet.push(l.percentile);
  });
  percentileSet.sort(function (a, b) {
    return a - b
  });
  var currentCol = 0;
  r = t.insertRow(currentRow++);
  c = r.insertCell(currentCol++);
  c.innerHTML = "<b>Overall/Topic</b>";
  percentileSet.forEach(function (p) {
    c = r.insertCell(currentCol++);
    c.innerHTML = "<b>"+p+"</b>";
  });
  n.allTopicsLatency.forEach(function (l) {
    var topic = l.topic;
    r = t.insertRow(currentRow++);
    var currentColumn = 0;
    c = r.insertCell(currentColumn++);
    c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" +
      topic + "', '" + n.cluster +
      "', false)\" style=\"cursor: pointer; cursor: hand;color:#00f;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px\">" +
      topic + "</button>";
    percentileSet.forEach(function (p) {
      var currentPercentile = p;
      l.latencyList.forEach(function (pl) {
        if (pl.percentile == currentPercentile) {
          c = r.insertCell(currentColumn++);
          if (pl.latency == 0)
            c.innerHTML = "< 1";
          else
            c.innerHTML = pl.latency;
        }
      });
    });
  });
  currentCol = 0;
  r = t.insertRow(currentRow++);
  c = r.insertCell(currentCol++);
  c.innerHTML = "Overall";
  percentileSet.forEach(function (p) {
    var currentPercentile = p;
    n.overallLatency.forEach(function (pl) {
      if (pl.percentile == currentPercentile) {
        c = r.insertCell(currentCol++);
        if (pl.latency == 0)
          c.innerHTML = "< 1";
        else
          c.innerHTML = pl.latency;
      }
    });
  });
  document.getElementById("infoPanel")
    .appendChild(t);
}

function getStreamsCausingDataLoss(l) {
  var streamslist = [];
  var isstreampresent = false;
  if (l.source.tier.toLowerCase() == "hdfs") {
    l.target.allsenttopicstats.forEach(function (t) {
      l.source.allreceivedtopicstats.forEach(function (s) {
        if (t.topic == s.topic && s.hostname == l.target.name) {
          isstreampresent = true;
          if (t.messages > s.messages)
            streamslist.push(t.topic);
        }
      });
      if (!isstreampresent && !(streamslist.contains(t.topic)))
        streamslist.push(t.topic);
      isstreampresent = false;
    });
  } else {
    l.target.allreceivedtopicstats.forEach(function (t) {
      l.source.allreceivedtopicstats.forEach(function (s) {
        if (t.topic == s.topic) {
          isstreampresent = true;
          if (t.messages > s.messages)
            streamslist.push(t.topic);
        }
      });
      if (!isstreampresent && !(streamslist.contains(t.topic)))
        streamslist.push(t.topic);
      isstreampresent = false;
    });
  }
  return streamslist;
}

function linkclick(l) {
  document.getElementById("infoPanel")
    .innerHTML = "";
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Source Name:";
  c = r.insertCell(1);
  c.innerHTML = l.source.name;
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Source Tier:";
  c = r.insertCell(1);
  c.innerHTML = l.source.tier;
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Source Cluster:";
  c = r.insertCell(1);
  c.innerHTML =
    "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('all', '" + l.source
    .cluster +
    "', true)\" style=\"cursor: pointer; cursor: hand;color:#00f;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px\">" +
    l.source.cluster + "</button>";
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Target Name:";
  c = r.insertCell(1);
  c.innerHTML = l.target.name;
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Target Tier:";
  c = r.insertCell(1);
  c.innerHTML = l.target.tier;
  currentRow++;
  r = t.insertRow(currentRow);
  c = r.insertCell(0);
  c.innerHTML = "Target Cluster:";
  c = r.insertCell(1);
  c.innerHTML =
    "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('all', '" + l.target
    .cluster +
    "', true)\" style=\"cursor: pointer; cursor: hand;color:#00f;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px\">" +
    l.target.cluster + "</button>";
  currentRow++;
  var streams = getStreamsCausingDataLoss(l);
  if (streams.length > 0) {
    r = t.insertRow(currentRow);
    c = r.insertCell(0);
    c.innerHTML = "<b>Streams causing dataloss: </b>";
    currentRow++;
    streams.forEach(function (s) {
      r = t.insertRow(currentRow);
      c = r.insertCell(0);
      c.innerHTML =
        "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" + s +
        "', '" + l.source.cluster +
        "', true)\" style=\"cursor: pointer; cursor: hand;color:#00f;display:block;width:100%;height:100%;text-decoration:none;text-align:left;background:#fff;border:#fff;padding:0px;margin:0px;color:#ff0000\">" +
        s + "</button>";
      currentRow++;
    });
  }
  document.getElementById("infoPanel")
    .appendChild(t);
}

function travelTreeAndSetCollectorChildList(node) {
  var collectorchildren;
  if (node.tier == "hdfs") {
    if (node.children.length > 0) {
      node.children.forEach(function (c) {
        if (c.children.length > 0) {
          collectorchildren = c.children;
        }
      });
      node.children.forEach(function (c) {
        if (c.children.length == 0) {
          c.children = collectorchildren;
        }
      });
    }
  }
}

function createNewObjectForTree(c, clusterNodeList, root) {
  var h = new Object();
  h.name = c.name;
  h.tier = c.tier;
  h.cluster = c.cluster;
  h.aggregatemessagesreceived = c.aggregatemessagesreceived;
  h.allreceivedtopicstats = c.allreceivedtopicstats;
  h.aggregatemessagesent = c.aggregatemessagesent;
  h.allsenttopicstats = c.allsenttopicstats;
  h.overallLatency = c.overallLatency;
  h.allTopicsLatency = c.allTopicsLatency;
  if (root.tier.toLowerCase() == "collector" && c.tier.toLowerCase() == "vip") {
    if (collectorIndex == 0) {
      h.children = travelTree(c, clusterNodeList);
    } else {
      h.children = [];
    }
    collectorIndex++;
  } else
    h.children = travelTree(c, clusterNodeList);
  h.parent = [];
  h.parent.push(root);
  return h;
}

function travelTree(root, clusterNodeList) {
  var returnArray = [];
  var numCollectors;
  clusterNodeList.forEach(function (c) {
    var createNode = false;
    if (root.tier.toLowerCase() == "local" && c.tier.toLowerCase() == "hdfs") {
      createNode = true;
    } else if (root.tier.toLowerCase() == "hdfs" && c.tier.toLowerCase() ==
      "collector") {
      createNode = true;
    } else if (root.tier.toLowerCase() == "collector" && c.tier.toLowerCase() ==
      "vip") {
      createNode = true;
    } else if (root.tier.toLowerCase() == "vip" && c.tier.toLowerCase() ==
      "agent") {
      createNode = true;
    } else if (root.tier.toLowerCase() == "agent" && c.tier.toLowerCase() ==
      "publisher" && root.name == c.name) {
      createNode = true;
    }
    if (createNode)
      returnArray.push(createNewObjectForTree(c, clusterNodeList, root));
  });
  return returnArray;
}

function getNumOfNodes(nodes, tier) {
  var num = 0;
  nodes.forEach(function (n) {
    if (n.tier == tier)
      num++;
  });
  return num;
}

function setNodesAngles(angle, diff, nodes, isRootHdfs) {
  var numCollectors = getNumOfNodes(nodes, "collector");
  var numAgents = getNumOfNodes(nodes, "agent");
  var numPublishers = getNumOfNodes(nodes, "publisher");
  var collectorAngleDiff = diff / (numCollectors + 1);
  var collectorAngle = (angle - (diff / 2)) + collectorAngleDiff;
  var agentAngleDiff = diff / (numAgents + 1);
  var agentAngle = (angle - (diff / 2)) + agentAngleDiff;
  var publisherAngleDiff = diff / (numPublishers + 1);
  var publisherAngle = (angle - (diff / 2)) + publisherAngleDiff;
  yDiff = 40;
  nodes.forEach(function (n) {
    if (n.tier.toLowerCase() == "local") {
      n.x = angle;
      n.y = 3 * yDiff;
    } else if (n.tier.toLowerCase() == "hdfs") {
      n.x = angle;
      n.y = 4 * yDiff;
    } else if (n.tier.toLowerCase() == "vip") {
      n.x = angle;
      n.y = 6 * yDiff;
    } else if (n.tier.toLowerCase() == "collector") {
      n.x = collectorAngle;
      collectorAngle += collectorAngleDiff;
      n.y = 5 * yDiff;
    } else if (n.tier.toLowerCase() == "agent") {
      n.x = agentAngle;
      agentAngle += agentAngleDiff;
      n.y = 7 * yDiff;
    } else if (n.tier.toLowerCase() == "publisher") {
      n.x = publisherAngle;
      publisherAngle += publisherAngleDiff;
      n.y = 8 * yDiff;
    }
  });
  return yDiff;
}

function drawLinesToMarkDifferentClusters(graphsvg, divisions, r) {
  if (divisions != 1) {
    var angle = 2 * Math.PI / divisions
    var radialPoints = []
    for (var k = 0; k < divisions; k++)
      radialPoints.push([r * Math.cos((angle * k) - (Math.PI / 2)), r * Math.sin(
        (angle * k) - (Math.PI / 2))]);
    graphsvg.selectAll("line")
      .data(radialPoints)
      .enter()
      .append("svg:line")
      .attr(
        "x1", 0)
      .attr("y1", 0)
      .attr("x2", function (p) {
        return p[0]
      })
      .attr("y2", function (p) {
        return p[1]
      })
      .attr("stroke", "#ccc");
  }
}

function drawConcentricCircles(graphsvg, yDiff, divisions) {
  var rad = [];
  var index = 8;
  for (var i = 1; i <= index; i++)
    rad.push(i * yDiff);
  rad.sort(function (a, b) {
    return a - b
  });
  for (var i = rad.length; i > -1; i--) {
    graphsvg.append("circle")
      .attr("r", rad[i])
      .style("fill", "white")
      .style(
        "stroke-dasharray", ("3, 3"))
      .style("stroke", "#ccc");
  }
  var r = rad[rad.length - 1] + yDiff;
  drawLinesToMarkDifferentClusters(graphsvg, divisions, r);
}

function populateMergeMirrorList(clusterNodeList) {
  var returnArray = [];
  clusterNodeList.forEach(function (n) {
    if (n.tier == "merge" || n.tier == "mirror")
      returnArray.push(n);
  });
  return returnArray;
}

function addClusterName(clusterName, tree, angle, yDiff, graphsvg, isCountView) {
  var clusternamenode = new Node(clusterName, clusterName, "clusterName");
  clusterNameTreeNode = tree.nodes(clusternamenode);
  clusterNameTreeNode[0].x = angle;
  clusterNameTreeNode[0].y = (9 * yDiff);
  var clusterNode = graphsvg.selectAll("g.node")
    .filter(function (d, i) {
      return d.cluster == clusterName && d.tier == "clusterName";
    })
    .data(clusterNameTreeNode)
    .enter()
    .append("svg:g")
    .attr("class",
      "clusternamenode")
    .attr("transform", function (d) {
      return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")";
    });
  clusterNode.append("text")
    .attr("x", function (d) {
      return d.x < 180 ? 5 : -5;
    })
    .attr("y", 10)
    .attr("text-anchor", function (d) {
      return d.x < 180 ? "start" : "end";
    })
    .attr("transform", function (d) {
      return d.x < 180 ? null : "rotate(180)";
    })
    .text(function (d) {
      return d.name;
    })
    .style("cursor", "hand")
    .style("cursor", "pointer")
    .on("click", function (d) {
      saveHistoryAndLoadGraph('all', d.name, isCountView);
    });
}

function addColorsToNodes() {
  d3.selectAll("g.node")
    .select("circle")
    .style("fill", function (d) {
      var color = "#ccc";
      switch (d.tier.toLowerCase()) {
      case "publisher":
        color = hexcodeList[0];
        break;
      case "agent":
        if (d.aggregatemessagesent < d.aggregatemessagesreceived)
          color = "#ff0000";
        else
          color = hexcodeList[1];
        break;
      case "vip":
        color = hexcodeList[2];
        break;
      case "collector":
        if (d.aggregatemessagesent < d.aggregatemessagesreceived)
          color = "#ff0000";
        else
          color = hexcodeList[3];
        break;
      case "hdfs":
        color = hexcodeList[4];
        break;
      case "local":
        color = hexcodeList[5];
        break;
      case "merge":
        color = hexcodeList[6];
        break;
      case "mirror":
        color = hexcodeList[7];
        break;
      }
      return color;
    });
}

function addLegendBox(graphsvg) {
  var tierList = ["Publisher", "Agent", "VIP", "Collector", "HDFS"];
  var inc = 15;
  for (var i = 0; i < tierList.length; i++) {
    graphsvg.append("circle")
      .attr("class", "legendColor")
      .attr("r", 5)
      .attr(
        "cx", -r * 2.5 + 10)
      .attr("cy", -r * 2.5 + 10 + (i) * 15)
      .style("fill",
        hexcodeList[i])
      .style("stroke", hexcodeList[i]);
    graphsvg.append("text")
      .attr("class", "legend")
      .text(tierList[i])
      .attr("x", -
        r * 2.5 + 20)
      .attr("y", -r * 2.5 + (i + 1) * 15);
  }
}

function loadDefaultView(isCountView) {
  var nodeOverList = d3.selectAll("g.node")
    .filter(function (d) {
      return d.tier.toLowerCase() == "local" || d.tier.toLowerCase() ==
        "hdfs";
    })
    .data();
  var clear = true;
  nodeOverList.forEach(function (n) {
    nodeover(n, isCountView, clear);
    if(clear)
    	clear = false;        
  });
  addColorsToNodes();
}

function clearHistory() {

}

function clearPreviousGraph() {
  document.getElementById("infoPanel").innerHTML = "";
  d3.select("#graphsvg").remove();
}

function saveHistory(streamName, clusterName, isCountView) {
  var History = window.History;
  if (History.enabled) {
    if (streamName == undefined && clusterName == undefined)
      History.pushState({
        gstream: streamName,
        gcluster: clusterName
      }, "Databus Visualization", queryString + "&gstream=all&gcluster=all");
    else
      History.pushState({
          gstream: streamName,
          gcluster: clusterName
        }, "Databus Visualization", queryString + "&gstream=" + streamName +
        "&gcluster=" + clusterName);
  } else {
    console.log("History not enabled");
  }
  History.Adapter.bind(window, 'statechange', function () {
      loadGraph(History.getState().data.gstream, History.getState().data.gcluster, isCountView);      
  });
}

function saveHistoryAndLoadGraph(streamName, clusterName, isCountView) {
  saveHistory(streamName, clusterName, isCountView);
  loadGraph(streamName, clusterName, isCountView);
}

function popAllTopicStatsNotBelongingToStream(streamName, treeList) {
  treeList.forEach(function (l) {
    l.forEach(function (n) {
      var newReceivedStats = [];
      var newSentStats = [];
      var overallLatency = [];
      var allTopicsLatency = [];
      n.aggregatemessagesreceived = 0;
      n.aggregatemessagesent = 0;
      n.allreceivedtopicstats.forEach(function (s) {
        if (s.topic == streamName) {
          n.aggregatemessagesreceived += s.messages;
          newReceivedStats.push(new TopicStats(s.topic, s.messages, s.hostname));
        }
      });
      n.allsenttopicstats.forEach(function (s) {      
        if (s.topic == streamName) {
          n.aggregatemessagesent += s.messages;
          newSentStats.push(new TopicStats(s.topic, s.messages, s.hostname));
        }
      });
      n.allTopicsLatency.forEach(function (t) {
        var topic = t.topic;
        if (topic == streamName) {
          var topicLateny = new TopicLatency(topic);
          t.latencyList.forEach(function (l) {
            topicLateny.latencyList.push(new PercentileLatency(l.percentile,
              l.latency));
            overallLatency.push(new PercentileLatency(l.percentile, l.latency));
          });
          allTopicsLatency.push(topicLateny);
        };
      });
      n.allreceivedtopicstats = newReceivedStats;
      n.allsenttopicstats = newSentStats;
      n.allTopicsLatency = allTopicsLatency;
      n.overallLatency = overallLatency;
    });
  });
}

function cloneNode(n) {
  var node = new Object();
  node.name = n.name;
  node.cluster = n.cluster;
  node.tier = n.tier;
  node.aggregatemessagesreceived = n.aggregatemessagesreceived;
  node.aggregatemessagesent = n.aggregatemessagesent;
  node.allreceivedtopicstats = [];
  node.allsenttopicstats = [];
  node.source = [];
  node.overallLatency = [];
  node.allTopicsLatency = [];
  n.allreceivedtopicstats.forEach(function (t) {
    if (t.hostname != undefined)
      node.allreceivedtopicstats.push(new TopicStats(t.topic, t.messages, t.hostname));
    else
      node.allreceivedtopicstats.push(new TopicStats(t.topic, t.messages));
  });
  n.allsenttopicstats.forEach(function (t) {
    if (t.hostname != undefined)
      node.allsenttopicstats.push(new TopicStats(t.topic, t.messages, t.hostname));
    else
      node.allsenttopicstats.push(new TopicStats(t.topic, t.messages));
  });
  n.source.forEach(function (t) {
    node.source.push(new TopicStats(t.topic, t.messages));
  });
  n.overallLatency.forEach(function (l) {
    node.overallLatency.push(new PercentileLatency(l.percentile, l.latency));
  });
  n.allTopicsLatency.forEach(function (t) {
    var topicLatency = new TopicLatency(t.topic);
    t.latencyList.forEach(function (l) {
      topicLatency.latencyList.push(new PercentileLatency(l.percentile, l.latency));
    });
    node.allTopicsLatency.push(topicLatency);
  });
  return node;
}

function loadGraph(streamName, clusterName, isCountView) {
  clearPreviousGraph();
  var treeList = getTreeList(streamName, clusterName);
  var graphsvg = d3.select("#graphPanel")
    .append("svg:svg")
    .style("stroke",
      "gray")
    .attr("width", r * 5)
    .attr("height", r * 5)
    .style("background",
      "#fff")
    .attr("id", "graphsvg")
    .append("svg:g")
    .attr("transform",
      "translate(" + r * 2.5 + "," + (r * 2.5) + ")");
  var calledOnce = false;
  var divisions = treeList.length;
  var angle = 360 / (2 * divisions);
  var diff = 360 / divisions;
  var yDiff = r;
  var isRootHdfs = false;
  if (divisions == 1) {
    angle = 90;
    diff = 90;
  }
  for (var index = 0; index < treeList.length; index++) {
    collectorIndex = 0;
    var clusterNodeList = treeList[index];
    var clusterName = clusterNodeList[0].cluster;
    //var i;
    var startindex = getStartIndex(clusterNodeList);
    if (startindex == undefined) {
      addClusterName(clusterName, tree, angle, yDiff, graphsvg, isCountView);
      angle += diff;
      continue;
    }
    var tree = d3.layout.tree()
      .size([360, r])
      .separation(function (a, b) {
        if (a.region != b.region) {
          return 1;
        } else {
          return (a.children == b.children ? 3 : 3) / a.depth;
        }
      });
      
    var root = cloneNode(clusterNodeList[startindex]);
    root.children = travelTree(root, clusterNodeList);

    if (getNumOfNodes(clusterNodeList, "collector") > 1) {
      travelTreeAndSetCollectorChildList(root);
    }

    var diagonal = d3.svg.diagonal.radial()
      .projection(function (d) {
        return [d.y, d.x / 180 * Math.PI];
      });

    var nodes = tree.nodes(root);
    var links = tree.links(nodes);
    
    yDiff = setNodesAngles(angle, diff, nodes, isRootHdfs);

    if (!calledOnce) {
      drawConcentricCircles(graphsvg, yDiff, divisions);
      calledOnce = true;
    }

    var drawlink = graphsvg.selectAll("path.link")
      .filter(function (d, i) {
        return d.cluster == clusterName;
      })
      .data(links)
      .enter()
      .append("svg:path")
      .attr("class", "link")
      .attr("d",
        diagonal)
      .style("stroke-width", "2px")
      .style("fill", "#dedede")
      .style("stroke", "#dedede");
    var drawnode = graphsvg.selectAll("g.node")
      .filter(function (d, i) {
        return d.cluster == clusterName;
      })
      .data(nodes)
      .enter()
      .append("svg:g")
      .attr("class", "node")
      .attr(
        "transform", function (d) {
          return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")";
        });
    var div = d3.select("body")
      .append("div")
      .attr("class", "tooltip")
      .style(
        "opacity", 0);
    drawnode.attr("class", "node")
      .append("svg:circle")
      .attr("r", 5)
      .style(
        "stroke-width", "0px")
      .style("cursor", "hand")
      .style("cursor", "pointer")
      .on("mouseover", function (n) {
        div.transition()
          .duration(200)
          .style("opacity", .9)
        div.html(n.name)
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
        
        nodeover(n, isCountView, true);          
      })
      .on("mouseout", function (n) {
        div.transition()
          .duration(500)
          .style("opacity", 0);
        loadDefaultView(isCountView);        
      });
    if (isCountView) {
      drawlink.style("cursor", "hand")
      	.style("cursor", "pointer")
      	.on("click", linkclick);  
      drawnode.on("click", nodeclick);  	
    } else
      drawnode.on("click", latencynodeclick);

    var mergeMirrorList = populateMergeMirrorList(clusterNodeList);
    if (mergeMirrorList.length > 0) {
      mergeMirrorList.forEach(function (n) {
        var mergemirrornodes = tree.nodes(n);
        var mergeMirrorNode;
        mergemirrornodes.forEach(function (n) {
          if (n.tier == "merge") {
            n.x = angle;
            n.y = 2 * yDiff;
            mergeMirrorNode = graphsvg.selectAll("g.node")
              .filter(function (
                d) {
                return d.cluster == clusterName && (d.tier == "merge");
              })
              .data(mergemirrornodes)
              .enter()
              .append("svg:g")
              .attr("class",
                "node")
              .attr("transform", function (d) {
                return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")";
              })
          } else if (n.tier == "mirror") {
            n.x = angle;
            n.y = yDiff;
            mergeMirrorNode = graphsvg.selectAll("g.node")
              .filter(function (
                d) {
                return d.cluster == clusterName && (d.tier == "mirror");
              })
              .data(mergemirrornodes)
              .enter()
              .append("svg:g")
              .attr("class",
                "node")
              .attr("transform", function (d) {
                return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")";
              })
          }
        });
        mergeMirrorNode.attr("class", "node")
          .append("svg:circle")
          .attr("r",
            5)
          .style("stroke-width", "0px")
          .style("cursor", "hand")
          .style(
            "cursor", "pointer")
          .on("mouseover", function (n) {
            div.transition()
              .duration(200)
              .style("opacity", .9);
            /*
          
                  div.html(n.name + "<br/>" + n.aggregatemessagesreceived)*/
            div.html(n.name)
              .style("left", (
                d3.event.pageX) + "px")
              .style("top", (d3.event.pageY - 28) + "px");              
            nodeover(n, isCountView, true);                
          })
          .on("mouseout", function (n) {
            div.transition()
              .duration(500)
              .style("opacity", 0);              
            loadDefaultView(isCountView);              
          });
        if (isCountView)
          mergeMirrorNode.on("click", nodeclick);
        else
          mergeMirrorNode.on("click", latencynodeclick);
      });
    }
    addClusterName(clusterName, tree, angle, yDiff, graphsvg, isCountView);
    
    angle += diff;
  }
  loadDefaultView(isCountView);    
  addLegendBox(graphsvg);
}

function drawGraph(result, cluster, stream, baseQueryString, drillDownCluster, drillDownStream, agent, vip, collector, hdfs, local, merge, mirror, percentileFrSla, percentageForLoss) {

  agentSla = agent;
  vipSla = vip;
  collectorSla = collector;
  hdfsSla = hdfs;
  localSla = local
  mergeSla = merge
  mirrorSla = mirror;
  percentileForSla = percentileFrSla;
  percentageForLoss = percentageForLoss;
  document.getElementById("tabs").style.display = "block";
  queryString = baseQueryString;
  jsonresponse = JSON.parse(result);
  while (fullTreeList.length > 0) {
    fullTreeList.pop();
  }
  clearHistory();
  buildNodeList();
  if (drillDownCluster != 'null' && drillDownStream != 'null')
    tabSelected(1, drillDownStream, drillDownCluster);
  else
    tabSelected(1, stream, cluster);
}

function clearSvgAndAddLoadSymbol() {
  document.getElementById("infoPanel").innerHTML = "";
  d3.select("#graphsvg").remove();
  var graphsvg = d3.select("#graphPanel")
    .append("svg:svg")
    .style("stroke",
      "gray")
    .attr("width", r * 5)
    .attr("height", r * 5)
    .style("background",
      "#fff")
    .attr("id", "graphsvg")
    .append("svg:g")
  var imgpath = "bar-ajax-loader.gif";
  var imgs = graphsvg.append("svg:image")
    .attr("xlink:href", imgpath)
    .attr("x",
      r * 2.5 - 100)
    .attr("y", r * 2.5)
    .attr("width", "200")
    .attr("height", "40");
}

function tabSelected(selectedTabID, stream, cluster) {
  if (stream == 'null' && cluster == 'null') {
    stream = window.History.getState().data.qstream;
    cluster = window.History.getState().data.qcluster;
  }
  var isCountView;
  if (selectedTabID == 1) {
    document.getElementById("count").className = "active";
    document.getElementById("latency").className = "";
    isCountView = true;
  } else if (selectedTabID == 2) {
    document.getElementById("count").className = "";
    document.getElementById("latency").className = "active";
    isCountView = false;
  }
  clearSvgAndAddLoadSymbol();
  saveHistoryAndLoadGraph(stream, cluster, isCountView);
}

function getTreeList(streamName, clusterName) {
  var treeList = undefined;
  if ((streamName == undefined || streamName.toLowerCase() == 'all') && (
    clusterName == undefined || clusterName.toLowerCase() == 'all')) {
    isPartial = false;
    treeList = fullTreeList;
  } else {
    treeList = [];
    if (streamName != undefined && streamName.toLowerCase() != 'all' &&
      clusterName != undefined) {
      var clusterList = [];
      fullTreeList.forEach(function (l) {
        if (l[0].cluster == clusterName) {
          l.forEach(function (nodeInCluster) {
            clusterList.push(cloneNode(nodeInCluster));
          });
        }
      });
      treeList.push(clusterList);
      popAllTopicStatsNotBelongingToStream(streamName, treeList);
    } else if (clusterName != undefined) {
      var clusterList = [];
      fullTreeList.forEach(function (l) {
        if (l[0].cluster == clusterName) {
          l.forEach(function (nodeInCluster) {
            clusterList.push(cloneNode(nodeInCluster));
          });
        }
      });
      treeList.push(clusterList);
    }
  }
  return treeList;
}

function getStartIndex(nodeList) {
  var startindex = undefined;
  if (getNumOfNodes(nodeList, "local") > 0) {
    for (i = 0; i < nodeList.length; i++) {
      if (nodeList[i].tier.toLowerCase() == "local") {
        startindex = i;
        break;
      }
    }
  } else if (getNumOfNodes(nodeList, "hdfs") > 0) {
    for (i = 0; i < nodeList.length; i++) {
      if (nodeList[i].tier.toLowerCase() == "hdfs") {
        startindex = i;
        isRootHdfs = true;
        break;
      }
    }
  }
  return startindex;
}