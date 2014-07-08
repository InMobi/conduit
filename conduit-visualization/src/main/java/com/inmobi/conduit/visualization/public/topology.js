/*
When creating the full graph, 3 different types of trees are created.
1) First tree is till LOCAL tier. In this tree, dummy nodes of VIP tier are
created (i.e. with empty children array) because more than one collector can
have same VIP as child
2) Second tree consists of only MERGE-LOCAL tiers. In this tree,
LOCAL nodes are dummy, because LOCAL tier node for each cluster are already
created in tree type 1
3) Second tree consists of only MIRROR-MERGE tiers. In this tree, MERGE
nodes are dummy, because MERGE tier node for all possible clusters are already
created in tree type 2
*/

var jsonresponse;
var isNodeColored = false;
var pathLinkCache = [],
  lineLinkCache = [];
//this variable is used so that when loaing default view, same vip child tree need not be highlighted for every collector
var isVipNodeHighlighed = false; 
var r = 180;
var yDiff = 40;
var degToRadFactor = Math.PI / 180;
var radToDegFactor = 180 / Math.PI;
var linelength = 9 * yDiff;

function Node(name, clusterName, tier, aggregatemessagesreceived,
  aggregatemessagesent) {
  this.name = name;
  this.clusterName = clusterName;
  this.tier = tier;
  this.aggregatemessagesreceived = aggregatemessagesreceived;
  this.aggregatemessagesent = aggregatemessagesent;
  this.children = [];
  this.point = new Point(0, 0);
}

function isLoss(count1, count2) {
  var marginAllowed = parseInt(percentageForLoss * count2, 10) / 100;
  if (count1 < (count2 - marginAllowed))
    return true;
  else
    return false;
}

function isDummyNode(n) {
  // Return true if n is dummy node, else false
  if ((n.tier.equalsIgnoreCase("vip") || n.tier.equalsIgnoreCase("local")
    || n.tier.equalsIgnoreCase("merge")) && n.children.length === 0) {
    return true;
  }
  return false;
}

function getNonDummyNode(n) {
  // Return non-dummy node corresponding to n if n is a dummy node
  if (isDummyNode(n)) {
    if (n.tier.equalsIgnoreCase('vip')) {
      var vipNode = vipNodesMap[n.clusterName];
      if (vipNode != undefined && vipNode.children.length > 0) {
        return vipNode;
      }
    } else {
      var tierMap = rootNodesMap[n.clusterName];
      if (tierMap != undefined) {
        var node = tierMap[n.tier];
        if (node != undefined) {
          return node;
        }
      }
    }
  }
  return n;
}

function highlightChildNodes(n, isLoadDefaultView) {
  if (n.tier.equalsIgnoreCase("hdfs")) {
    highlightHDFSNode(n);
  } else {
    if (n.tier.equalsIgnoreCase("vip") && isVipNodeHighlighed &&
      isLoadDefaultView) {
      // Return without further action if vip is already highlighted since
      // all collectors have same vip as child and same part of tree is
      // highlighted multiple times if not returned here
      return;
    }
    var totalaggregatechild = 0;
    var totalaggregateparent = 0;
    totalaggregateparent = parseInt(n.aggregatemessagesreceived, 10);
    n = getNonDummyNode(n);
    if (n.children == 0) {
      // Even after replacing n with non-dummy node it has no
      // children which indicates that tree stops at this point
      return;
    }
    if (n.tier.equalsIgnoreCase("collector")) {
      totalaggregateparent = aggCollectorNodeMap[n.clusterName].received;
    }
    if (isMergeMirror(n)) {
      for (var t in n.topicSourceMap) {
        n.topicSourceMap[t].forEach(function(s) {
          for (var i = 0; i < n.children.length; i++) {
            if (n.children[i].clusterName.equalsIgnoreCase(s)) {
              var received = n.children[i].topicReceivedMap[t];
              if (received != undefined) {
                totalaggregatechild += received;
                break;
              }
              break;
            }
          }
        });
      }
    } else {
      n.children.forEach(function (c) {
        totalaggregatechild += parseInt(c.aggregatemessagesreceived, 10);
      });
    }
    n.children.forEach(function (c) {
      var currentLink;
      if (isMergeMirror(n)) {
        currentLink = lineLinkCache
          .filter(function (d) {
            return d.source.clusterName == n.clusterName && d.source.name ==
              n.name && d.source.tier.equalsIgnoreCase(n.tier) && d.target.name ==
              c.name;
          })
          .transition()
          .duration(100);
      } else {
        currentLink = pathLinkCache
          .filter(function (d) {
            return d.source.clusterName == n.clusterName && d.source.name ==
              n.name && d.source.tier.equalsIgnoreCase(n.tier) && d.target.name ==
              c.name;
          })
          .transition()
          .duration(100);
      }
      if (countEntriesInMap(n.topicReceivedMap) == 0 || (isAgentCollector(c) && countEntriesInMap(c.topicSentMap) == 0) || countEntriesInMap(c.topicReceivedMap) == 0) {
        currentLink.style("fill", "none")
          .style("stroke", "#dedede")
          .each(function (d) {
            d.status = "nofill";
          });
      } else if (isLoss(totalaggregateparent, totalaggregatechild)) {
        currentLink.style("fill", "none")
          .style("stroke", "#ff0000")
          .each(function (d) {
            d.status = "loss";
          });
      } else {
        currentLink.style("fill", "none")
          .style("stroke", "#00ff00")
          .each(function (d) {
            d.status = "healthy";
          });
      }
    });
    if (n.tier.equalsIgnoreCase("vip") && isLoadDefaultView) {
      isVipNodeHighlighed = true;
    }
  }
  if (countEntriesInMap(n.topicReceivedMap) > 0) {
    n.children.forEach(function (c) {
      highlightChildNodes(c, isLoadDefaultView);
    });
  }
}

function highlightHDFSNode(n) {
  n.children.forEach(function (c) {
    var aggregateparent = 0;
    var aggregatechild = 0;
    var link = pathLinkCache
      .filter(function (d) {
        return d.source.clusterName == n.clusterName && d.source.name ==
          n.name && d.source.tier.equalsIgnoreCase(n.tier) && d.target.name ==
          c.name;
      })
      .transition()
      .duration(100);
    if (countEntriesInMap(c.topicSentMap) == 0)
      link.style("fill", "none")
        .style("stroke", "#dedede")
        .each(function (d) {
          d.status = "nofill";
        });
    else {
      for (var t in n.topicReceivedMap) {
        var num = n.topicReceivedMap[t][c.name];
        if (num != undefined) {
          var cNum = c.topicSentMap[t];
          if (cNum != undefined) {
            aggregateparent += num;
            aggregatechild += cNum;
          }
        }
      }
      if (countEntriesInMap(n.topicReceivedMap) == 0)
        link.style("fill", "none")
          .style("stroke", "#dedede")
          .each(function (d) {
            d.status = "nofill";
          });
      else if (isLoss(aggregateparent, aggregatechild))
        link.style("fill", "none")
          .style("stroke", "#ff0000")
          .each(function (d) {
            d.status = "loss";
          });
      else
        link.style("fill", "none")
          .style("stroke", "#00ff00")
          .each(function (d) {
            d.status = "healthy";
          });
    }
  });
}

function nodeover(n, clear, isLoadDefaultView) {
  if (clear) {
    lineLinkCache
      .transition()
      .duration(100)
      .style("fill", "none")
      .style("stroke", "#dedede");
    pathLinkCache
      .transition()
      .duration(100)
      .style("fill", "none")
      .style("stroke", "#dedede");
  }
  if (isCountView)
    highlightChildNodes(n, isLoadDefaultView);
  else
    latencyhighlightChildNodes(n, isLoadDefaultView);
}

function latencyhighlightChildNodes(n, isLoadDefaultView) {
  if (n.tier.equalsIgnoreCase("vip") && isVipNodeHighlighed &&
    isLoadDefaultView) {
    return;
  }
  var color;
  var latency = n.latencyMap[percentileForSla];
  if (latency != undefined && latency > tierLatencySlaMap[n.tier.toLowerCase()]) {
    color = '#f00';
  }
  if (color != undefined) {
    d3.selectAll("g.node")
      .filter(function (d) {
        return d.name == n.name && d.clusterName == n.clusterName && d.tier.equalsIgnoreCase(
          n.tier);
      })
      .select("circle")
      .transition()
      .duration(100)
      .style("fill", color);
  }
  n = getNonDummyNode(n);
  if (n.children == 0) {
    // Even after replacing n with non-dummy node it has no
    // children which indicates that tree stops at this point
    return;
  }
  if (n.tier.equalsIgnoreCase("vip") && isLoadDefaultView) {
    isVipNodeHighlighed = true;
  }
  if (isLoadDefaultView) {
    lineLinkCache.each(function (d) {
      d.status = "nofill";
    });
    pathLinkCache.filter(function (d) {
      d.status = "nofill";
    });
  }
  n.children.forEach(function (c) {
    if (isMergeMirror(n)) {
      lineLinkCache.filter(function (d) {
        return d.source.clusterName == n.clusterName && d.source.name ==
          n.name && d.source.tier.equalsIgnoreCase(n.tier) && d.target.name ==
          c.name && countEntriesInMap(n.topicLatencyMap) > 0 && countEntriesInMap(c.topicLatencyMap) > 0;
      })
        .transition()
        .duration(100)
        .style("fill", "none")
        .style("stroke", "#ADBCE6")
        .each(function (d) {
          d.status = "healthy";
        });
    } else {
      pathLinkCache.filter(function (d) {
        return d.source.clusterName == n.clusterName && d.source.name ==
          n.name && d.source.tier.equalsIgnoreCase(n.tier) && d.target.name ==
          c.name && countEntriesInMap(n.topicLatencyMap) > 0 && countEntriesInMap(c.topicLatencyMap) > 0;
      })
        .transition()
        .duration(100)
        .style("fill", "none")
        .style("stroke", "#ADBCE6")
        .each(function (d) {
          d.status = "healthy";
        });
    }
    if (countEntriesInMap(n.topicLatencyMap) > 0) {
      latencyhighlightChildNodes(c, isLoadDefaultView);
    }
  });
}

function addListToInfoPanel(n) {
  var div = document.createElement('div');
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Showing Node Information:";
  c.style.fontWeight = "bold";
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Name:";
  c = r.insertCell(1);
  c.innerHTML = n.name;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Tier:";
  c = r.insertCell(1);
  c.innerHTML = n.tier.toLowerCase();
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "clusterName:";
  c = r.insertCell(1);
  if (isCountView) {
    c.innerHTML =
      "<button type=\"button\" onclick=\"saveHistoryAndReload('All', '" + n.clusterName +
      "','all', 1)\" class=\"transparentButton\">" + n.clusterName +
      "</button>";
    r = t.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "Aggregate Received:";
    c = r.insertCell(1);
    c.innerHTML = n.aggregatemessagesreceived;
    if (isAgentCollector(n)) {
      r = t.insertRow(currentRow++);
      c = r.insertCell(0);
      c.innerHTML = "Aggregate Sent:";
      c = r.insertCell(1);
      c.innerHTML = n.aggregatemessagesent;
    }
    r = t.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "Message Count Table:";
    c.style.fontWeight = "bold";
  } else {
    c.innerHTML =
      "<button type=\"button\" onclick=\"saveHistoryAndReload('All', '" + n.clusterName +
      "', 'all', 2)\" class=\"transparentButton\">" + n.clusterName +
      "</button>";
    r = t.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "Latency Table:";
    c.style.fontWeight = "bold";
  }
  div.appendChild(t);
  document.getElementById("infoPanel")
    .appendChild(div);
}

function nodeclick(n) {
  document.getElementById("infoPanel")
    .innerHTML = "";
  lineLinkCache
    .style("stroke-width", "2px");
  pathLinkCache
    .style("stroke-width", "2px");
  d3.selectAll("g.node")
    .select("circle")
    .attr("r", 5);
  d3.selectAll("g.node")
    .filter(function (d) {
      return d.clusterName == n.clusterName && d.name == n.name && d.tier.equalsIgnoreCase(
        n.tier);
    })
    .select("circle")
    .attr("r", 7);
  addListToInfoPanel(n, true);
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  if (isAgentCollector(n)) {
    r = t.insertRow(currentRow);
    c = r.insertCell(0);
    c.innerHTML = "<b>Topic</b>";
    c = r.insertCell(1);
    c.innerHTML = "<b>Received</b>";
    c = r.insertCell(2);
    c.innerHTML = "<b>Sent</b>";
    currentRow++;
    for (var topic in n.topicReceivedMap) {
      var received = n.topicReceivedMap[topic];
      var sent = n.topicSentMap[topic];
      r = t.insertRow(currentRow);
      c = r.insertCell(0);
      c.innerHTML =
        "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
        topic + "', '" + n.clusterName + "', 'all', 1)\" class=\"transparentButton\">" + topic +
        "</button>";
      if (sent != received) {
        c.firstChild.style.color = "#ff0000";
      } else {
        c.firstChild.style.color = "#00a000";
      }
      c = r.insertCell(1);
      c.innerHTML = received;
      c = r.insertCell(2);
      c.innerHTML = sent;
      currentRow++;
    }
  } else {
    var cell = 0;
    r = t.insertRow(currentRow);
    c = r.insertCell(cell);
    c.innerHTML = "<b>Topic</b>";
    cell++;
    c = r.insertCell(cell);
    c.innerHTML = "<b>Received</b>";
    currentRow++;
    if (n.tier.equalsIgnoreCase("hdfs")) {
      var aggregateMap = new Object();
      for (var topic in n.topicReceivedMap) {
        if (aggregateMap[topic] == undefined) {
          aggregateMap[topic] = 0;
        }
        for (var h in n.topicReceivedMap[topic]) {
          aggregateMap[topic] += n.topicReceivedMap[topic][h];
        }
      }
      for (var topic in aggregateMap) {
        r = t.insertRow(currentRow);
        var cell = 0;
        c = r.insertCell(cell);
        c.innerHTML =
          "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
          topic + "', '" + n.clusterName + "', 'all', 1)\" class=\"transparentButton\">" + topic +
          "</button>";
        cell++;
        c = r.insertCell(cell);
        c.innerHTML = aggregateMap[topic];
        currentRow++;
      }
    } else {
      for (var topic in n.topicReceivedMap) {
        r = t.insertRow(currentRow);
        var cell = 0;
        c = r.insertCell(cell);
        c.innerHTML =
          "<button type=\"button\" onclick=\"saveHistoryAndReload('" + topic + "', '" + n.clusterName +
          "', 'all', 1)\" class=\"transparentButton\">" + topic + "</button>";
        cell++;
        c = r.insertCell(cell);
        c.innerHTML = n.topicReceivedMap[topic];
        currentRow++;
      }
    }
  }
  t.className = "valuesTable";
  document.getElementById("infoPanel")
    .appendChild(t);
}

function latencynodeclick(n) {
  document.getElementById("infoPanel")
    .innerHTML = "";
  lineLinkCache
    .style("stroke-width", "2px");
  pathLinkCache
    .style("stroke-width", "2px");
  d3.selectAll("g.node")
    .select("circle")
    .attr("r", 5);
  d3.selectAll("g.node")
    .filter(function (d) {
      return d.clusterName == n.clusterName && d.name == n.name && d.tier.equalsIgnoreCase(
        n.tier);
    })
    .select("circle")
    .attr("r", 7);
  addListToInfoPanel(n, false);
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  var currentCol = 0;
  r = t.insertRow(currentRow++);
  c = r.insertCell(currentCol++);
  c.innerHTML = "<b>Overall/Topic</b>";
  for (var l in n.latencyMap) {
    c = r.insertCell(currentCol++);
    c.innerHTML = "<b>" + l + "</b>";    
  }
  for (var topic in n.topicLatencyMap) {
    r = t.insertRow(currentRow++);
    var currentColumn = 0;
    c = r.insertCell(currentColumn++);
    c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndReload('" + topic + "', '" + n.clusterName + "', 'all', 2)\" class=\"transparentButton\">" + topic + "</button>";
    for (var l in n.topicLatencyMap[topic]) {
      c = r.insertCell(currentColumn++);
      if (n.topicLatencyMap[topic][l] == 0) {
        c.innerHTML = "<1";        
      } else {
        c.innerHTML = n.topicLatencyMap[topic][l];        
      }
    }
  }

  currentCol = 0;
  r = t.insertRow(currentRow++);
  c = r.insertCell(currentCol++);
  c.innerHTML = "Overall";
  for (var l in n.latencyMap) {
    c = r.insertCell(currentCol++);
    if (n.latencyMap[l] == 0) {
      c.innerHTML = "<1";
    } else {
      c.innerHTML = n.latencyMap[l];
    }
  }
  t.className = "valuesTable";
  document.getElementById("infoPanel")
    .appendChild(t);
}

function getStreamsCausingDataLoss(l) {
  var streamslist = [];
  var isstreampresent = false;
  if (l.source.tier.equalsIgnoreCase("hdfs")) {
    l.target.allsenttopicstats.forEach(function (t) {
      l.source.allreceivedtopicstats.forEach(function (s) {
        if (t.topic == s.topic && s.hostname == l.target.name) {
          isstreampresent = true;
          if (isLoss(s.messages, t.messages))
            streamslist.push(t.topic);
        }
      });
      if (!isstreampresent && !(streamslist.contains(t.topic)))
        streamslist.push(t.topic);
      isstreampresent = false;
    });
  } else if (l.source.tier.equalsIgnoreCase("collector")) {
    var linkList = pathLinkCache
      .filter(function (d) {
        return d.source.clusterName == l.source.clusterName && d.source
          .tier.equalsIgnoreCase("collector");
      })
      .data();
    var aggMsgList = [];
    linkList.forEach(function (l) {
      l.source.allreceivedtopicstats.forEach(function (stat) {
        var isPresent = false;
        for (var index = 0; index < aggMsgList.length; index++) {
          if (aggMsgList[index].topic == stat.topic) {
            aggMsgList[index].messages += stat.messages;
            isPresent = true;
            break;
          }
        }
        if (!isPresent)
          aggMsgList.push(new TopicStats(stat.topic, stat.messages));
      });
    });
    l.target.allreceivedtopicstats.forEach(function (t) {
      aggMsgList.forEach(function (s) {
        if (t.topic == s.topic) {
          isstreampresent = true;
          if (isLoss(s.messages, t.messages))
            streamslist.push(t.topic);
        }
      });
      if (!isstreampresent && !(streamslist.contains(t.topic)))
        streamslist.push(t.topic);
      isstreampresent = false;
    });
  } else if (l.source.tier.equalsIgnoreCase("merge") || l.source.tier.equalsIgnoreCase(
    "mirror")) {
    var targetNodeStreams = [];
    l.target.allreceivedtopicstats.forEach(function (s) {
      targetNodeStreams.push(s.topic);
    });
    l.source.allreceivedtopicstats.forEach(function (s) {
      var topic = s.topic;
      var topicSourceList;
      l.source.streamSourceList.forEach(function (sourceStream) {
        if (sourceStream.topic == topic) {
          topicSourceList = sourceStream.source;
        }
      });
      if (topicSourceList.contains(l.target.clusterName)) {
        l.target.allreceivedtopicstats.forEach(function (t) {
          if (t.topic == topic) {
            isstreampresent = true;
            if (isLoss(s.messages, t.messages)) {
              streamslist.push(t.topic);
            }
          }
        });
        if (!isstreampresent && !(streamslist.contains(topic)) &&
          targetNodeStreams.contains(topic)) {
          streamslist.push(topic);
        }
        isstreampresent = false;
      }
    });
  } else {
    l.target.allreceivedtopicstats.forEach(function (t) {
      l.source.allreceivedtopicstats.forEach(function (s) {
        if (t.topic == s.topic) {
          isstreampresent = true;
          if (isLoss(s.messages, t.messages))
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
  lineLinkCache
    .style("stroke-width", "2px");
  pathLinkCache
    .style("stroke-width", "2px");
  d3.selectAll("g.node")
    .select("circle")
    .attr("r", 5);
  if (isMergeMirror(l.source)) {
    lineLinkCache
      .filter(function (d) {
        return d.target.clusterName == l.target.clusterName && d.target.name ==
          l.target.name && d.target.tier.equalsIgnoreCase(l.target.tier) && d
          .source.clusterName ==
          l.source.clusterName && d.source.name == l.source.name && d.source.tier
          .equalsIgnoreCase(l.source.tier);
      })
      .style("stroke-width", "5px");
  } else {
    pathLinkCache
      .filter(function (d) {
        return d.target.clusterName == l.target.clusterName && d.target.name ==
          l.target.name && d.target.tier.equalsIgnoreCase(l.target.tier) && d
          .source.clusterName ==
          l.source.clusterName && d.source.name == l.source.name && d.source.tier
          .equalsIgnoreCase(l.source.tier);
      })
      .style("stroke-width", "5px");
  }
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Showing Link Information:";
  c.style.fontWeight = "bold";
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Source Name:";
  c = r.insertCell(1);
  c.innerHTML = l.target.name;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Source Tier:";
  c = r.insertCell(1);
  c.innerHTML = l.target.tier.toLowerCase();
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Source cluster:";
  c = r.insertCell(1);
  c.innerHTML =
    "<button type=\"button\" onclick=\"saveHistoryAndReload('All', '" + l.target
    .clusterName + "', 'all', 1)\" class=\"transparentButton\">" + l.target.clusterName +
    "</button>";
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Target Name:";
  c = r.insertCell(1);
  c.innerHTML = l.source.name;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Target Tier:";
  c = r.insertCell(1);
  c.innerHTML = l.source.tier.toLowerCase();
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Target Cluster:";
  c = r.insertCell(1);
  c.innerHTML =
    "<button type=\"button\" onclick=\"saveHistoryAndReload('All', '" + l.source
    .clusterName + "', 'all', 1)\" class=\"transparentButton\">" + l.source.clusterName +
    "</button>";
  var streams = getStreamsCausingDataLoss(l);
  if (streams.length > 0) {
    r = t.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "<b>Streams causing dataloss: </b>";
    streams.forEach(function (s) {
      r = t.insertRow(currentRow++);
      c = r.insertCell(0);
      c.innerHTML =
        "<button type=\"button\" onclick=\"saveHistoryAndReload('" + s +
        "', '" + l.source.clusterName +
        "', 'all', 1)\" class=\"transparentButton\">" + s + "</button>";
    });
  }
  document.getElementById("infoPanel")
    .appendChild(t);
}

function getNumOfNodes(nodes, tier) {
  var num = 0;
  nodes.forEach(function (n) {
    if (n.tier.equalsIgnoreCase(tier))
      num++;
  });
  return num;
}

function setNodesAngles(angle, diff, nodes) {
  var numCollectors = getNumOfNodes(nodes, "collector");
  var numAgents = getNumOfNodes(nodes, "agent");
  var numPublishers = getNumOfNodes(nodes, "publisher");
  var collectorAngleDiff = diff / (numCollectors + 1);
  var collectorAngle = (angle - (diff / 2)) + collectorAngleDiff;
  var agentAngleDiff = diff / (numAgents + 1);
  var agentAngle = (angle - (diff / 2)) + agentAngleDiff;
  var publisherAngleDiff = diff / (numPublishers + 1);
  var publisherAngle = (angle - (diff / 2)) + publisherAngleDiff;
  nodes.forEach(function (n) {
    if (n.tier.equalsIgnoreCase("mirror")) {
      n.x = angle;
      n.y = yDiff;
    } else if (n.tier.equalsIgnoreCase("merge")) {
      n.x = angle;
      n.y = 2 * yDiff;
    } else if (n.tier.equalsIgnoreCase("local")) {
      n.x = angle;
      n.y = 3 * yDiff;
    } else if (n.tier.equalsIgnoreCase("hdfs")) {
      n.x = angle;
      n.y = 4 * yDiff;
    } else if (n.tier.equalsIgnoreCase("vip")) {
      n.x = angle;
      n.y = 6 * yDiff;
    } else if (n.tier.equalsIgnoreCase("collector")) {
      n.x = collectorAngle;
      collectorAngle += collectorAngleDiff;
      n.y = 5 * yDiff;
    } else if (n.tier.equalsIgnoreCase("agent")) {
      n.x = agentAngle;
      agentAngle += agentAngleDiff;
      n.y = 7 * yDiff;
    } else if (n.tier.equalsIgnoreCase("publisher")) {
      n.x = publisherAngle;
      publisherAngle += publisherAngleDiff;
      n.y = 8 * yDiff;
    }
  });
}

function addClusterName(clusterName, tree, angle, graphsvg) {
  var clusternamenode = new Node(clusterName, clusterName, "clusterName");
  clusterNameTreeNode = tree.nodes(clusternamenode);
  clusterNameTreeNode[0].x = angle;
  clusterNameTreeNode[0].y = (9 * yDiff);
  var clusterNode = graphsvg.selectAll("g.node")
    .filter(function (d, i) {
      return d.clusterName == clusterName && d.tier == "clusterName";
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
      saveHistoryAndReload('All', d.name, 'all');
    });
}

function addColorsToNodes() {
  d3.selectAll("g.node")
    .select("circle")
    .style("fill", function (d) {
      var color = "#ccc";
      if (d.tier.toLowerCase().equalsIgnoreCase("collector") || d.tier
        .toLowerCase().equalsIgnoreCase("agent")) {
        if (isCountView) {
          if (parseInt(d.aggregatemessagesent, 10) < parseInt(d.aggregatemessagesreceived, 10)) {
            color = "#ff0000";
          } else {
            color = tierColorMap[d.tier.toLowerCase()];
          }
        } else {
          var latency = d.latencyMap[percentileForSla];
          if (latency == undefined || latency > tierLatencySlaMap[d.tier.toLowerCase()]) {
            color = '#f00';
          } else {
            color = tierColorMap[d.tier.toLowerCase()];
          }
        }
      } else {
        color = tierColorMap[d.tier.toLowerCase()];
      }
      return color;
    });
}

function addLegendBox(graphsvg) {
  var inc = 100;
  var i = 0;
  for (var tier in tierColorMap) {
    if (tier.equalsIgnoreCase("all")) {
      continue;
    }
    graphsvg.append("circle")
      .attr("class", "legendColor")
      .attr("r", 5)
      .attr("cx", -r * 2.5 + 10 + (i) * inc)
      .attr("cy", -r * 2.5 + 10)
      .style("fill", tierColorMap[tier])
      .style("stroke", tierColorMap[tier]);
    graphsvg.append("text")
      .attr("class", "legend")
      .text(tier)
      .attr("x", -r * 2.5 + 20 + (i) * inc)
      .attr("y", -r * 2.5 + 15);
    i++;
  }
}

function loadDefaultView() {
  if (!isNodeColored) {
    var clear = true;
    for (var s in rootNodesMap) {
      var tierMap = rootNodesMap[s];
      if (tierMap != undefined) {
        for (var t in tierMap) {
          if (t.equalsIgnoreCase('LOCAL')) {
            isVipNodeHighlighed = false;
          }
          nodeover(tierMap[t], clear, true);
          if (clear) {
            clear = false;
          }
        }
      }
    }
    addColorsToNodes();
    isNodeColored = true;
  } else {
    pathLinkCache.each(function (d) {
      var color;
      switch (d.status) {
      case "loss":
        color = "#ff0000";
        break;
      case "healthy":
        if (isCountView) {
          color = "#00ff00";
        } else {
          color = "#ADBCE6";
        }
        break;
      default:
        color = "#dedede";
      }
      d3.select(this).style("fill", "none").style("stroke", color);
    });
    lineLinkCache.each(function (d) {
      var color;
      switch (d.status) {
      case "loss":
        color = "#ff0000";
        break;
      case "healthy":
        if (isCountView) {
          color = "#00ff00";
        } else {
          color = "#ADBCE6";
        }
        break;
      default:
        color = "#dedede";
      }
      d3.select(this).style("fill", "none").style("stroke", color);
    });
  }
}

function appendClusterTreeTillLocalToSVG(graphsvg, tree, root, angle, diff, clusterName) {
  var diagonal = d3.svg.diagonal.radial()
    .projection(function (d) {
      return [d.y, d.x / 180 * Math.PI];
    });
  var nodes = tree.nodes(root);
  var links = tree.links(nodes);
  setNodesAngles(angle, diff, nodes);
  var drawlink = graphsvg.selectAll("path.link")
    .filter(function (d, i) {
      return d.clusterName == clusterName;
    })
    .data(links)
    .enter()
    .append("svg:path")
    .attr("class", "link")
    .attr("d", diagonal)
    .attr('id', function (d) {
      return "line" + d.source.name + "_" + d.target.name + "_" + d.source.clusterName +
        "_" + d.target.clusterName + "_" + d.source.tier + "_" + d.target.tier;
    })
    .style("stroke-width", "2px")
    .style("fill", "none")
    .style("stroke", "#dedede");
  var drawnode = graphsvg.selectAll("g.node")
    .filter(function (d, i) {
      return d.clusterName == clusterName;
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
    .style("opacity", 0);
  drawnode.attr("class", "node")
    .append("svg:circle")
    .attr("r", 5)
    .style("stroke-width", "0px")
    .style("cursor", "pointer")
    .on("mouseover", function (n) {
      div.transition()
        .duration(200)
        .style("opacity", .9)
      div.html(n.name)
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
      nodeover(n, true, false);
    })
    .on("mouseout", function (n) {
      div.transition()
        .duration(500)
        .style("opacity", 0);
      loadDefaultView();
    });
  if (isCountView) {
    drawlink.style("cursor", "pointer")
      .on("click", linkclick);
    drawnode.on("click", nodeclick);
  } else
    drawnode.on("click", latencynodeclick);
}

function setNodeAnglesForLocalNodes(angle, diff, nodes) {
  nodes.forEach(function (n) {
    var arr = d3.selectAll('g.node')
      .filter(function (d) {
        return d.name == n.name && d.clusterName == n.clusterName && d.tier
          .equalsIgnoreCase(n.tier);
      })
      .data();
    if (arr.length > 0) {
      n.x = arr[0].x;
      n.y = arr[0].y;
    } else {
      var newArry = [];
      newArry.push(n);
      setNodesAngles(angle, diff, newArry);
    }
  });
}

function appendMergeMirrorTreesToSVG(graphsvg, tree, root, angle, clusterName, diff) {
  var diagonal = d3.svg.diagonal.radial()
    .projection(function (d) {
      return [d.y, d.x / 180 * Math.PI];
    });
  var div = d3.select("body")
    .append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);
  var nodes = tree.nodes(root);
  var links = tree.links(nodes);
  setNodeAnglesForLocalNodes(angle, diff, nodes);
  nodes.forEach(function (n) {
    setCoordinatesOfNode(n);
  });
  var drawlink = graphsvg.selectAll("line.link")
    .filter(function (l) {
      return l.source.clusterName == root.clusterName && l.source.tier.equalsIgnoreCase(
        root.tier) && l.name == root.name;
    })
    .data(links).enter()
    .append("svg:g")
    .append("svg:line")
    .attr("class", "link")
    .attr("id", function (d) {
      return "line" + d.source.name + "_" + d.target.name + "_" + d.source.clusterName +
        "_" + d.target.clusterName + "_" + d.source.tier + "_" + d.target.tier;
    })
    .attr("x1", function (d) {
      return d.source.point.x;
    })
    .attr("y1", function (d) {
      return d.source.point.y;
    })
    .attr("x2", function (d) {
      return d.target.point.x;
    })
    .attr("y2", function (d) {
      return d.target.point.y;
    })
    .style("stroke-dasharray", ("7, 2"))
    .style("stroke-width", "2px")
    .style("fill", 'none');
  var drawnode = graphsvg.selectAll("g.node")
    .filter(function (d) {
      return d.clusterName.equalsIgnoreCase(clusterName) && d.tier.equalsIgnoreCase(root.tier) &&
        d.name.equalsIgnoreCase(root.name);
    })
    .data(nodes)
    .enter()
    .append("svg:g")
    .attr("class", "node")
    .attr(
      "transform", function (d) {
        return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")";
      });
  drawnode.attr("class", "node")
    .append("svg:circle")
    .attr("r", 5)
    .style("stroke-width", "0px")
    .style("cursor", "pointer")
    .on("mouseover", function (n) {
      div.transition()
        .duration(200)
        .style("opacity", 0.9);
      div.html(n.name)
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
      nodeover(n, true, false);
    })
    .on("mouseout", function (n) {
      div.transition()
        .duration(500)
        .style("opacity", 0);
      loadDefaultView();
    });
  if (isCountView) {
    drawlink.style("cursor", "pointer")
      .on("click", linkclick);
    drawnode.on("click", nodeclick);
  } else
    drawnode.on("click", latencynodeclick);
}

function setCoordinatesOfNode(n) {
  var angle = parseInt(n.x, 10);
  var rad = parseInt(n.y, 10);
  var xAngle = 90 - angle;
  var yAngle = 270 - angle;
  n.point.x = rad * Math.cos(xAngle * degToRadFactor);
  n.point.y = rad * Math.sin(yAngle * degToRadFactor);
}

function getAngleOfLine(sourcePoint, targetPoint) {
  var dx = targetPoint.x - sourcePoint.x;
  var dy = targetPoint.y - sourcePoint.y;
  var slope = dy / dx;
  var angleInRad = Math.atan(slope);
  return angleInRad * radToDegFactor;
}

function appendArrowMarkersToLinks(graphsvg) {
  var links = lineLinkCache.data();
  links.forEach(function (link) {
    var sourcePoint = link.source.point;
    var targetPoint = link.target.point;
    var midpoint = new Point((sourcePoint.x + targetPoint.x) / 2, (
      sourcePoint.y + targetPoint.y) / 2);
    var angleOfLink = getAngleOfLine(sourcePoint, targetPoint);
    var slopeOfLink = (targetPoint.y - sourcePoint.y) / (targetPoint.x -
      sourcePoint.x);
    var slopeOfPerp = -1 / slopeOfLink;
    var angleOfPerpLine = Math.atan(slopeOfPerp) * radToDegFactor;
    var l = 4;
    var midPtOfPerp;
    if (sourcePoint.x <= targetPoint.x) {
      midPtOfPerp = new Point(midpoint.x + l * Math.cos(angleOfLink *
        degToRadFactor), midpoint.y + l * Math.sin(angleOfLink *
        degToRadFactor));
    } else {
      midPtOfPerp = new Point(midpoint.x - l * Math.cos(angleOfLink *
        degToRadFactor), midpoint.y - l * Math.sin(angleOfLink *
        degToRadFactor));
    }
    var x1 = midPtOfPerp.x + l * Math.cos(angleOfPerpLine * degToRadFactor);
    var y1 = midPtOfPerp.y + l * Math.sin(angleOfPerpLine * degToRadFactor);
    var x2 = midPtOfPerp.x - l * Math.cos(angleOfPerpLine * degToRadFactor);
    var y2 = midPtOfPerp.y - l * Math.sin(angleOfPerpLine * degToRadFactor);
    graphsvg.append('svg:path')
      .attr('d', 'M ' + midpoint.x + ' ' + midpoint.y + ' L ' + x1 + ' ' + y1 +
        ' L ' + x2 + ' ' + y2 + ' z')
      .style('fill', '#dedede');
  });
}

function cacheLinks() {
  pathLinkCache = d3.selectAll("path.link");
  lineLinkCache = d3.selectAll("line.link");
}

function getNumberOfPublishers(localRootNode) {
  // Since number of publishers and agents is same, we can return number of agents as well
  if (localRootNode == undefined) {
    return 0;
  } else {
    var hdfsNode = localRootNode.children[0];
    for (var i = 0; i < hdfsNode.children.length; i++) {
      var vipNode = hdfsNode.children[i].children[0];
      if (vipNode.children.length != 0) {
        return vipNode.children.length;
      }
    }
  }
}

function loadGraph(streamName, clusterName) {
  isNodeColored = false;
  lineLinkCache.length = 0;
  pathLinkCache.length = 0;
  clearTopologyGraph();  
  setRootNodesMap(streamName, clusterName);
  var graphsvg = d3.select("#topologyPanel")
    .append("svg:svg")
    .style("stroke", "gray")
    .attr("width", r * 5)
    .attr("height", r * 5)
    .style("background", "#EBF4F8")
    .attr("id", "graphsvg")
    .append("svg:g")
    .attr("transform", "translate(" + r * 2.5 + "," + (r * 2.5) + ")");
  var numClusters = countEntriesInMap(rootNodesMap);
  var totalNumPub = 0, clustersWithoutPub = 0;
  for (var c in rootNodesMap) {
    var num = getNumberOfPublishers(rootNodesMap[c]['LOCAL']);
    if (num == undefined || num == 0) {
      clustersWithoutPub++;
    } else {
      totalNumPub += num;      
    }
  }
  var angleLeft = 360 - 20 * clustersWithoutPub;
  var eachPubAngle = angleLeft / totalNumPub;
  var diff = 0, angle = 0, lineAngle = 0;
  //first append all nodes till local to the svg so that when merge nodes are appended all local nodes are available when retrieving them from the svg using d3.selectAll(). Similarly append all merge nodes before appending mirror nodes
  for (var c in rootNodesMap) {
    if (numClusters == 1) {
      angle = 90;
      diff = 90;
    } else {
      var numPub = getNumberOfPublishers(rootNodesMap[c]['LOCAL'])
      if (numPub == undefined || numPub == 0) {
        diff = 20;
      } else {
        diff = numPub * eachPubAngle;
      }
      angle += diff / 2;
    }
    var tree = d3.layout.tree()
      .separation(function (a, b) {
        if (a.region != b.region) {
          return 1;
        } else {
          return (a.children == b.children ? 3 : 3) / a.depth;
        }
      }).size([diff, r]);
    if (numClusters != 1) {
      lineAngle += diff;
      var xcoord = linelength * Math.cos((lineAngle - 90) * degToRadFactor);
      var ycoord = linelength * Math.sin((lineAngle - 90) * degToRadFactor);
      graphsvg.append("svg:line")
        .attr("x1", 0)
        .attr("y1", 0)
        .attr("x2", xcoord)
        .attr("y2", ycoord)
        .attr("stroke", "#ccc");
    }
    var localNode = rootNodesMap[c]['LOCAL'];
    if (localNode == undefined) {
      addClusterName(c, tree, angle, graphsvg);
      angle += diff / 2;
      continue;
    }
    appendClusterTreeTillLocalToSVG(graphsvg, tree, localNode, angle, diff, c);
    addClusterName(c, tree, angle, graphsvg);
    angle += diff / 2;
  }

  angle = 0;
  diff = 0;
  for (var c in rootNodesMap) {
    if (numClusters == 1) {
      angle = 90;
      diff = 90;
    } else {
      var numPub = getNumberOfPublishers(rootNodesMap[c]['LOCAL'])
      if (numPub == undefined || numPub == 0) {
        diff = 20;
      } else {
        diff = numPub * eachPubAngle;
      }
      angle += diff / 2;
    }
    var tree = d3.layout.tree()
      .separation(function (a, b) {
        if (a.region != b.region) {
          return 1;
        } else {
          return (a.children == b.children ? 3 : 3) / a.depth;
        }
      }).size([diff, r]);
    var mergeNode = rootNodesMap[c]['MERGE'];
    if (mergeNode != undefined) {
      appendMergeMirrorTreesToSVG(graphsvg, tree, mergeNode, angle, c, diff);
    }
    angle += diff / 2;  
  }

  angle = 0;
  diff = 0;
  for (var c in rootNodesMap) {
    if (numClusters == 1) {
      angle = 90;
      diff = 90;
    } else {
      var numPub = getNumberOfPublishers(rootNodesMap[c]['LOCAL'])
      if (numPub == undefined || numPub == 0) {
        diff = 20;
      } else {
        diff = numPub * eachPubAngle;
      }
      angle += diff / 2;
    }
    var tree = d3.layout.tree()
      .separation(function (a, b) {
        if (a.region != b.region) {
          return 1;
        } else {
          return (a.children == b.children ? 3 : 3) / a.depth;
        }
      }).size([diff, r]);
    var mirrorNode = rootNodesMap[c]['MIRROR'];
    if (mirrorNode != undefined) {
      appendMergeMirrorTreesToSVG(graphsvg, tree, mirrorNode, angle, c, diff);
    }
    angle += diff / 2;  
  }

  cacheLinks();
  appendArrowMarkersToLinks(graphsvg);
  loadDefaultView();
  addLegendBox(graphsvg);
  addSummaryBox();
}

function drawGraph(result) {
  jsonresponse = JSON.parse(result);
  buildNodeMap();
  loadGraph(qStream, qCluster, qSelectedTab);
}