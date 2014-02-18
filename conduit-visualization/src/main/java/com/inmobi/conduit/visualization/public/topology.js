var jsonresponse;
var collectorIndex = 0;
var r = 180;
var yDiff = 40;
var fullTreeList = []; // Full list of Node objects grouped by respective cluster name
var degToRadFactor = Math.PI / 180;
var radToDegFactor = 180 / Math.PI;
var isNodeColored = false;
var rootNodes = [];
var pathLinkCache = [],
  lineLinkCache = [];
var aggCollectorNodeList = [];
var isVipNodeHighlighed = false; //this variable is used so that when loaing default view, same vip child tree need not be highlighted for every collector
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
function TopicStats(topic, messages, hostname) {
  this.topic = topic;
  this.messages = messages;
  this.hostname = hostname;
}

function TopicLatency(topic) {
  this.topic = topic;
  this.latencyList = [];
}

function StreamSource(topic) {
  this.topic = topic;
  this.source = [];
}

function Node(name, clusterName, tier, aggregatemessagesreceived,
  aggregatemessagesent) {
  this.name = name;
  this.clusterName = clusterName;
  this.tier = tier;
  this.aggregatemessagesreceived = aggregatemessagesreceived;
  this.aggregatemessagesent = aggregatemessagesent;
  this.allreceivedtopicstats = [];
  this.allsenttopicstats = [];
  this.children = [];
  this.source = []; //for merge and mirror tier nodes
  this.streamSourceList = []; //for merge and mirror tier nodes
  this.overallLatency = [];
  this.allTopicsLatency = [];
  this.point = new Point(0, 0);
}

function buildNodeList() {
  jsonresponse.nodes.forEach(function (n) {
    var node = new Node(n.name, n.cluster, n.tier, n.aggregatereceived,
      n.aggregatesent);
    if (node.tier.equalsIgnoreCase("collector")) {
      var aggNodeObj = undefined;
      for (var i = 0; i < aggCollectorNodeList.length; i++) {
        if (aggCollectorNodeList[i].clusterName == node.clusterName) {
          aggNodeObj = aggCollectorNodeList[i];
          break;
        }
      }
      if (aggNodeObj == undefined) {
        aggNodeObj = {
          clusterName: node.clusterName,
          received: 0,
          sent: 0
        };
        aggCollectorNodeList.push(aggNodeObj);
      }
      aggNodeObj.received += node.aggregatemessagesreceived;
      aggNodeObj.sent += node.aggregatemessagesent;
    }
    n.receivedtopicStatsList.forEach(function (t) {
      if (t.hostname !== undefined)
        node.allreceivedtopicstats.push(new TopicStats(t.topic,
          t.messages, t.hostname));
      else
        node.allreceivedtopicstats.push(new TopicStats(t.topic,
          t.messages));
    });
    n.senttopicStatsList.forEach(function (t) {
      if (t.hostname !== undefined)
        node.allsenttopicstats.push(new TopicStats(t.topic, t.messages,
          t.hostname));
      else
        node.allsenttopicstats.push(new TopicStats(t.topic, t.messages));
    });
    if (n.tier.equalsIgnoreCase("merge") || n.tier.equalsIgnoreCase("mirror")) {
      n.topicSource.forEach(function (s) {
        var obj = new StreamSource(s.topic);
        s.sourceList.forEach(function (sclus) {
          if (!obj.source.contains(sclus)) {
            obj.source.push(sclus);
          }
        });
        node.streamSourceList.push(obj);
      });
      node.source = n.source;
    }
    n.overallLatency.forEach(function (l) {
      node.overallLatency.push(new PercentileLatency(l.percentile,
        l.latency));
    });
    n.topicLatency.forEach(function (t) {
      var topicLatency = new TopicLatency(t.topic);
      t.percentileLatencyList.forEach(function (l) {
        topicLatency.latencyList.push(new PercentileLatency(
          l.percentile,
          l.latency));
      });
      node.allTopicsLatency.push(topicLatency);
    });
    var isAdded = false;
    for (var j = 0; j < fullTreeList.length; j++) {
      var clusterList = fullTreeList[j];
      if (node.clusterName == clusterList[0].clusterName) {
        clusterList.push(node);
        isAdded = true;
        break;
      }
    }
    if (!isAdded) {
      var newClusterList = [];
      newClusterList.push(node);
      fullTreeList.push(newClusterList);
    }
  });
}

function isLoss(count1, count2) {
  var marginAllowed = parseInt(percentageForLoss * count2, 10) / 100;
  if (count1 < (count2 - marginAllowed))
    return true;
  else
    return false;
}

function highlightChildNodes(n, isLoadDefaultView) {
  if (n.tier.equalsIgnoreCase("hdfs")) {
    highlightHDFSNode(n);
  } else {
    if (n.tier.equalsIgnoreCase("vip") && isVipNodeHighlighed &&
      isLoadDefaultView) {
      return;
    }
    var totalaggregatechild = 0;
    var totalaggregateparent = 0;
    totalaggregateparent = n.aggregatemessagesreceived;
    n.children.forEach(function (c) {
      totalaggregatechild += parseInt(c.aggregatemessagesreceived, 10);
    });
    if (n.tier.equalsIgnoreCase("vip") || n.tier.equalsIgnoreCase("local") || n
      .tier.equalsIgnoreCase("merge")) {
      if (n.children.length === 0) {
        for (var i = 0; i < rootNodes.length; i++) {
          if (rootNodes[i].tier.equalsIgnoreCase(n.tier) && rootNodes[i].clusterName ==
            n.clusterName) {
            n = rootNodes[i];
            break;
          }
        }
      }
      if (n.children.length == 0) {
        return;
      }
      n.children.forEach(function (c) {
        totalaggregatechild += parseInt(c.aggregatemessagessent, 10);
      });
    } else if (n.tier.equalsIgnoreCase("collector")) {
      for (var i = 0; i < aggCollectorNodeList.length; i++) {
        if (aggCollectorNodeList[i].clusterName == n.clusterName) {
          totalaggregateparent = aggCollectorNodeList[i].received;
          break;
        }
      }
    }
    n.children.forEach(function (c) {
      var currentLink;
      if (n.tier.equalsIgnoreCase("merge") || n.tier.equalsIgnoreCase(
        "mirror")) {
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
      if (n.allreceivedtopicstats.length === 0 || ((c.tier.equalsIgnoreCase(
            "agent") || c.tier.equalsIgnoreCase("collector")) && c
          .allsenttopicstats.length === 0) || c.allreceivedtopicstats.length ===
        0) {
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
  if (n.allTopicsLatency.length > 0) {
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
    if (c.allsenttopicstats.length === 0)
      link.style("fill", "none")
        .style("stroke", "#dedede")
        .each(function (d) {
          d.status = "nofill";
        });
    else {
      n.allreceivedtopicstats.forEach(function (t) {
        if (t.hostname == c.name) {
          c.allsenttopicstats.forEach(function (ct) {
            if (ct.topic == t.topic) {
              aggregateparent += parseInt(t.messages, 10);
              aggregatechild += parseInt(ct.messages, 10);
            }
          });
        }
      });
      if (n.allreceivedtopicstats.length === 0)
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
  for (var i = 0; i < n.overallLatency.length; i++) {
    l = n.overallLatency[i];
    if (l.percentile == percentileForSla) {
      if (l.latency > tierLatencySlaMap[n.tier.toLowerCase()])
        color = '#ff0000';
      break;
    }
  }
  if (color !== undefined) {
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
  if (n.children.length === 0 && (n.tier.equalsIgnoreCase("vip") || n.tier
    .equalsIgnoreCase("local") || n.tier.equalsIgnoreCase("merge"))) {
    for (var i = 0; i < rootNodes.length; i++) {
      if (rootNodes[i].tier.equalsIgnoreCase(n.tier) && rootNodes[i].clusterName ==
        n.clusterName) {
        n = rootNodes[i];
        break;
      }
    }
    if (n.children.length == 0) {
      return;
    }
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
    if (n.tier.equalsIgnoreCase("mirror") || n.tier.equalsIgnoreCase("merge")) {
      lineLinkCache.filter(function (d) {
        return d.source.clusterName == n.clusterName && d.source.name ==
          n.name && d.source.tier.equalsIgnoreCase(n.tier) && d.target.name ==
          c.name &&
          n.allTopicsLatency.length > 0 && c.allTopicsLatency.length > 0;
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
          c.name &&
          n.allTopicsLatency.length > 0 && c.allTopicsLatency.length > 0;
      })
        .transition()
        .duration(100)
        .style("fill", "none")
        .style("stroke", "#ADBCE6")
        .each(function (d) {
          d.status = "healthy";
        });
    }
    if (n.allTopicsLatency.length > 0) {
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
    if (n.tier.equalsIgnoreCase("agent") || n.tier.equalsIgnoreCase("collector")) {
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
  if (n.tier.equalsIgnoreCase("agent") || n.tier.equalsIgnoreCase("collector")) {
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
        "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
        receivedStats.topic + "', '" + n.clusterName +
        "', 'all', 1)\" class=\"transparentButton\">" + receivedStats.topic +
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
    });
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
      var aggregateList = [];
      n.allreceivedtopicstats.forEach(function (topicstats) {
        var isPresent = false;
        aggregateList.forEach(function (prevStats) {
          if (topicstats.topic == prevStats.topic) {
            var prevCount = parseInt(prevStats.messages, 10);
            var currCount = parseInt(topicstats.messages, 10);
            var finalCount = prevCount + currCount;
            prevStats.messages = finalCount;
            isPresent = true;
          }
        });
        if (!isPresent) {
          var newTopicStats = new TopicStats(topicstats.topic, topicstats.messages);
          aggregateList.push(newTopicStats);
        }
      });
      aggregateList.forEach(function (topicstats) {
        r = t.insertRow(currentRow);
        var cell = 0;
        c = r.insertCell(cell);
        c.innerHTML =
          "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
          topicstats.topic + "', '" + n.clusterName +
          "', 'all', 1)\" class=\"transparentButton\">" + topicstats.topic +
          "</button>";
        cell++;
        c = r.insertCell(cell);
        c.innerHTML = topicstats.messages;
        currentRow++;
      });
    } else {
      n.allreceivedtopicstats.forEach(function (topicstats) {
        r = t.insertRow(currentRow);
        var cell = 0;
        c = r.insertCell(cell);
        c.innerHTML =
          "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
          topicstats.topic + "', '" + n.clusterName +
          "', 'all', 1)\" class=\"transparentButton\">" + topicstats.topic +
          "</button>";
        cell++;
        c = r.insertCell(cell);
        c.innerHTML = topicstats.messages;
        currentRow++;
      });
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
  var percentileSet = [];
  n.overallLatency.forEach(function (l) {
    percentileSet.push(l.percentile);
  });
  percentileSet.sort(function (a, b) {
    return a - b;
  });
  var currentCol = 0;
  r = t.insertRow(currentRow++);
  c = r.insertCell(currentCol++);
  c.innerHTML = "<b>Overall/Topic</b>";
  percentileSet.forEach(function (p) {
    c = r.insertCell(currentCol++);
    c.innerHTML = "<b>" + p + "</b>";
  });
  n.allTopicsLatency.forEach(function (l) {
    var topic = l.topic;
    r = t.insertRow(currentRow++);
    var currentColumn = 0;
    c = r.insertCell(currentColumn++);
    c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
      topic + "', '" + n.clusterName +
      "', 'all', 2)\" class=\"transparentButton\">" + topic + "</button>";
    percentileSet.forEach(function (p) {
      var currentPercentile = p;
      l.latencyList.forEach(function (pl) {
        if (pl.percentile == currentPercentile) {
          c = r.insertCell(currentColumn++);
          if (pl.latency === 0)
            c.innerHTML = "<1";
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
        if (pl.latency === 0)
          c.innerHTML = "<1";
        else
          c.innerHTML = pl.latency;
      }
    });
  });
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
  if (l.source.tier.equalsIgnoreCase("merge") || l.source.tier.equalsIgnoreCase(
    "mirror")) {
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

function createNewObjectForTree(c, clusterNodeList, p) {
  var h = {
    name: c.name,
    tier: c.tier,
    clusterName: c.clusterName,
    aggregatemessagesreceived: c.aggregatemessagesreceived,
    allreceivedtopicstats: c.allreceivedtopicstats,
    aggregatemessagesent: c.aggregatemessagesent,
    allsenttopicstats: c.allsenttopicstats,
    overallLatency: c.overallLatency,
    allTopicsLatency: c.allTopicsLatency,
    point: c.point
  }
  if (p.tier.equalsIgnoreCase("collector") && c.tier.equalsIgnoreCase("vip")) {
    if (collectorIndex == 0) {
      h.children = travelTree(c, clusterNodeList);
      rootNodes.push(h);
    } else {
      h.children = [];
    }
    collectorIndex++;
  } else {
    h.children = travelTree(c, clusterNodeList);
  }
  return h;
}

function createNewObjectForMergeMirrorTree(c) {
  var h = {
    name: c.name,
    tier: c.tier,
    clusterName: c.clusterName,
    aggregatemessagesreceived: c.aggregatemessagesreceived,
    allreceivedtopicstats: c.allreceivedtopicstats,
    aggregatemessagesent: c.aggregatemessagesent,
    allsenttopicstats: c.allsenttopicstats,
    overallLatency: c.overallLatency,
    allTopicsLatency: c.allTopicsLatency,
    point: c.point,
    children: []
  }
  return h;
}

function travelTree(root, clusterNodeList) {
  var returnArray = [];
  var numCollectors;
  clusterNodeList.forEach(function (c) {
    var createNode = false;
    if (root.tier.equalsIgnoreCase("local") && c.tier.equalsIgnoreCase("hdfs")) {
      createNode = true;
    } else if (root.tier.equalsIgnoreCase("hdfs") && c.tier.equalsIgnoreCase(
      "collector")) {
      createNode = true;
    } else if (root.tier.equalsIgnoreCase("collector") && c.tier.equalsIgnoreCase(
      "vip")) {
      createNode = true;
    } else if (root.tier.equalsIgnoreCase("vip") && c.tier.equalsIgnoreCase(
      "agent")) {
      createNode = true;
    } else if (root.tier.equalsIgnoreCase("agent") && c.tier.equalsIgnoreCase(
      "publisher") && root.name == c.name) {
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

function getTierList(clusterNodeList, tier) {
  var returnArray = [];
  clusterNodeList.forEach(function (n) {
    if (n.tier.equalsIgnoreCase(tier))
      returnArray.push(n);
  });
  return returnArray;
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
          if (d.aggregatemessagesent < d.aggregatemessagesreceived) {
            color = "#ff0000";
          } else {
            color = tierColorMap[d.tier.toLowerCase()];
          }
        } else {
          for (var i = 0; i < d.overallLatency.length; i++) {
            if (d.overallLatency[i].percentile == percentileForSla) {
              if (d.overallLatency[i].latency > tierLatencySlaMap[d.tier.toLowerCase()]) {
                color = "#ff0000";
              } else {
                color = tierColorMap[d.tier.toLowerCase()];
              }
              break;
            }
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
  }
}

function loadDefaultView() {
  if (!isNodeColored) {
    var clear = true;
    rootNodes.forEach(function (n) {
      if (!n.tier.equalsIgnoreCase("vip")) {
        if (n.tier.equalsIgnoreCase("local")) {
          isVipNodeHighlighed = false;
        }
        nodeover(n, clear, true);
        if (clear)
          clear = false;
      }
    });
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

function clearHistory() {}

function popAllTopicStatsNotBelongingToStreamList(streams, treeList) {
  treeList.forEach(function (n) {
    var newReceivedStats = [];
    var newSentStats = [];
    var overallLatency = [];
    var allTopicsLatency = [];
    var topicSourceList = []; //for merge and mirror tier nodes
    var allSourceList = []; //for merge and mirror tier nodes
    n.aggregatemessagesreceived = 0;
    n.aggregatemessagesent = 0;
    n.allreceivedtopicstats.forEach(function (s) {
      for (var i = 0; i < streams.length; i++) {
        if (s.topic == streams[i]) {
          n.aggregatemessagesreceived += s.messages;
          newReceivedStats.push(new TopicStats(s.topic, s.messages, s.hostname));
          break;
        }
      }
    });
    n.allsenttopicstats.forEach(function (s) {
      for (var i = 0; i < streams.length; i++) {
        if (s.topic == streams[i]) {
          n.aggregatemessagesent += s.messages;
          newSentStats.push(new TopicStats(s.topic, s.messages, s.hostname));
          break;
        }
      }
    });
    n.allTopicsLatency.forEach(function (t) {
      var topic = t.topic;
      for (var i = 0; i < streams.length; i++) {
        if (topic == streams[i]) {
          var topicLateny = new TopicLatency(topic);
          t.latencyList.forEach(function (l) {
            topicLateny.latencyList.push(new PercentileLatency(l.percentile,
              l.latency));
            overallLatency.push(new PercentileLatency(l.percentile, l.latency));
          });
          allTopicsLatency.push(topicLateny);
          break;
        }
      }
    });
    n.streamSourceList.forEach(function (s) {
      var topic = s.topic;
      for (var i = 0; i < streams.length; i++) {
        if (topic == streams[i]) {
          var streamSource = new StreamSource(topic);
          s.source.forEach(function (sourceObj) {
            streamSource.source.push(sourceObj);
            if (!allSourceList.contains(sourceObj)) {
              allSourceList.push(sourceObj);
            }
          });
          topicSourceList.push(streamSource);
          break;
        }
      }
    });
    n.allreceivedtopicstats = newReceivedStats;
    n.allsenttopicstats = newSentStats;
    n.allTopicsLatency = allTopicsLatency;
    n.overallLatency = overallLatency;
    n.streamSourceList = topicSourceList;
    n.source = allSourceList;
  });
}

function cloneNode(n) {
  var newNode = new Node(n.name, n.clusterName, n.tier, n.aggregatemessagesreceived,
    n.aggregatemessagesent);
  n.allreceivedtopicstats.forEach(function (t) {
    if (t.hostname !== undefined)
      newNode.allreceivedtopicstats.push(new TopicStats(t.topic, parseInt(t.messages,
          10),
        t.hostname));
    else
      newNode.allreceivedtopicstats.push(new TopicStats(t.topic, parseInt(t.messages,
        10)));
  });
  n.allsenttopicstats.forEach(function (t) {
    if (t.hostname !== undefined)
      newNode.allsenttopicstats.push(new TopicStats(t.topic, parseInt(t.messages,
          10),
        t.hostname));
    else
      newNode.allsenttopicstats.push(new TopicStats(t.topic, parseInt(t.messages,
        10)));
  });
  if (n.tier.equalsIgnoreCase("merge") || n.tier.equalsIgnoreCase("mirror")) {
    n.streamSourceList.forEach(function (s) {
      var obj = new StreamSource(s.topic);
      s.source.forEach(function (sourceObj) {
        obj.source.push(sourceObj);
        if (!newNode.source.contains(sourceObj)) {
          newNode.source.push(sourceObj);
        }
      });
      newNode.streamSourceList.push(obj);
    });
  }
  n.overallLatency.forEach(function (l) {
    newNode.overallLatency.push(new PercentileLatency(l.percentile, l.latency));
  });
  n.allTopicsLatency.forEach(function (t) {
    var topicLatency = new TopicLatency(t.topic);
    t.latencyList.forEach(function (l) {
      topicLatency.latencyList.push(new PercentileLatency(l.percentile,
        l.latency));
    });
    newNode.allTopicsLatency.push(topicLatency);
  });
  return newNode;
}

function appendClusterTreeTillLocalToSVG(graphsvg, tree, clusterNodeList,
  startindex, angle, diff, clusterName) {
  var root = cloneNode(clusterNodeList[startindex]);
  root.children = travelTree(root, clusterNodeList);
  rootNodes.push(root);
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

function constructTreeForMergeMirror(root) {
  var returnArray = [];
  if (root.tier.equalsIgnoreCase("merge")) {
    for (var i = 0; i < rootNodes.length; i++) {
      if (rootNodes[i].tier.equalsIgnoreCase("local")) {
        root.source.forEach(function (s) {
          if (s == rootNodes[i].clusterName) {
            returnArray.push(createNewObjectForMergeMirrorTree(rootNodes[i]));
          }
        });
      }
    }
  } else if (root.tier.equalsIgnoreCase("mirror")) {
    for (var i = 0; i < rootNodes.length; i++) {
      if (rootNodes[i].tier.equalsIgnoreCase("merge")) {
        root.source.forEach(function (s) {
          if (s == rootNodes[i].clusterName) {
            returnArray.push(createNewObjectForMergeMirrorTree(rootNodes[i]));
          }
        });
      }
    }
  }
  return returnArray;
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

function appendMergeMirrorTreesToSVG(graphsvg, tree, node, angle, clusterName,
  diff) {
  var diagonal = d3.svg.diagonal.radial()
    .projection(function (d) {
      return [d.y, d.x / 180 * Math.PI];
    });
  var div = d3.select("body")
    .append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);
  var root = cloneNode(node);
  root.children = constructTreeForMergeMirror(root);
  rootNodes.push(root);
  var nodes = tree.nodes(root);
  var links = tree.links(nodes);
  setNodeAnglesForLocalNodes(angle, diff, nodes);
  nodes.forEach(function (n) {
    setCoordinatesOfNode(n);
  });
  var drawlink = graphsvg.selectAll("line.link")
    .filter(function (l) {
      return l.source.clusterName == node.clusterName && l.source.tier.equalsIgnoreCase(
        node.tier) && l.name == node.name;
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
      return d.clusterName == clusterName && d.tier.equalsIgnoreCase(node.tier) &&
        d.name == node.name;
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

function loadGraph(streamName, clusterName) {
  isNodeColored = false;
  rootNodes.length = 0;
  lineLinkCache.length = 0;
  pathLinkCache.length = 0;
  clearTopologyGraph();
  var treeList = getTreeList(streamName, clusterName);
  console.log("AAAA");
  console.log(treeList);
  var graphsvg = d3.select("#topologyPanel")
    .append("svg:svg")
    .style("stroke", "gray")
    .attr("width", r * 5)
    .attr("height", r * 5)
    .style("background", "#EBF4F8")
    .attr("id", "graphsvg")
    .append("svg:g")
    .attr("transform", "translate(" + r * 2.5 + "," + (r * 2.5) + ")");
  var divisions = treeList.length;
  var totalNumPub = 0;
  var clustersWithoutPub = 0;
  treeList.forEach(function (l) {
    var numPublishers = getNumOfNodes(l, "publisher");
    if (numPublishers == 0) {
      clustersWithoutPub++;
    } else {
      totalNumPub += numPublishers;
    }
  });
  var linelength = 9 * yDiff;
  var angleLeft = 360 - 20 * clustersWithoutPub;
  var eachPubAngle = angleLeft / totalNumPub;
  var diff = 0,
    angle = 0,
    lineAngle = 0;
  //first append all nodes till local to the svg so that when merge nodes are appended all local nodes are available when retrieving them from the svg using d3.selectAll(). Similarly append all merge nodes before appending mirror nodes
  for (var index = 0; index < treeList.length; index++) {
    if (divisions == 1) {
      angle = 90;
      diff = 90;
    } else {
      var numPub = getNumOfNodes(treeList[index], 'publisher');
      if (numPub == 0) {
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
    if (treeList.length != 1) {
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
    collectorIndex = 0;
    var clusterNodeList = treeList[index];
    var currentCluster = clusterNodeList[0].clusterName;
    var startindex = getStartIndex(clusterNodeList);
    if (startindex === undefined) {
      addClusterName(currentCluster, tree, angle, graphsvg);
      angle += diff / 2;
      continue;
    }
    appendClusterTreeTillLocalToSVG(graphsvg, tree, clusterNodeList, startindex,
      angle, diff, currentCluster);
    addClusterName(currentCluster, tree, angle, graphsvg);
    angle += diff / 2;
  }
  angle = 0;
  diff = 0;
  for (index = 0; index < treeList.length; index++) {
    if (divisions == 1) {
      angle = 90;
      diff = 90;
    } else {
      var numPub = getNumOfNodes(treeList[index], 'publisher');
      if (numPub == 0) {
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
    var clusterNodeList = treeList[index];
    if (getNumOfNodes(clusterNodeList, "merge") != 0) {
      var currentCluster = clusterNodeList[0].clusterName;
      var mergeList = getTierList(clusterNodeList, "merge");
      appendMergeMirrorTreesToSVG(graphsvg, tree, mergeList[0], angle,
        clusterName, diff);
    }
    angle += diff / 2;
  }
  angle = 0;
  diff = 0;
  for (index = 0; index < treeList.length; index++) {
    if (divisions == 1) {
      angle = 90;
      diff = 90;
    } else {
      var numPub = getNumOfNodes(treeList[index], 'publisher');
      if (numPub == 0) {
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
    var clusterNodeList = treeList[index];
    if (getNumOfNodes(clusterNodeList, "mirror") != 0) {
      var currentCluster = clusterNodeList[0].cluster;
      var mirrorList = getTierList(clusterNodeList, "mirror");
      appendMergeMirrorTreesToSVG(graphsvg, tree, mirrorList[0], angle,
        clusterName, diff);
    }
    angle += diff / 2;
  }
  cacheLinks();
  appendArrowMarkersToLinks(graphsvg);
  loadDefaultView();
  addLegendBox(graphsvg);
  addSummaryBox(treeList);
}

function drawGraph(result) {
  fullTreeList.length = 0;
  aggCollectorNodeList.length = 0;
  console.log("topology result:" + result);
  jsonresponse = JSON.parse(result);
  clearHistory();
  buildNodeList();
  loadGraph(qStream, qCluster, qSelectedTab);
}

function checkAndClearList(treeList) {
  for (var k = 0; k < treeList.length; k++) {
    var clusterList = treeList[k];
    for (var i = 0; i < clusterList.length; i++) {
      var currentNode = clusterList[i];
      if (currentNode.tier.equalsIgnoreCase("local") && currentNode.allreceivedtopicstats
        .length == 0) {
        treeList.splice(k, 1);
      }
    }
  }
}

function getTreeList(streamName, clusterName) {
  var treeList = [];
  if (!clusterName.equalsIgnoreCase('all')) {
    if (fullTreeList.length != 0) {
      var clusterList = [];
      var streams = [];
      var mirrorSourceList;
      var mergeSourceList = [];
      for (var i = 0; i < fullTreeList.length; i++) {
        var l = fullTreeList[i];
        if (l[0] != undefined && l[0].clusterName == clusterName) {
          l.forEach(function (nodeInCluster) {
            var clonedNode = cloneNode(nodeInCluster);
            if (clonedNode.tier.equalsIgnoreCase("merge") || clonedNode.tier.equalsIgnoreCase(
              "mirror")) {
              if (clonedNode.tier.equalsIgnoreCase("mirror")) {
                mirrorSourceList = clonedNode.source;
              }
              if (clonedNode.tier.equalsIgnoreCase("merge")) {
                clonedNode.source.forEach(function (s) {
                  if (!mergeSourceList.contains(s) && s != clusterName) {
                    mergeSourceList.push(s);
                  }
                });
              }
              clonedNode.streamSourceList.forEach(function (s) {
                if (!streams.contains(s.topic)) {
                  streams.push(s.topic);
                }
              });
            }
            clusterList.push(clonedNode);
          });
          treeList.push(clusterList);
          break;
        }
      }
      if (getNumOfNodes(clusterList, "mirror") != 0) {
        fullTreeList.forEach(function (l) {
          if (l[0] != undefined && l[0].clusterName != clusterName &&
            mirrorSourceList.contains(l[0].clusterName)) {
            var mirrorClusterList = [];
            for (var i = 0; i < l.length; i++) {
              var nodeInCluster = l[i];
              // Add only the merge node and not the full tree for the source cluster as the source of merge may/may not have the current cluster as source
              if (nodeInCluster.tier.equalsIgnoreCase("merge")) {
                var clonedNode = cloneNode(nodeInCluster);
                clonedNode.source.forEach(function (s) {
                  if (!mergeSourceList.contains(s) && s != clusterName) {
                    mergeSourceList.push(s);
                  }
                });
                mirrorClusterList.push(clonedNode);
                break;
              }
            }
            treeList.push(mirrorClusterList);
          }
        });
      }
      for (var i = 0; i < mergeSourceList.length; i++) {
        var sourceCluster = mergeSourceList[i];
        var isPresent = false;
        var isLocalPresent = false;
        var clusterList = [];
        for (var j = 0; j < treeList.length; j++) {
          var list = treeList[j];
          if (list[0] != undefined && list[0].clusterName == sourceCluster) {
            clusterList = treeList[j];
            isPresent = true;
            if (getNumOfNodes(list, "local") != 0) {
              isLocalPresent = true;
            }
            break;
          }
        }
        if (isPresent && isLocalPresent) {
          continue;
        } else {
          var tree = [];
          if (isPresent && !isLocalPresent) {
            tree = clusterList;
          }
          fullTreeList.forEach(function (lst) {
            if (lst[0] != undefined && lst[0].clusterName == sourceCluster) {
              lst.forEach(function (nodeInCluster) {
                if (!nodeInCluster.tier.equalsIgnoreCase("mirror") && !
                  nodeInCluster.tier.equalsIgnoreCase("merge")) {
                  tree.push(cloneNode(nodeInCluster));
                }
              });
              if (!isPresent) {
                treeList.push(tree);
              }
            }
          });
        }
      }
      if (!streamName.equalsIgnoreCase('all')) {
        streams.length = 0;
        streams.push(streamName);
        treeList.forEach(function (clusterList) {
          popAllTopicStatsNotBelongingToStreamList(streams, clusterList);
        });
      } else {
        treeList.forEach(function (clusterList) {
          if (clusterList[0] != undefined && clusterList[0].clusterName !=
            clusterName) {
            popAllTopicStatsNotBelongingToStreamList(streams, clusterList);
          }
        });
      }
    }
  } else {
    treeList = fullTreeList;
  }
  checkAndClearList(treeList);
  return treeList;
}

function getStartIndex(nodeList) {
  var startindex = undefined;
  if (getNumOfNodes(nodeList, "local") > 0) {
    for (i = 0; i < nodeList.length; i++) {
      if (nodeList[i].tier.equalsIgnoreCase("local")) {
        startindex = i;
        break;
      }
    }
  }
  return startindex;
}