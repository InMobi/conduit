var jsonresponse;
var collectorIndex = 0;
var r = 180;
var yDiff = 40;
var fullTreeList = []; // Full list of Node objects grouped by respective cluster name
var hexcodeList = ["#FF9C42", "#DD75DD", "#C69C6E", "#FF86C2", "#F7977A",
  "ae8886", "#fb6183", "#8e4804"
];
var degToRadFactor = Math.PI / 180;
var radToDegFactor = 180 / Math.PI;
var publisherSla, agentSla, vipSla, collectorSla, hdfsSla, localSla, mergeSla,
  mirrorSla, percentileForSla,
  percentageForLoss, percentageForWarn, lossWarnThresholdDiff;
var publisherLatency = 0,
  agentLatency = 0,
  collectorLatency = 0,
  hdfsLatency = 0,
  localLatency = 0,
  mergeLatency = 0,
  mirrorLatency = 0;
var qStream, qCluster, qstart, qend;
var isGWTDevMode;

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

function PercentileLatency(percentile, latency) {
  this.percentile = percentile;
  this.latency = latency;
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
    if (n.tier.toLowerCase() == "merge" || n.tier.toLowerCase() == "mirror") {
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
  var marginAllowed = parseFloat(parseInt(percentageForLoss * count2) / 100, 10);
  if (count1 < (count2 - marginAllowed))
    return true;
  else
    return false;
}

function highlightChildNodes(n) {
  if (n.tier.toLowerCase() == "hdfs") {
    highlightHDFSNode(n);
  } else {
    var totalaggregatechild = 0;
    var totalaggregateparent = 0;
    totalaggregateparent = n.aggregatemessagesreceived;
    n.children.forEach(function (c) {
      totalaggregatechild += parseInt(c.aggregatemessagesreceived, 10);
    });
    if (n.tier.toLowerCase() == "vip" || n.tier.toLowerCase() == "local" || n.tier
      .toLowerCase() == "merge") {
      /*
      VIP, LOCAL and MERGE nodes can be dummy nodes if picked up from type1,
      type2 and type3 graphs respectively i.e they have no children. So
      to find the child nodes of these nodes, to complete traversal,
      filter and find the node with child nodes.
      */
      if (n.children.length === 0) {
        var arr = d3.selectAll("g.node")
          .filter(function (d) {
            return d.tier == n.tier && d.clusterName == n.clusterName && (d.children
              .length > 0);
          })
          .data();
        n = arr[0];
      }
      /*
       n == undefined can happen when there is no node in the graph with
       same tier and cluster and which has children. One scenario this can
       happen is when we drill down on cluster which has a mirror node. When
       creating a partial treeList, only dummy merge nodes of corresponding
       source clusters of mirror node are added i.e type 3 trees. The sources
       of the source merge nodes i.e type 2 trees are not added to treeList
       So no merge nodes would be present in the treeList with number of
       children > 0
      */
      if (n == undefined)
        return;
      n.children.forEach(function (c) {
        totalaggregatechild += parseInt(c.aggregatemessagessent, 10);
      });
    } else if (n.tier.toLowerCase() == "collector") {
      /*
      VIP sends messages to all collectors in cluster and it is not possible
      to decipher which collector a message is sent to. Hence,
      all collector stats for a cluster are aggregated to be compared with
      vip node. If there is a message loss between collectors and vip,
      then links between vip and each collector, will be red.
      */
      totalaggregateparent = 0;
      var nodesInCluster = d3.selectAll("g.node")
        .filter(function (d) {
          return d.clusterName == n.clusterName && d.tier.toLowerCase() ==
            "collector";
        })
        .data();
      nodesInCluster.forEach(function (c) {
        totalaggregateparent += parseInt(c.aggregatemessagesreceived, 10);
      });
    }
    n.children.forEach(function (c) {
      var currentLink = d3.selectAll("line.link")
        .filter(function (d) {
          return d.source.clusterName == n.clusterName && d.source.name ==
            n.name &&
            d.source.tier == n.tier && d.target.name == c.name;
        })
        .transition()
        .duration(100);
      if (n.allreceivedtopicstats.length === 0 || ((c.tier == "agent" || c.tier ==
          "collector") && c.allsenttopicstats.length === 0) || c.allreceivedtopicstats
        .length === 0) {
        currentLink.style("fill", "none")
          .style("stroke", "#dedede");
      } else if (isLoss(totalaggregateparent, totalaggregatechild)) {
        currentLink.style("fill", "none")
          .style("stroke", "#ff0000");
      } else {
        currentLink.style("fill", "none")
          .style("stroke", "#00ff00");
      }
    });
  }
  n.children.forEach(function (c) {
    highlightChildNodes(c);
  });
}

function highlightHDFSNode(n) {
  n.children.forEach(function (c) {
    var aggregateparent = 0;
    var aggregatechild = 0;
    var link = d3.selectAll("line.link")
      .filter(function (d) {
        return d.source.clusterName == n.clusterName && d.source.name ==
          n.name && d.source.tier == n.tier && d.target.name == c.name;
      })
      .transition()
      .duration(100);
    if (c.allsenttopicstats.length === 0)
      link.style("fill", "none")
        .style("stroke", "#dedede");
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
          .style("stroke", "#dedede");
      else if (isLoss(aggregateparent, aggregatechild))
        link.style("fill", "none")
          .style("stroke", "#ff0000");
      else
        link.style("fill", "none")
          .style("stroke", "#00ff00");
    }
  });
}

function nodeover(n, isCountView, clear) {
  if (clear) {
    d3.selectAll("line.link")
      .transition()
      .duration(100)
      .style("fill", "none")
      .style("stroke", "#dedede");
  }
  if (isCountView)
    highlightChildNodes(n);
  else
    latencyhighlightChildNodes(n);
}

function latencyhighlightChildNodes(n) {
  if (n.children.length === 0 && (n.tier.toLowerCase() == "vip" || n.tier.toLowerCase() ==
    "local" || n.tier.toLowerCase() == "merge")) {
    var arr = d3.selectAll("g.node")
      .filter(function (d) {
        return d.tier == n.tier && d.clusterName == n.clusterName && (d.children
          .length > 0);
      })
      .data();
    n = arr[0];
    if (n == undefined) {
      return;
    }
  }
  var graphnode = d3.selectAll("g.node")
    .filter(function (d) {
      return d.name == n.name && d.clusterName == n.clusterName && d.tier ==
        n.tier;
    })
    .select("circle")
    .transition()
    .duration(100);
  var color;
  for (var i = 0; i < n.overallLatency.length; i++) {
    l = n.overallLatency[i];
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
      break;
    }
  }
  if (color !== undefined) {
    graphnode.style("fill", color);
  }
  n.children.forEach(function (c) {
    d3.selectAll("line.link")
      .filter(function (d) {
        return d.source.clusterName == n.clusterName && d.source.name ==
          n.name &&
          d.source.tier == n.tier && d.target.name == c.name &&
          n.allTopicsLatency
          .length > 0 && c.allTopicsLatency.length > 0;
      })
      .transition()
      .duration(100)
      .style("fill", "none")
      .style("stroke", "#ADBCE6");
    latencyhighlightChildNodes(c);
  });
}

function addListToInfoPanel(n, isCountView) {
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
  c.innerHTML = n.tier;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "clusterName:";
  c = r.insertCell(1);
  if (isCountView) {
    c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('All', '" + n.clusterName + "', 1)\" class=\"transparentButton\">" + n.clusterName + "</button>";
    r = t.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "Aggregate Received:";
    c = r.insertCell(1);
    c.innerHTML = n.aggregatemessagesreceived;
    if (n.tier == "agent" || n.tier == "collector") {
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
    c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('All', '" + n.clusterName + "', 2)\" class=\"transparentButton\">" + n.clusterName + "</button>";
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
  d3.selectAll("line.link")
    .style("stroke-width", "2px");
  d3.selectAll("g.node")
    .select("circle")
    .attr("r", 5);
  d3.selectAll("g.node")
    .filter(function (d) {
      return d.clusterName == n.clusterName && d.name == n.name && d.tier ==
        n.tier;
    })
    .select("circle")
    .attr("r", 7);
  addListToInfoPanel(n, true);
  var c, r, t;
  var currentRow = 0;
  t = document.createElement('table');
  if (n.tier == "agent" || n.tier == "collector") {
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
      c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" + receivedStats.topic + "', '" + n.clusterName + "', 1)\" class=\"transparentButton\">" + receivedStats.topic + "</button>";
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
    if (n.tier.toLowerCase() == "hdfs") {
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
        c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" + topicstats.topic + "', '" + n.clusterName + "', 1)\" class=\"transparentButton\">" + topicstats.topic + "</button>";
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
        c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" + topicstats.topic + "', '" + n.clusterName + "', 1)\" class=\"transparentButton\">" + topicstats.topic + "</button>";
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
  d3.selectAll("line.link")
    .style("stroke-width", "2px");
  d3.selectAll("g.node")
    .select("circle")
    .attr("r", 5);
  d3.selectAll("g.node")
    .filter(function (d) {
      return d.clusterName == n.clusterName && d.name == n.name && d.tier ==
        n.tier;
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
    c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" + topic + "', '" + n.clusterName + "', 2)\" class=\"transparentButton\">" + topic + "</button>";
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
  if (l.source.tier.toLowerCase() == "hdfs") {
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
  } else if (l.source.tier.toLowerCase() == "collector") {
    var linkList = d3.selectAll("line.link")
      .filter(function (d) {
        return d.source.clusterName == l.source.clusterName && d.source
          .tier.toLowerCase() == "collector";
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
  } else if (l.source.tier.toLowerCase() == "merge" || l.source.tier.toLowerCase() ==
    "mirror") {
    l.source.allreceivedtopicstats.forEach(function (s) {
      var topic = s.topic;
      var topicSourceList;
      l.source.streamSourceList.forEach(function (sourceStream) {
        if (sourceStream.topic == topic) {
          topicSourceList = sourceStream.source;
        }
      });
      if (topicSourceList.contains(l.target.tier)) {
        l.target.allreceivedtopicstats.forEach(function (t) {
          if (t.topic == topic) {
            isstreampresent = true;
            if (isLoss(s.messages, t.messages))
              streamslist.push(t.topic);
          }
        });
        if (!isstreampresent && !(streamslist.contains(topic)))
          streamslist.push(topic);
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
  d3.selectAll("line.link")
    .style("stroke-width", "2px");
  d3.selectAll("g.node")
    .select("circle")
    .attr("r", 5);
  d3.selectAll("line.link")
    .filter(function (d) {
      return d.target.clusterName == l.target.clusterName && d.target.name ==
        l.target.name && d.target.tier == l.target.tier && d.source.clusterName ==
        l.source.clusterName && d.source.name == l.source.name && d.source.tier ==
        l.source.tier;
    })
    .style("stroke-width", "5px");
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
  c.innerHTML = l.target.tier;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Source cluster:";
  c = r.insertCell(1);
  c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('All', '" + l.target.clusterName + "', 1)\" class=\"transparentButton\">" + l.target.clusterName + "</button>";
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Target Name:";
  c = r.insertCell(1);
  c.innerHTML = l.source.name;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Target Tier:";
  c = r.insertCell(1);
  c.innerHTML = l.source.tier;
  r = t.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Target Cluster:";
  c = r.insertCell(1);
  c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('All', '" + l.source.clusterName + "', 1)\" class=\"transparentButton\">" + l.source.clusterName + "</button>";
  var streams = getStreamsCausingDataLoss(l);
  if (streams.length > 0) {
    r = t.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "<b>Streams causing dataloss: </b>";
    streams.forEach(function (s) {
      r = t.insertRow(currentRow++);
      c = r.insertCell(0);
      c.innerHTML = "<button type=\"button\" onclick=\"saveHistoryAndLoadGraph('" + s + "', '" + l.source.clusterName + "', 1)\" class=\"transparentButton\">" + s + "</button>";
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
        if (c.children.length === 0) {
          c.children = collectorchildren;
        }
      });
    }
  }
}

function createNewObjectForTree(c, clusterNodeList, p, isMergeMirrorTree) {
  var h = {};
  h.name = c.name;
  h.tier = c.tier;
  h.clusterName = c.clusterName;
  h.aggregatemessagesreceived = c.aggregatemessagesreceived;
  h.allreceivedtopicstats = c.allreceivedtopicstats;
  h.aggregatemessagesent = c.aggregatemessagesent;
  h.allsenttopicstats = c.allsenttopicstats;
  h.overallLatency = c.overallLatency;
  h.allTopicsLatency = c.allTopicsLatency;
  h.point = c.point;
  if (isMergeMirrorTree || (p.tier.toLowerCase() == "collector" && c.tier.toLowerCase() == "vip" && collectorIndex != 0)) {
    h.children = [];
  } else {
    h.children = travelTree(c, clusterNodeList);
  }
  return h;
}

function travelTree(root, clusterNodeList) {
  var returnArray = [];
  var numCollectors;
  clusterNodeList.forEach(function (c) {
    var createNode = false;
    if (root.tier.toLowerCase() == "local" && c.tier.toLowerCase() ==
      "hdfs") {
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
      returnArray.push(createNewObjectForTree(c, clusterNodeList, root, false));
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
    if (n.tier.toLowerCase() == "mirror") {
      n.x = angle;
      n.y = yDiff;
    } else if (n.tier.toLowerCase() == "merge") {
      n.x = angle;
      n.y = 2 * yDiff;
    } else if (n.tier.toLowerCase() == "local") {
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
}

function drawLinesToMarkDifferentClusters(graphsvg, divisions, r) {
  if (divisions != 1) {
    var angle = 2 * Math.PI / divisions;
    var radialPoints = [];
    for (var k = 0; k < divisions; k++)
      radialPoints.push([r * Math.cos((angle * k) - (Math.PI / 2)), r *
        Math.sin(
          (angle * k) - (Math.PI / 2))
      ]);
    graphsvg.selectAll("line")
      .data(radialPoints)
      .enter()
      .append("svg:line")
      .attr(
        "x1", 0)
      .attr("y1", 0)
      .attr("x2", function (p) {
        return p[0];
      })
      .attr("y2", function (p) {
        return p[1];
      })
      .attr("stroke", "#ccc");
  }
}

function drawConcentricCircles(graphsvg, divisions) {
  var rad = [];
  var index = 8;
  for (var i = 1; i <= index; i++)
    rad.push(i * yDiff);
  rad.sort(function (a, b) {
    return a - b;
  });
  for (var j = rad.length; j > -1; j--) {
    graphsvg.append("circle")
      .attr("r", rad[j])
      .style("fill", "#E3F0F6")
      .style(
        "stroke-dasharray", ("3, 3"))
      .style("stroke", "#ccc");
  }
  var r = rad[rad.length - 1] + yDiff;
  drawLinesToMarkDifferentClusters(graphsvg, divisions, r);
}

function getTierList(clusterNodeList, tier) {
  var returnArray = [];
  clusterNodeList.forEach(function (n) {
    if (n.tier == tier)
      returnArray.push(n);
  });
  return returnArray;
}

function addClusterName(clusterName, tree, angle, graphsvg, selectedTabID) {
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
      saveHistoryAndLoadGraph('All', d.name, selectedTabID);
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
  } else if (isWarn(count1, count2)) {
    return 1;
  } else if (isLoss(count1, count2)) {
    return 2;
  } else {
    return 0;
  }
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

function getComparableCountValue(sourceTier, targetTier, treeList) {
  var count = 0;
  var targetNodes = d3.selectAll("g.node")
    .filter(function (d) {
      return d.tier.toLowerCase() == targetTier && d.children.length > 0;
    })
    .data();
  if (targetNodes == undefined || targetNodes.length == 0) {
    /*
    There are no nodes in the graph with the targetTier(merge/mirror) with
    number of children > 0, so calculating count is not applicable. Hence
    returning -1
    */
    return -1;
  }
  targetNodes.forEach(function (tnode) {
    tnode.streamSourceList.forEach(function (topicList) {
      var topic = topicList.topic;
      topicList.source.forEach(function (cluster) {
        var sourceNodeList = d3.selectAll("g.node")
          .filter(function (d) {
            return d.tier.toLowerCase() == sourceTier && d.clusterName ==
              cluster && d.children.length != 0;
          })
          .data();
        if (sourceNodeList == undefined || sourceNodeList.length == 0) {
          return -1;
        }
        sourceNodeList.forEach(function (n) {
          n.allreceivedtopicstats.forEach(function (stats) {
            if (stats.topic == topic) {
              count += stats.messages;
            }
          });
        });
      });
    });
  });
  return count;
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
  /*
    MERGE and MIRROR tiers's numbers cannot be compared directly since not all
    LOCAL streams have to MERGE and not all MERGE streams are mirrored;
    Also, one stream can be mirrored to multiple clusters, in this scenario,
    for the many mirrored clusters, each cluster number should match with
    merge numbers i.e if mirrored to n clusters, then sum(all mirrrored for
    that stream) == n * merge numbers for that stream.
  */
  var comparableLocalNum = getComparableCountValue("local", "merge", treeList);
  var comparableMergeNum = getComparableCountValue("merge", "mirror", treeList);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Merge",
    mergeCount, 0, comparableLocalNum, true);
  currentRow = addTierCountDetailsToSummary(div, currentRow, "Mirror",
    mirrorCount, 0, comparableMergeNum, true);
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

function addSummaryBox(isCountView, treeList) {
  if (isCountView) {
    loadCountSummary(treeList);
  } else {
    loadLatencySummary();
  }
}

function addLegendBox(graphsvg) {
  var tierList = ["Publisher", "Agent", "VIP", "Collector", "HDFS", "Local",
    "Merge", "Mirror"
  ];
  var inc = 100;
  for (var i = 0; i < tierList.length; i++) {
    graphsvg.append("circle")
      .attr("class", "legendColor")
      .attr("r", 5)
      .attr("cx", -r * 2.5 + 10 + (i) * inc)
      .attr("cy", -r * 2.5 + 10)
      .style("fill", hexcodeList[i])
      .style("stroke", hexcodeList[i]);
    graphsvg.append("text")
      .attr("class", "legend")
      .text(tierList[i])
      .attr("x", -r * 2.5 + 20 + (i) * inc)
      .attr("y", -r * 2.5 + 15);
  }
}

function loadDefaultView(isCountView) {
  var nodeOverList = d3.selectAll("g.node")
    .filter(function (d) {
      return (d.tier.toLowerCase() == "local" && d.children.length > 0) || d.tier
        .toLowerCase() == "merge" || d.tier.toLowerCase() == "mirror";
    })
    .data();
  var clear = true;
  nodeOverList.forEach(function (n) {
    nodeover(n, isCountView, clear);
    if (clear)
      clear = false;
  });
  addColorsToNodes();
}

function clearHistory() {}

function clearPreviousGraph() {
  document.getElementById("summaryPanel")
    .innerHTML = "";
  document.getElementById("summaryPanel")
    .style.backgroundColor = "#D8EAF3";
  document.getElementById("infoPanel")
    .innerHTML = "";
  document.getElementById("infoPanel")
    .style.backgroundColor = "#D8EAF3";
  d3.select("#graphsvg")
    .remove();
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
      }, "Databus Visualization", url);
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

function saveHistoryAndLoadGraph(streamName, clusterName, selectedTabID) {
  saveHistory(streamName, clusterName, selectedTabID);
  loadGraph(streamName, clusterName, selectedTabID);
}

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
            allSourceList.push(sourceObj);
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

function cloneNode(n, cloneCoords) {
  var newNode = new Node(n.name, n.clusterName, n.tier, n.aggregatemessagesreceived,
    n.aggregatemessagesent);
  n.allreceivedtopicstats.forEach(function (t) {
    if (t.hostname !== undefined)
      newNode.allreceivedtopicstats.push(new TopicStats(t.topic, t.messages,
        t.hostname));
    else
      newNode.allreceivedtopicstats.push(new TopicStats(t.topic, t.messages));
  });
  n.allsenttopicstats.forEach(function (t) {
    if (t.hostname !== undefined)
      newNode.allsenttopicstats.push(new TopicStats(t.topic, t.messages,
        t.hostname));
    else
      newNode.allsenttopicstats.push(new TopicStats(t.topic, t.messages));
  });
  if (n.tier == "merge" || n.tier == "mirror") {
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
  if (cloneCoords != undefined && cloneCoords == true) {
    newNode.point.x = n.point.x;
    newNode.point.y = n.point.y;
  }
  return newNode;
}

function appendClusterTreeTillLocalToSVG(graphsvg, tree, clusterNodeList,
  startindex, angle, diff, isCountView, clusterName) {
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
  setNodesAngles(angle, diff, nodes);
  nodes.forEach(function (n) {
    setCoordinatesOfNode(n);
  });
  var drawlink = graphsvg.selectAll("line.link")
    .filter(function (d, i) {
      return d.clusterName == clusterName;
    })
    .data(links)
    .enter()
    .append("svg:line")
    .attr("class", "link")
    .attr('id', function (d) {
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
      nodeover(n, isCountView, true);
    })
    .on("mouseout", function (n) {
      div.transition()
        .duration(500)
        .style("opacity", 0);
      loadDefaultView(isCountView);
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
  if (root.tier.toLowerCase() == "merge") {
    var localNodeList = d3.selectAll("g.node")
      .filter(function (d) {
        return d.tier.toLowerCase() == "local";
      })
      .data();
    localNodeList.forEach(function (l) {
      root.source.forEach(function (s) {
        if (s == l.clusterName) {
          returnArray.push(createNewObjectForTree(l, undefined, root, true));
        }
      });
    });
  } else if (root.tier.toLowerCase() == "mirror") {
    var mergeNodeList = d3.selectAll("g.node")
      .filter(function (d) {
        return d.tier.toLowerCase() == "merge";
      })
      .data();
    mergeNodeList.forEach(function (m) {
      root.source.forEach(function (s) {
        if (s == m.clusterName) {
          returnArray.push(createNewObjectForTree(m, undefined, root, true));
        }
      });
    });
  }
  return returnArray;
}

function setNodeAnglesForLocalNodes(angle, diff, nodes) {
  nodes.forEach(function (n) {
    var arr = d3.selectAll('g.node')
      .filter(function (d) {
        return d.name == n.name && d.clusterName == n.clusterName && d.tier ==
          n.tier;
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

function appendMergeMirrorTreesToSVG(graphsvg, tree, node, angle, isCountView,
  clusterName, diff) {
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
  var nodes = tree.nodes(root);
  var links = tree.links(nodes);
  setNodeAnglesForLocalNodes(angle, diff, nodes);
  nodes.forEach(function (n) {
    setCoordinatesOfNode(n);
  });
  var drawlink = graphsvg.selectAll("line.link")
    .filter(function (l) {
      return l.source.clusterName == clusterName && l.source.tier.toLowerCase() ==
        node.tier.toLowerCase();
    })
    .data(links);
  var linkEnter = drawlink.enter()
    .append("svg:g");
  var pathEl = linkEnter.append("svg:line")
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
    .style(
      "stroke-dasharray", ("7, 2"))
    .style("stroke-width", "2px")
    .style("fill", 'none');
  var drawnode = graphsvg.selectAll("g.node")
    .filter(function (d) {
      return d.clusterName == clusterName && d.tier.toLowerCase() == node.tier
        .toLowerCase() && d.name == node.name;
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
      nodeover(n, isCountView, true);
    })
    .on("mouseout", function (n) {
      div.transition()
        .duration(500)
        .style("opacity", 0);
      loadDefaultView(isCountView);
    });
  if (isCountView) {
    drawlink.style("cursor", "pointer")
      .on("click", linkclick);
    drawnode.on("click", nodeclick);
  } else
    drawnode.on("click", latencynodeclick);
}

function checkCountView(selectedTabID) {
  var isCountView;
  if (parseInt(selectedTabID, 10) == 1)
    isCountView = true;
  else if (parseInt(selectedTabID, 10) == 2)
    isCountView = false;
  return isCountView;
}

function Point(x, y) {
  this.x = x;
  this.y = y;
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
  var links = d3.selectAll('line.link')
    .data();
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

function loadGraph(streamName, clusterName, selectedTabID) {
  highlightTab(selectedTabID);
  var isCountView = checkCountView(selectedTabID);
  clearPreviousGraph();
  var treeList = getTreeList(streamName, clusterName);
  var graphsvg = d3.select("#graphPanel")
    .append("svg:svg")
    .style("stroke",
      "gray")
    .attr("width", r * 5)
    .attr("height", r * 5)
    .style("background", "#D8EAF3")
    .attr("id", "graphsvg")
    .append("svg:g")
    .attr("transform",
      "translate(" + r * 2.5 + "," + (r * 2.5) + ")");
  var divisions = treeList.length;
  var angle = 360 / (2 * divisions);
  var diff = 360 / divisions;
  if (divisions == 1) {
    angle = 90;
    diff = 90;
  }
  var tree = d3.layout.tree()
    .size([diff, r])
    .separation(function (a, b) {
      if (a.region != b.region) {
        return 1;
      } else {
        return (a.children == b.children ? 3 : 3) / a.depth;
      }
    });
  drawConcentricCircles(graphsvg, divisions);

  /*
		first append all nodes till local to the svg so that when merge nodes are
		appended all local nodes are available when retrieving them from the svg
		using d3.selectAll(). Similarly append all merge nodes before appending
		mirror nodes
  */
  for (var index = 0; index < treeList.length; index++) {
    collectorIndex = 0;
    var clusterNodeList = treeList[index];
    var currentCluster = clusterNodeList[0].clusterName;
    var startindex = getStartIndex(clusterNodeList);
    if (startindex === undefined) {
      addClusterName(currentCluster, tree, angle, graphsvg, selectedTabID);
      angle += diff;
      continue;
    }
    appendClusterTreeTillLocalToSVG(graphsvg, tree, clusterNodeList, startindex,
      angle, diff, isCountView, currentCluster);
    addClusterName(currentCluster, tree, angle, graphsvg, selectedTabID);
    angle += diff;
  }

  angle = 360 / (2 * divisions);
  if (divisions == 1) {
    angle = 90;
  }
  for (index = 0; index < treeList.length; index++) {
    var clusterNodeList = treeList[index];
    if (getNumOfNodes(clusterNodeList, "merge") != 0) {
      var currentCluster = clusterNodeList[0].clusterName;
      var mergeList = getTierList(clusterNodeList, "merge");
      appendMergeMirrorTreesToSVG(graphsvg, tree, mergeList[0], angle,
        isCountView, clusterName, diff);
    }
    angle += diff;
  }

  angle = 360 / (2 * divisions);
  if (divisions == 1) {
    angle = 90;
  }
  for (index = 0; index < treeList.length; index++) {
    var clusterNodeList = treeList[index];
    if (getNumOfNodes(clusterNodeList, "mirror") != 0) {
      var currentCluster = clusterNodeList[0].cluster;
      var mirrorList = getTierList(clusterNodeList, "mirror");
      appendMergeMirrorTreesToSVG(graphsvg, tree, mirrorList[0], angle,
        isCountView, clusterName, diff);
    }
    angle += diff;
  }

  appendArrowMarkersToLinks(graphsvg);
  loadDefaultView(isCountView);
  addLegendBox(graphsvg);
  addSummaryBox(isCountView, treeList);
}

function drawGraph(result, cluster, stream, start, end, selectedTab, publisher,
  agent, vip, collector, hdfs, local, merge, mirror, percentileFrSla,
  percentageFrLoss, percentageFrWarn, lWThresholdDiff, isDevMode) {
  publisherSla = publisher;
  agentSla = agent;
  vipSla = vip;
  collectorSla = collector;
  hdfsSla = hdfs;
  localSla = local;
  mergeSla = merge;
  mirrorSla = mirror;
  percentileForSla = percentileFrSla;
  percentageForLoss = percentageFrLoss;
  percentageForWarn = percentageFrWarn;
  lossWarnThresholdDiff = lWThresholdDiff;
  isGWTDevMode = isDevMode;
  document.getElementById("tabs")
    .style.display = "block";
  qStream = stream;
  qCluster = cluster;
  qstart = start;
  qend = end;
  jsonresponse = JSON.parse(result);
  while (fullTreeList.length > 0) {
    fullTreeList.pop();
  }
  clearHistory();
  buildNodeList();
  loadGraph(stream, cluster, selectedTab);
}

function clearSvgAndAddLoadSymbol() {
  clearPreviousGraph();
  var graphsvg = d3.select("#graphPanel")
    .append("svg:svg")
    .style("stroke",
      "gray")
    .attr("width", r * 5)
    .attr("height", r * 5)
    .style("background", "#D8EAF3")
    .attr("id", "graphsvg")
    .append("svg:g")
  var imgpath = "Visualization/bar-ajax-loader.gif";
  var imgs = graphsvg.append("svg:image")
    .attr("xlink:href", imgpath)
    .attr("x",
      r * 2.5 - 100)
    .attr("y", r * 2.5)
    .attr("width", "200")
    .attr("height", "40");
}

function highlightTab(selectedTabID) {
  if (parseInt(selectedTabID) == 1) {
    document.getElementById("count")
      .className = "active";
    document.getElementById("latency")
      .className = "";
  } else if (parseInt(selectedTabID) == 2) {
    document.getElementById("count")
      .className = "";
    document.getElementById("latency")
      .className = "active";
  }
}

function tabSelected(selectedTabID, stream, cluster) {
  if (stream == 'null' && cluster == 'null') {
    stream = qStream;
    cluster = qCluster;
  }
  clearSvgAndAddLoadSymbol();
  saveHistoryAndLoadGraph(stream, cluster, selectedTabID);
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

function checkAndClearList(treeList) {
  for (var k = 0; k < treeList.length; k++) {
    var clusterList = treeList[k];
    for (var i = 0; i < clusterList.length; i++) {
      var currentNode = clusterList[i];
      if (currentNode.allreceivedtopicstats == 0) {
        clusterList.splice(i, 1);
      }
    }
    if (clusterList.length == 0) {
      treeList.splice(k, 1);
    }
  }
}

function getTreeList(streamName, clusterName) {
  var treeList = [];
  if (clusterName.toLowerCase() != 'all') {
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
            if (clonedNode.tier.toLowerCase() == "merge" || clonedNode.tier.toLowerCase() == "mirror") {
              if (clonedNode.tier.toLowerCase() == "mirror") {                
                mirrorSourceList = clonedNode.source;
              }
              if (clonedNode.tier.toLowerCase() == "merge") {                
                clonedNode.source.forEach(function (s) {
                  if (!mergeSourceList.contains(s) && s != clusterName) {
                    mergeSourceList.push(s);
                  }
                });
              }
              clonedNode.streamSourceList.forEach(function(s) {
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
          if (l[0] != undefined && l[0].clusterName != clusterName && mirrorSourceList.contains(l[0].clusterName)) {
            var mirrorClusterList = [];
            for (var i = 0; i < l.length; i++) {
              var nodeInCluster = l[i];
              // Add only the merge node and not the full tree for the source cluster as the source of merge may/may not have the current cluster as source
              if (nodeInCluster.tier.toLowerCase() == "merge") {
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
                if (nodeInCluster.tier.toLowerCase() != "mirror" &&
                  nodeInCluster.tier.toLowerCase() != "merge") {
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
      if (streamName.toLowerCase() != 'all') {
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
      if (nodeList[i].tier.toLowerCase() == "local") {
        startindex = i;
        break;
      }
    }
  }
  return startindex;
}

function setTierLatencyValues(pLatency, aLatency, cLatency, hLatency, lLatency,
  meLatency, miLatency) {
  publisherLatency = pLatency;
  agentLatency = aLatency;
  collectorLatency = cLatency;
  hdfsLatency = hLatency;
  localLatency = lLatency;
  mergeLatency = meLatency;
  mirrorLatency = miLatency;
  loadLatencySummary();
}