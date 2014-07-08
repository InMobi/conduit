var nodeMap = new Object();
var overallNodeInfoMap = new Object();

function countEntriesInMap(obj) {
  if (obj.__count__ !== undefined) {
    return obj.__count__;
  }
  if (Object.keys) {
    return Object.keys(obj).length;
  }
  var c = 0, p;
  for (p in obj) {
    if (obj.hasOwnProperty(p)) {
      c += 1;
    }
  }
  return c;
}

function createLatencyMap(latencyList) {
  var latencyMap = new Object();
  latencyList.forEach(function (l) {
    latencyMap[l.percentile] = l.latency;
  });
  return latencyMap;
}

function isMergeMirror (node) {
  if (node.tier.equalsIgnoreCase("merge") || node.tier.equalsIgnoreCase("mirror")) {
    return true;
  }
  return false;
}

function isAgentCollector (node) {
  if (node.tier.equalsIgnoreCase("agent") || node.tier.equalsIgnoreCase("collector")) {
    return true;
  }
  return false;
}

function getHostObjectFromMap(topicMap, topic, tier, host) {
  var tierMap = topicMap[topic];
  if (tierMap == undefined) {
    tierMap = new Object();
    topicMap[topic] = tierMap;
  };
  var hostMap = tierMap[tier];
  if (hostMap == undefined) {
    hostMap = new Object();
    tierMap[tier] = hostMap;
  };
  var hostObj = hostMap[host];
  if (hostObj == undefined) {
    hostObj = new Object();
    hostMap[host] = hostObj;
  }
  return hostObj;
}

function getStatsObjFromList(hostObj, source) {
  if (hostObj.statsList == undefined) {
    hostObj.statsList = [];
  }
  var statsObj = undefined;
  if (source != undefined) {
    hostObj.statsList.forEach(function(s) {
      if (s.hostname.equalsIgnoreCase(source)) {
        statsObj = s;
      }
    });
  } else {
    statsObj = hostObj.statsList[0];
  }
  if (statsObj == undefined) {
    statsObj = new Object();
    statsObj.hostname = source;
    hostObj.statsList.push(statsObj);
  }
  return statsObj;
}

function getHostObjectFromOverallMap(cluster, tier, host) {
  var overallTierMap = overallNodeInfoMap[cluster];
  if (overallTierMap == undefined) {
    overallTierMap = new Object();
    overallNodeInfoMap[cluster] = overallTierMap;
  }
  var overallHostMap = overallTierMap[tier];
  if (overallHostMap == undefined) {
    overallHostMap = new Object();
    overallTierMap[tier] = overallHostMap;
  }
  var hostObject = overallHostMap[host];
  if (hostObject == undefined) {
    hostObject = new Object();
    overallHostMap[host] = hostObject;
  }
  return hostObject;
}

function buildNodeMap() {
  nodeMap = new Object();
  jsonresponse.nodes.forEach(function(n) {
    var curCluster = n.cluster;
    var curHost = n.name;
    var curTier = n.tier;

    var hostObject = getHostObjectFromOverallMap(curCluster, curTier, curHost);
    hostObject.received = parseInt(n.aggregatereceived, 10);
    hostObject.sent = parseInt(n.aggregatesent, 10);
    if (isMergeMirror(n)) {
      hostObject.source = [];
      n.source.forEach(function(cl) {
        cl.forEach(function(c) {
          hostObject.source.push(c);
        });
      });
    }
    hostObject.latencyMap = createLatencyMap(n.overallLatency);

    var topicMap = nodeMap[curCluster];
    if (topicMap == undefined) {
      topicMap = new Object();
      nodeMap[curCluster] = topicMap;
    }

    n.receivedtopicStatsList.forEach(function (t) {
      var hostObj = getHostObjectFromMap(topicMap, t.topic, curTier, curHost, t.hostname);
      var statsObj = getStatsObjFromList(hostObj, t.hostname);
      statsObj.received = t.messages;
    });

    n.senttopicStatsList.forEach(function (t) {
      var hostObj = getHostObjectFromMap(topicMap, t.topic, curTier, curHost, t.hostname);
      var statsObj = getStatsObjFromList(hostObj, t.hostname);
      statsObj.sent = t.messages;
    });

    n.topicLatency.forEach(function (t) {
      var hostObj = getHostObjectFromMap(topicMap, t.topic, curTier, curHost, t.hostname);
      hostObj.latencyMap = createLatencyMap(t.percentileLatencyList);
    });

    if (isMergeMirror(n)) {
      n.topicSource.forEach(function (s) {
      var hostObj = getHostObjectFromMap(topicMap, s.topic, curTier, curHost, undefined);
        if (hostObj != undefined) {
          hostObj.source = s.sourceList;
        }
      });
    }
  });
}