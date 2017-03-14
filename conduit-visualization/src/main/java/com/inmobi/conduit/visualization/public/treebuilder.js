var vipsAdded = 0;
var rootNodesMap = new Object();
var vipNodesMap = new Object();
var aggCollectorNodeMap = new Object();

function setTierList(topicMap, cluster, selectedTopic, tier, initialList) {
  if (topicMap != undefined) {
    if (!selectedTopic.equalsIgnoreCase('all')) {
      var tierMap = topicMap[selectedTopic];
      if (tierMap != undefined) {
        var hostMap = tierMap[tier];
        if (hostMap != undefined) {
          if (!initialList.contains(cluster)) {
            initialList.push(cluster);
          }
        }
      }
    } else {
      for (var t in topicMap) {
        var tierMap = topicMap[t];
        if (tierMap != undefined) {
          var hostMap = tierMap[tier];
          if (hostMap != undefined) {
            if (!initialList.contains(cluster)) {
              initialList.push(cluster);
            }
          }
        }
      }
    }
  }
}

function getClustersContainingTier(selectedCluster, selectedTopic, initialList, tier) {
  if (selectedCluster.equalsIgnoreCase('all')) {
    for (var s in nodeMap) {
      var topicMap = nodeMap[s];
      setTierList(topicMap, s, selectedTopic, tier, initialList);
    }
  } else {
    var topicMap = nodeMap[selectedCluster];
    setTierList(topicMap, selectedCluster, selectedTopic, tier, initialList);
  }
}

function setSourceList(m, selectedTopic, list, tier) {
  var topicMap = nodeMap[m];
  if (topicMap != undefined) {
    if (!selectedTopic.equalsIgnoreCase('all')) {
      var tierMap = topicMap[selectedTopic];
      if (tierMap != undefined) {
        var hostMap = tierMap[tier];
        if (hostMap != undefined) {
          if (tier.equalsIgnoreCase('local')) {
            if (!list.contains(m)) {
              list.push(m);
            }
          } else {
            for (var h in hostMap) {
              var nodeInfo = hostMap[h];
              nodeInfo.source.forEach(function(s) {
                var nodeInfo = hostMap[h];
                if (!list.contains(s)) {
                  list.push(s);
                }
              });
            }
          }
        }
      }
    } else {
      var tierMap = overallNodeInfoMap[m];
      if (tierMap != undefined) {
        var hostMap = tierMap[tier];
        if (hostMap != undefined) {
          if (tier.equalsIgnoreCase('local')) {
            if (!list.contains(m)) {
              list.push(m);
            }
          } else {
            for (var h in hostMap) {
              var nodeInfo = hostMap[h];
              nodeInfo.source.forEach(function(s) {
                var nodeInfo = hostMap[h];
                if (!list.contains(s)) {
                  list.push(s);
                }
              });
            }
          }
        }
      }
    }
  }
}

function setPerTopicStats(node, hostObj, topic) {
  node.topicLatencyMap = new Object();
  var topicLatency = new Object();
  for (var l in hostObj.latencyMap) {
    topicLatency[l] = hostObj.latencyMap[l];
  }
  node.topicLatencyMap[topic] = topicLatency;
  node.topicReceivedMap = new Object();
  if (node.tier.equalsIgnoreCase('hdfs')) {
    var hostReceivedMap = new Object();
    for (var i = 0; i < hostObj.statsList.length; i++) {
      hostReceivedMap[hostObj.statsList[i].hostname] = hostObj.statsList[i].received;
    }
    node.topicReceivedMap[topic] = hostReceivedMap;
  } else {
    if (hostObj.statsList.length > 0) {
      node.topicReceivedMap[topic] = hostObj.statsList[0].received;
    }
    if (isAgentCollector(node)) {
      node.topicSentMap = new Object();
      if (hostObj.statsList.length > 0) {
        node.topicSentMap[topic] = hostObj.statsList[0].sent;
      }
    }
  }
  if (isMergeMirror(node)) {
    node.topicSourceMap = new Object();
    var sList = [];
    hostObj.source.forEach(function(s) {
      sList.push(s);
    });
    node.topicSourceMap[topic] = sList;
  }
}

function createOrUpdateNode(node, host, hostObj, topic, cluster, tier, selectedCluster, selectedTopic, mergeMirrorStreams) {
  if (node == undefined) {
    if (selectedTopic.equalsIgnoreCase('all')) {
      if (selectedCluster.equalsIgnoreCase('all') || (!selectedCluster.equalsIgnoreCase('all') && (cluster.equalsIgnoreCase(selectedCluster) || (!cluster.equalsIgnoreCase(selectedCluster) && mergeMirrorStreams.contains(topic))))) {
        var overallNode = overallNodeInfoMap[cluster][tier][host];
        var received = 0, sent = 0;
        if (selectedCluster.equalsIgnoreCase('all') || cluster.equalsIgnoreCase(selectedCluster)) {
          received = parseInt(overallNode.received, 10);
          sent = parseInt(overallNode.sent, 10);
        } else {
          received = hostObj.statsList[0].received;
        }
        node = new Node(host, cluster, tier, received, sent);
        node.latencyMap = new Object();
        for (var l in overallNode.latencyMap) {
          node.latencyMap[l] = overallNode.latencyMap[l];
        }
        if (overallNode.source != undefined) {
          node.source = [];
          overallNode.source.forEach(function(s) {
            node.source.push(s);
          });
        }
        setPerTopicStats(node, hostObj, topic);
      }
    } else {
      node = new Node(host, cluster, tier, parseInt(hostObj.statsList[0].received, 10), parseInt(hostObj.statsList[0].sent, 10));
      node.latencyMap = new Object();
      for (var l in hostObj.latencyMap) {
        node.latencyMap[l] = hostObj.latencyMap[l];
      }
      if (hostObj.source != undefined) {
        node.source = [];
        hostObj.source.forEach(function(s) {
          node.source.push(s);
        });
      }
      setPerTopicStats(node, hostObj, topic);
    }
  } else {
    if (selectedCluster.equalsIgnoreCase('all') || (!selectedCluster.equalsIgnoreCase('all') && (cluster.equalsIgnoreCase(selectedCluster) || (!cluster.equalsIgnoreCase(selectedCluster) && mergeMirrorStreams.contains(topic))))) {
      if (!cluster.equalsIgnoreCase(selectedCluster) && mergeMirrorStreams.contains(topic)) {
        node.aggregatemessagesreceived += hostObj.statsList[0].received;
      }
      var topicLatency = new Object();
      for (var l in hostObj.latencyMap) {
        topicLatency[l] = hostObj.latencyMap[l];
      }
      node.topicLatencyMap[topic] = topicLatency;
      if (tier.equalsIgnoreCase('hdfs')) {
        var hostReceivedMap = new Object();
        for (var i = 0; i < hostObj.statsList.length; i++) {
          hostReceivedMap[hostObj.statsList[i].hostname] = hostObj.statsList[i].received;
        }
        node.topicReceivedMap[topic] = hostReceivedMap;
      } else {
        node.topicReceivedMap[topic] = hostObj.statsList[0].received;
      }
      if (isAgentCollector(node)) {
        node.topicSentMap[topic] = hostObj.statsList[0].sent;
      }
      if (isMergeMirror(node)) {
        var sList = [];
        hostObj.source.forEach(function(s) {
          sList.push(s);
        });
        node.topicSourceMap[topic] = sList;
      }
    }
  }
  return node;
}

function createRootNode(hostMap, topic, cluster, tier, selectedCluster, selectedTopic, mergeMirrorStreams) {
  var hostObj, host;
  for (host in hostMap) {
    hostObj = hostMap[host];
  }
  var clusterMap = rootNodesMap[cluster];
  if (clusterMap == undefined) {
    clusterMap = new Object();
    rootNodesMap[cluster] = clusterMap;
  }
  var rootNode = clusterMap[tier];
  rootNode = createOrUpdateNode(rootNode, host, hostObj, topic, cluster, tier, selectedCluster, selectedTopic, mergeMirrorStreams)
  clusterMap[tier] = rootNode;
  return rootNode;
}

function getChildTier(parentTier) {
  var childTier;
  switch(parentTier.toLowerCase()) {
    case 'mirror':
        childTier = 'MERGE';
        break;
    case 'merge':
        childTier = 'LOCAL';
        break;
    case 'local':
        childTier = 'hdfs';
        break;
    case 'hdfs':
        childTier = 'collector';
        break;
    case 'collector':
        childTier = 'VIP';
        break;
    case 'vip':
        childTier = 'agent';
        break;
    case 'agent':
        childTier = 'publisher';
        break;
  }
  return childTier;
}

function getNodeFromList(list, h) {
  //get object from list which has h as name
  for (var i=0; i < list.length; i++) {
    var l = list[i];
    if (l.name.equalsIgnoreCase(h)) {
      return l;
    }
  }
}

function getChildrenNodes(children, topic, tierMap, parentTier, parentHost, cluster, selectedCluster, selectedTopic, mergeMirrorStreams) {
  var childTier = getChildTier(parentTier);
  var chilldHostMap = tierMap[childTier];
  if (chilldHostMap != undefined) {
    for (var h in chilldHostMap) {
      if (!parentTier.equalsIgnoreCase('agent') || (parentTier.equalsIgnoreCase('agent') && h.equalsIgnoreCase(parentHost))) {
        var hostInfo = chilldHostMap[h];
        var childNode = getNodeFromList(children, h);
        var toBeAdded = false;
        if (childNode == undefined) {
          toBeAdded = true;
        };
        childNode = createOrUpdateNode(childNode, h, hostInfo, topic, cluster, childTier, selectedCluster, selectedTopic, mergeMirrorStreams);
        if (childNode != undefined) {
          if (toBeAdded) {
            children.push(childNode);
            if (childTier.equalsIgnoreCase('vip')) {
              vipsAdded++;
            }
          }
          if (!childTier.equalsIgnoreCase('vip') || (childTier.equalsIgnoreCase('vip') && (vipsAdded == 1 || childNode.children.length > 0))) {
            getChildrenNodes(childNode.children, topic, tierMap, childTier, childNode.name, childNode.clusterName, selectedCluster, selectedTopic, mergeMirrorStreams);
            if (childTier.equalsIgnoreCase('vip')) {
              vipNodesMap[cluster] = childNode;
            }
          }
        }
      }
    }
  }
}

function createRootNodeAndPopulateChildren(cluster, topic, tier, selectedCluster, selectedTopic, mergeMirrorStreams) {
  var topicMap = nodeMap[cluster];
  if (topicMap != undefined) {
    var tierMap = topicMap[topic];
    if (tierMap != undefined) {
      if (tierMap[tier] != undefined) {
        var rootNode = createRootNode(tierMap[tier], topic, cluster, tier, selectedCluster, selectedTopic, mergeMirrorStreams);
        if (tier.equalsIgnoreCase('local') && rootNode != undefined) {
          vipsAdded = 0;
          getChildrenNodes(rootNode.children, topic, tierMap, rootNode.tier, rootNode.name, rootNode.clusterName, selectedCluster, selectedTopic, mergeMirrorStreams);
        }
        var rootNodesClusterMap = rootNodesMap[cluster];
        if (rootNodesClusterMap == undefined) {
          rootNodesClusterMap = new Object();
          rootNodesMap[cluster] = rootNodesClusterMap;
        }
        if (rootNode != undefined) {
          rootNodesClusterMap[tier] = rootNode;
        }
      }
    }
  }
}

function getMergeMirrorStreams(cluster, topic) {
  var streamslist = [];
  var topicMap = nodeMap[cluster];
  if (topicMap != undefined) {
    if (topic.equalsIgnoreCase('all')) {
      for (var t in topicMap) {
        var tierMap = topicMap[t];
        if (tierMap['MIRROR'] != undefined || tierMap['MERGE'] != undefined) {
          if (!streamslist.contains(t)) {
            streamslist.push(t);
          }
        }
      }
    } else {
      var tierMap = topicMap[topic];
      if (tierMap['MIRROR'] != undefined || tierMap['MERGE'] != undefined) {
        if (!streamslist.contains(t)) {
          streamslist.push(topic);
        }
      }
    }
  }
  return streamslist;
}

function createDummyChildNodesForMergeMirror(tier) {
  for (var s in rootNodesMap) {
    var rootNode = rootNodesMap[s][tier];
    if (rootNode != undefined) {
      var childTier = getChildTier(tier);
      rootNode.source.forEach(function(sCluster) {
        var sourceTierMap = rootNodesMap[sCluster];
        if (sourceTierMap != undefined) {
          var sourceNode = sourceTierMap[childTier];
          if (sourceNode != undefined) {
            var childNode = new Node(sourceNode.name, sourceNode.clusterName, sourceNode.tier, sourceNode.aggregatemessagesreceived, sourceNode.aggregatemessagesent);
            childNode.latencyMap = sourceNode.latencyMap;
            childNode.topicReceivedMap = sourceNode.topicReceivedMap;
            childNode.topicLatencyMap = sourceNode.topicLatencyMap;
            if (isMergeMirror(childNode)) {
              childNode.source = sourceNode.source;
              childNode.topicSourceMap = sourceNode.topicSourceMap;
            }
            rootNode.children.push(childNode);
          }
        }
      });
    }
  }
}

function createRootNodeForCluster(cluster, selectedTopic, tier, selectedCluster, selectedTopic, mergeMirrorStreams) {
  if (selectedTopic.equalsIgnoreCase('all')) {
    var topicMap = nodeMap[cluster];
    if (topicMap != undefined) {
      for (var t in topicMap) {
        createRootNodeAndPopulateChildren(cluster, t, tier, selectedCluster, selectedTopic, mergeMirrorStreams);
      }
    }
  } else {
    createRootNodeAndPopulateChildren(cluster, selectedTopic, tier, selectedCluster, selectedTopic, mergeMirrorStreams);
  }
}

function aggregateCollectorStats(cluster, received, sent) {
  var aggCollectorNode = aggCollectorNodeMap[cluster];
  if (aggCollectorNode == undefined) {
    aggCollectorNode = {
          received: 0,
          sent: 0
      };
    aggCollectorNodeMap[cluster] = aggCollectorNode;
  }
  aggCollectorNode.received += received;
  aggCollectorNode.sent += sent;
}

function setAggCollectorNodeMap() {
  for (var c in rootNodesMap) {
    var localNode = rootNodesMap[c]['LOCAL'];
    if (localNode != undefined) {
      localNode.children.forEach(function (hdfsNode) {
        hdfsNode.children.forEach(function (collectorNode) {
          aggregateCollectorStats(c, collectorNode.aggregatemessagesreceived, collectorNode.aggregatemessagesent);
        });
      });
    }
  }
}

function setRootNodesMap(selectedTopic, selectedCluster) {
  rootNodesMap = new Object();
  aggCollectorNodeMap = new Object();
  vipNodesMap = new Object();

  var mirrorsList = [];
  getClustersContainingTier(selectedCluster, selectedTopic, mirrorsList, 'MIRROR');

  var mergeList = [];
  mirrorsList.forEach(function(m) {
    setSourceList(m, selectedTopic, mergeList, 'MIRROR');
  });
  getClustersContainingTier(selectedCluster, selectedTopic, mergeList, 'MERGE');

  var localList = [];
  mergeList.forEach(function(m) {
    setSourceList(m, selectedTopic, localList, 'MERGE');
  });
  getClustersContainingTier(selectedCluster, selectedTopic, localList, 'LOCAL');

  var mergeMirrorStreams = [];
  if (!selectedCluster.equalsIgnoreCase('all')) {
    mergeMirrorStreams = getMergeMirrorStreams(selectedCluster, selectedTopic);
  }

  mirrorsList.forEach(function(m) {
    //m is name of mirror cluster
    createRootNodeForCluster(m, selectedTopic, 'MIRROR', selectedCluster, selectedTopic, mergeMirrorStreams);
  });
  mergeList.forEach(function(m) {
    //m is name of merge cluster
    createRootNodeForCluster(m, selectedTopic, 'MERGE', selectedCluster, selectedTopic, mergeMirrorStreams);
  });
  localList.forEach(function(l) {
    //l is name of local cluster
    createRootNodeForCluster(l, selectedTopic, 'LOCAL', selectedCluster, selectedTopic, mergeMirrorStreams);
  });

  createDummyChildNodesForMergeMirror('MERGE');
  createDummyChildNodesForMergeMirror('MIRROR');
  setAggCollectorNodeMap();
}