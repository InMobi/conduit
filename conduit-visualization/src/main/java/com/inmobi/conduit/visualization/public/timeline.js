var timelinejson;
var dataPointMap;
var timeinterval = 60 * 60000; //in milliseconds
var minDate = 0,
  maxDate = 0,
  minCount = 0,
  maxCount = 0,
  maxLatency = 0;
var tierbuttonList = ["Mirror", "Merge", "Local", "HDFS", "Collector", "Agent",
  "Publisher", "All"
];
var svg;
var dateFormat = d3.time.format.utc("%b %d,%Y %I:%M %p");
var dateHighlighted;
var uniqId = 1;

function TimeLineTopicStats(topic) {
  this.topic = topic;
  this.received = 0;
  this.sent = 0;
  this.topicLatency = [];
}

function ClusterStatsTimeLine(cluster) {
  this.cluster = cluster;
  this.topicStatsList = [];
  this.clusterLatency = [];
}

function DataPoint(time, tier) {
  this.time = time;
  this.tier = tier;
  this.aggreceived = 0;
  this.aggsent = 0;
  this.clusterStatsList = [];
  this.overallLatency = [];
}

function buildDataPointMap(tier, stream, cluster) {
  /*
    console.log("building data point list for tier:"+tier+" stream:"+stream+" and cluster:"+cluster);*/
  dataPointMap = new Object();
  if (timelinejson != undefined) {
    for (var i = 0; i < timelinejson.datapoints.length; i++) {
      var tierList = timelinejson.datapoints[i];
      var currentTier = tierList.tier;
      if (tier.toLowerCase() == 'all' || (tier.toLowerCase() != 'all' &&
        currentTier.toLowerCase() == tier.toLowerCase())) {
        var tierPointMap = new Object();
        for (var j = 0; j < tierList.tierWisePointList.length; j++) {
          var p = tierList.tierWisePointList[j];
          var datapoint = new DataPoint(parseInt(p.time, 10), currentTier);
          if (minDate == 0 || parseInt(p.time, 10) < minDate) {
            minDate = parseInt(p.time, 10);
          }
          if (maxDate == 0 || parseInt(p.time, 10) > maxDate) {
            maxDate = parseInt(p.time, 10);
          }
          var actualReceived = 0;
          var actualSent = 0;
          var clusterList = p.clusterCountList;
          for (var k = 0; k < clusterList.length; k++) {
            var clusterEntry = clusterList[k];
            var currentCluster = clusterEntry.cluster;
            if (cluster.toLowerCase() == 'all' || (cluster.toLowerCase() !=
              'all' && cluster == currentCluster)) {
              var newClusterStats = new ClusterStatsTimeLine(currentCluster);
              if (!isCountView) {
                clusterEntry.clusterLatency.forEach(function (pl) {
                  newClusterStats.clusterLatency.push(new PercentileLatency(
                    parseFloat(pl.percentile), parseInt(pl.latency, 10)));
                });
              }
              for (var l = 0; l < clusterEntry.topicStats.length; l++) {
                var topicEntry = clusterEntry.topicStats[l];
                var currentTopic = topicEntry.topic;
                if (stream.toLowerCase() == 'all' || (stream.toLowerCase() !=
                  'all' && stream == currentTopic)) {
                  var newTopicStats = new TimeLineTopicStats(currentTopic);
                  if (isCountView) {
                    newTopicStats.received = parseInt(topicEntry.received, 10);
                    newTopicStats.sent = parseInt(topicEntry.sent, 10);
                    actualReceived += parseInt(topicEntry.received, 10);
                    actualSent += parseInt(topicEntry.sent, 10);
                  } else {
                    topicEntry.topicLatency.forEach(function (pl) {
                      newTopicStats.topicLatency.push(new PercentileLatency(
                        parseFloat(pl.percentile), parseInt(pl.latency, 10)
                      ));
                    });
                  }
                  newClusterStats.topicStatsList.push(newTopicStats);
                  if (stream.toLowerCase() != 'all' && stream == currentTopic) {
                    if (cluster.toLowerCase() != 'all' && cluster ==
                      currentCluster && !isCountView) {
                      topicEntry.topicLatency.forEach(function (pl) {
                      	if (parseFloat(pl.percentile) == percentileForSla && parseInt(pl.latency, 10) > maxLatency) {
                      		maxLatency = parseInt(pl.latency, 10);
                      	}
                        datapoint.overallLatency.push(new PercentileLatency(
                          parseFloat(pl.percentile), parseInt(pl.latency,
                            10)));
                      });
                    }
                    break;
                  }
                }
              }
              datapoint.clusterStatsList.push(newClusterStats);
              if (cluster.toLowerCase() != 'all' && cluster == currentCluster) {
                if (stream.toLowerCase() == 'all' && !isCountView) {
                  clusterEntry.clusterLatency.forEach(function (pl) {
                  	if (parseFloat(pl.percentile) == percentileForSla && parseInt(pl.latency, 10) > maxLatency) {
											maxLatency = parseInt(pl.latency, 10);
                    }
                    datapoint.overallLatency.push(new PercentileLatency(
                      parseFloat(pl.percentile), parseInt(pl.latency, 10)));
                  });
                }
                break;
              }
            }
          }
          if (datapoint.clusterStatsList.length == 0) {
            continue;
          }
          if (stream.toLowerCase() == 'all' && cluster.toLowerCase() == 'all' && !
            isCountView) {
            p.overallLatency.forEach(function (pl) {
            	if (parseFloat(pl.percentile) == percentileForSla && parseInt(pl.latency, 10) > maxLatency) {
              	maxLatency = parseInt(pl.latency, 10);
              }
              datapoint.overallLatency.push(new PercentileLatency(parseFloat(
                pl.percentile), parseInt(pl.latency, 10)));
            });
          }
          if (isCountView) {
            datapoint.aggreceived = actualReceived;
            datapoint.aggsent = actualSent;
            if (minCount == 0 || actualReceived < minCount) {
              minCount = actualReceived;
            }
            if (maxCount == 0 || actualReceived > maxCount) {
              maxCount = actualReceived;
            }
          }
          tierPointMap[parseInt(p.time, 10)] = datapoint;
        }
        if (isEmpty(tierPointMap)) {
          continue;
        }
        dataPointMap[currentTier] = tierPointMap;
        if (tier.toLowerCase() != 'all' && currentTier == tier) {
          break;
        }
      }
    }
  }
}
var margin = {
  top: 20,
  right: 20,
  bottom: 50,
  left: 100
},
  trendwidth = 1250 - margin.left - margin.right,
  trendheight = 500 - margin.top - margin.bottom;
//time-scale to scale time within range [0, trendwidth] i.e. over x-axis
var x = d3.time.scale.utc()
  .range([0, trendwidth]);
//linear scale to scale count within range [trendheight, 0] i.e. over y-axis
var y = d3.scale.linear()
  .range([trendheight, 0]);
var xAxis = d3.svg.axis()
  .scale(x)
  .orient("bottom")
  .tickSize(-trendheight, 0, 0)
  .tickPadding(6);
var yAxis = d3.svg.axis()
  .scale(y)
  .orient("left")
  .ticks(20)
  .tickSize(-trendwidth, 0, 0)
  .tickPadding(6);
var countLine = d3.svg.line()
  .x(function (d) {
    return x(new Date(parseInt(d.time, 10)));
  })
  .y(function (d) {
    return y(parseInt(d.datapoint.aggreceived, 10));
  });
var latencyLine = d3.svg.line()
  .x(function (d) {
    return x(new Date(parseInt(d.time, 10)));
  })
  .y(function (d) {
    for (var i = 0; i < d.datapoint.overallLatency.length; i++) {
      if (parseFloat(d.datapoint.overallLatency[i].percentile) ==
        percentileForSla) {
        return y(parseInt(d.datapoint.overallLatency[i].latency, 10));
      }
    }
  });
/*var brush = d3.svg.brush()
    .x(x)
    .on("brush", brushed);

function brushed() {
  x.domain(brush.empty() ? x.domain() : brush.extent());
  svg.selectAll("path").attr("d", line);
  svg.select(".x.grid").call(xAxis);
  svg.select(".y.grid").call(yAxis);
}*/
var callCount = 0;

function renderTimeLineForTierStreamCluster(tier, stream, cluster) {
  minDate = 0, maxDate = 0, minCount = 0, maxCount = 0;
  clearTrendSVG();
  buildDataPointMap(tier, stream, cluster);
  svg = d3.select("#timelinePanel").append("svg")
    .attr("width", trendwidth + margin.left + margin.right)
    .attr("height", trendheight + margin.top + margin.bottom)
    .attr("id", "trendsvg")
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
  x.domain(d3.extent([new Date(minDate), new Date(maxDate)]));
  if (isCountView) {
    y.domain(d3.extent([minCount, maxCount]));
  } else {
    y.domain(d3.extent([0, maxLatency]));
  }
  svg.append("g")
    .attr("class", "x grid")
    .attr("transform", "translate(0," + trendheight + ")")
    .call(xAxis)
    .style("z-index", "1");
  svg.append("g")
    .attr("class", "y grid")
    .call(yAxis)
    .style("z-index", "1");
  /*svg.append("g")
        .attr("class", "x brush")
        .call(brush)
      .selectAll("rect")
        .attr("y", -6)
        .attr("height", trendheight + 7);*/
  for (var t in dataPointMap) {
    var color = tierColorMap[t.toLowerCase()];
    var dataArray = [];
    for (var obj in dataPointMap[t]) {
      var arrayObj = {};
      arrayObj.time = obj;
      arrayObj.datapoint = dataPointMap[t][obj];
      dataArray.push(arrayObj);
    }
    if (isCountView) {
      svg.append("path")
        .datum(dataArray)
        .attr("d", countLine)
        .attr("id", t)
        .attr("stroke", color)
        .attr("fill", "none")
        .attr("stroke-width", 2)
        .on("click", function () {
          event.stopPropagation();
        })
        .on("mouseover", function (d) {
          d3.select(this).style("cursor", "pointer");
          highlighPathMouseOver(d3.event.pageX, d3.select(this).attr("id"));
        });
    } else {
      svg.append("path")
        .datum(dataArray)
        .attr("d", latencyLine)
        .attr("id", t)
        .attr("fill", "none")
        .attr("stroke", color)
        .attr("stroke-width", 2)
        .on("click", function () {
          event.stopPropagation();
        })
        .on("mouseover", function (d) {
          /*
          d3.select(this).style("cursor", "pointer");*/
          highlighPathMouseOver(d3.event.pageX, d3.select(this).attr("id"));
        });
    }
  }
  svg.on("mouseover", function () {
    mouseOverOnGraph(d3.event.pageX);
  });
}

function highlighPathMouseOver(xcoord, tier) {
  svg.selectAll(".smallindicator").data([]).exit().remove();
  svg.selectAll(".bigindicator").data([]).exit().remove();
  popupDiv.transition()
    .duration(200)
    .style("opacity", 0);
  var date = getNearestDate(xcoord);
  var correpPoint = dataPointMap[tier][date];
  if (correpPoint != undefined) {
    var color = tierColorMap[tier.toLowerCase()];
    var finalVal;
    if (isCountView) {
      finalVal = parseInt(correpPoint.aggreceived, 10);
    } else {
      for (var i = 0; i < correpPoint.overallLatency.length; i++) {
        if (parseFloat(correpPoint.overallLatency[i].percentile) ==
          percentileForSla) {
          finalVal = parseInt(correpPoint.overallLatency[i].latency, 10);
        }
      }
    }
    var data = [];
    data.push(correpPoint);
    svg.selectAll("circle.smallindicator")
      .data(data)
      .enter()
      .append("circle")
      .attr("class", "smallindicator")
      .attr("cx", x(new Date(date)))
      .attr("cy", y(finalVal))
      .attr("r", 4)
      .style("fill", color)
      .style("cursor", "pointer")
      .on("click", function (d) {
        showPointDetails(d, d3.event.pageX, d3.event.pageY);
      });
    svg.selectAll("circle.bigindicator")
      .data(data)
      .enter()
      .append("circle")
      .attr("class", "bigindicator")
      .attr("cx", x(new Date(date)))
      .attr("cy", y(finalVal))
      .attr("r", 6)
      .style("stroke", color)
      .style("fill", "none")
      .style("cursor", "pointer")
      .on("click", function (d) {
        showPointDetails(d, d3.event.pageX, d3.event.pageY);
      });
  }
}

function collapse(id, text, isButton, tier, stream, cluster) {
  var cellText;
  if (isButton) {
    cellText = "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
      stream + "','" + cluster + "','" + tier +
      "')\" class=\"popuptransparentButton\">" + text + "</button>";
  } else {
    cellText = text;
  }
  document.getElementById(id + "Cell").innerHTML =
    "<img src=\"Visualization/math-add-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"expand('" +
    id + "','" + text + "'," + isButton + ",'" + tier + "','" + stream + "','" +
    cluster + "')\"/> " + cellText;
  var elements = document.getElementsByClassName(id);
  for (var i = 0; i < elements.length; i++) {
    var el = elements[i];
    if (el.classList.contains("collapsibleRow")) {
      var strs = el.id.split("^");
      if (strs.length == 2) {
        collapse(strs[0], strs[1], false);
      } else {
        collapse(strs[0], strs[1], true, strs[2], strs[3], strs[4]);
      }
    }
    if (el.classList.contains("expanded")) {
      el.classList.remove("expanded");
      el.classList.add("collapsed");
    }
  }
}

function expand(id, text, isButton, tier, stream, cluster) {
  var cellText;
  if (isButton) {
    cellText = "<button type=\"button\" onclick=\"saveHistoryAndReload('" +
      stream + "','" + cluster + "','" + tier +
      "')\" class=\"popuptransparentButton\">" + text + "</button>";
  } else {
    cellText = text;
  }
  document.getElementById(id + "Cell").innerHTML =
    "<img src=\"Visualization/math-minus-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"collapse('" +
    id + "','" + text + "'," + isButton + ",'" + tier + "','" + stream + "','" +
    cluster + "')\"/> " + cellText;
  var elements = document.getElementsByClassName(id);
  for (var i = 0; i < elements.length; i++) {
    var el = elements[i];
    if (el.classList.contains("collapsed")) {
      el.classList.remove("collapsed");
      el.classList.add("expanded");
    }
  }
}

function showPointDetails(d, xcoord, ycoord) {
  popupDiv.html("");
  var timelinepaneltopOffset = document.getElementById("timelinePanel").offsetTop;
  var finalycoord;
  if (ycoord - timelinepaneltopOffset > 250) {
    finalycoord = timelinepaneltopOffset + 250;
  } else {
    finalycoord = ycoord;
  }
  if (xcoord > 1000) {
    popupDiv.transition()
      .duration(0)
      .style("height", "300px")
      .style("width", "250px")
      .style("top", (finalycoord - 10) + "px")
      .style("left", (xcoord - 260) + "px")
      .style("overflow", "auto")
      .style("opacity", 0.9);
  } else {
    popupDiv.transition()
      .duration(0)
      .style("height", "300px")
      .style("width", "250px")
      .style("top", (finalycoord - 10) + "px")
      .style("left", (xcoord + 10) + "px")
      .style("overflow", "auto")
      .style("opacity", 0.9);
  }
  var table = document.createElement('table');
  var currentRow = 0;
  var num;
  var r, c;
  r = table.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Time:";
  c = r.insertCell(1);
  c.innerHTML = dateFormat(new Date(d.time));
  r = table.insertRow(currentRow++);
  c = r.insertCell(0);
  c.innerHTML = "Tier:";
  c = r.insertCell(1);
  c.innerHTML =
    "<button type=\"button\" onclick=\"saveHistoryAndReload('"+currentStream+"', '"+currentCluster+"', '" +
    d.tier.toLowerCase() + "')\" class=\"popuptransparentButton\">" + d.tier +
    "</button>";
  if (isCountView) {
    r = table.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "Agg Received:";
    c = r.insertCell(1);
    c.innerHTML = d.aggreceived;
    r = table.insertRow(currentRow++);
    c = r.insertCell(0);
    c.innerHTML = "Agg Sent:";
    c = r.insertCell(1);
    c.innerHTML = d.aggsent;
  } else {
    r = table.insertRow(currentRow++);
    r.className = "collapsibleRow";
    r.id = uniqId + "^Overall Latency";
    r.data = "Overall Latency";
    c = r.insertCell(0);
    c.id = uniqId + "Cell";
    c.innerHTML =
      "<img src=\"Visualization/math-add-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"expand('" +
      uniqId + "','Overall Latency', false)\"/> Overall Latency";
    d.overallLatency.forEach(function (pl) {
      r = table.insertRow(currentRow++);
      r.className = "collapsed " + uniqId;
      c = r.insertCell(0);
      c.innerHTML = "";
      c = r.insertCell(1);
      c.innerHTML = pl.percentile + " %";
      c = r.insertCell(2);
      c.innerHTML = pl.latency + " minutes";
    });
    uniqId++;
  }
  r = table.insertRow(currentRow++);
  r.className = "collapsibleRow";
  r.id = uniqId + "^Cluster Stats";
  c = r.insertCell(0);
  c.id = uniqId + "Cell";
  c.innerHTML =
    "<img src=\"Visualization/math-add-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"expand('" +
    uniqId + "','Cluster Stats', false)\"/> Cluster Stats";
  var tempId = uniqId + 1;
  d.clusterStatsList.forEach(function (clusterStats) {
    r = table.insertRow(currentRow++);
    r.className = "collapsibleRow collapsed " + uniqId;
    r.id = tempId + "^" + clusterStats.cluster + "^all^all^" + clusterStats.cluster;
    c = r.insertCell(0);
    c.innerHTML = "";
    c = r.insertCell(1);
    c.id = tempId + "Cell";
    c.style.display = "inline-flex";
    c.innerHTML =
      "<img src=\"Visualization/math-add-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"expand('" +
      tempId + "','" + clusterStats.cluster + "', true, 'all', 'all', '" +
      clusterStats.cluster +
      "')\"/><button type=\"button\" onclick=\"saveHistoryAndReload('all', '" +
      clusterStats.cluster + "', 'all')\" class=\"popuptransparentButton\">" +
      clusterStats.cluster + "</button>";
    var temptempId = 0;
    if (!isCountView) {
      temptempId = tempId + 1;
      r = table.insertRow(currentRow++);
      r.className = "collapsibleRow collapsed " + tempId;
      r.id = temptempId + "^Cluster Latency";
      c = r.insertCell(0);
      c.innerHTML = "";
      c = r.insertCell(1);
      c.innerHTML = "";
      c = r.insertCell(2);
      c.id = temptempId + "Cell";
      c.innerHTML =
        "<img src=\"Visualization/math-add-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"expand('" +
        temptempId + "','Cluster Latency', false)\"/> Cluster Latency";
      clusterStats.clusterLatency.forEach(function (pl) {
        r = table.insertRow(currentRow++);
        r.className = "collapsed " + temptempId;
        c = r.insertCell(0);
        c.innerHTML = "";
        c = r.insertCell(1);
        c.innerHTML = "";
        c = r.insertCell(2);
        c.innerHTML = "";
        c = r.insertCell(3);
        c.innerHTML = pl.percentile + " %";
        c = r.insertCell(4);
        c.innerHTML = pl.latency + " minutes";
      });
    }
    r = table.insertRow(currentRow++);
    r.className = "collapsibleRow collapsed " + tempId;
    if (!isCountView) {
      tempId = temptempId + 1;
    } else {
      tempId++;
    }
    r.id = tempId + "^Topic Stats";
    c = r.insertCell(0);
    c.innerHTML = "";
    c = r.insertCell(1);
    c.innerHTML = "";
    c = r.insertCell(2);
    c.id = tempId + "Cell";
    c.innerHTML =
      "<img src=\"Visualization/math-add-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"expand('" +
      tempId + "','Topic Stats', false)\"/> Topic Stats";
    temptempId = tempId + 1;
    clusterStats.topicStatsList.forEach(function (t) {
      r = table.insertRow(currentRow++);
      r.className = "collapsibleRow collapsed " + tempId;
      r.id = temptempId + "^" + t.topic + "^all^" + t.topic + "^" +
        clusterStats.cluster;
      c = r.insertCell(0);
      c.innerHTML = "";
      c = r.insertCell(1);
      c.innerHTML = "";
      c = r.insertCell(2);
      c.innerHTML = "";
      c = r.insertCell(3);
      c.id = temptempId + "Cell";
      c.style.display = "inline-flex";
      c.innerHTML =
        "<img src=\"Visualization/math-add-icon.png\" height=\"12.5\" width=\"12.5\" onclick=\"expand('" +
        temptempId + "','" + t.topic + "', true, 'all', '" + t.topic +
        "', '" + clusterStats.cluster +
        "')\"/>  <button type=\"button\" onclick=\"saveHistoryAndReload('" +
        t.topic + "', '" + clusterStats.cluster +
        "', 'all')\" class=\"popuptransparentButton\">" + t.topic +
        "</button>";
      if (isCountView) {
        r = table.insertRow(currentRow++);
        r.className = "collapsed " + temptempId;
        c = r.insertCell(0);
        c.innerHTML = "";
        c = r.insertCell(1);
        c.innerHTML = "";
        c = r.insertCell(2);
        c.innerHTML = "";
        c = r.insertCell(3);
        c.innerHTML = "";
        c = r.insertCell(4);
        c.innerHTML = "Received";
        c = r.insertCell(5);
        c.innerHTML = t.received;
        r = table.insertRow(currentRow++);
        r.className = "collapsed " + temptempId;
        c = r.insertCell(0);
        c.innerHTML = "";
        c = r.insertCell(1);
        c.innerHTML = "";
        c = r.insertCell(2);
        c.innerHTML = "";
        c = r.insertCell(3);
        c.innerHTML = "";
        c = r.insertCell(4);
        c.innerHTML = "Sent";
        c = r.insertCell(5);
        c.innerHTML = t.sent;
      } else {
        t.topicLatency.forEach(function (l) {
          r = table.insertRow(currentRow++);
          r.className = "collapsed " + temptempId;
          c = r.insertCell(0);
          c.innerHTML = "";
          c = r.insertCell(1);
          c.innerHTML = "";
          c = r.insertCell(2);
          c.innerHTML = "";
          c = r.insertCell(3);
          c.innerHTML = "";
          c = r.insertCell(4);
          c.innerHTML = l.percentile + " %";
          c = r.insertCell(5);
          c.innerHTML = l.latency + " minutes";
        });
      }
      temptempId++;
    });
    tempId = temptempId + 1;
  });
  uniqId = tempId + 1;
  document.getElementById("popupDiv").appendChild(table);
}

function getNearestDate(xcoord) {
  var date = x.invert(xcoord - margin.left);
  if (date.getTime() < minDate || date.getTime() > maxDate) {
    return undefined;
  }
  var diff = date.getTime() % timeinterval;
  if (diff < timeinterval / 2) {
    date = date.getTime() - diff;
  } else {
    date = date.getTime() + (timeinterval - diff);
  }
  return date;
}

function mouseOverOnGraph(xcoord) {
  var date = getNearestDate(xcoord);
  if (date == undefined) {
    popupDiv.transition()
      .duration(200)
      .style("opacity", 0);
    return;
  }
  if (dateHighlighted != undefined && dateHighlighted == date) {
    return;
  }
  dateHighlighted = date;
  callCount++;
  svg.selectAll(".smallindicator").data([]).exit().remove();
  svg.selectAll(".bigindicator").data([]).exit().remove();
  popupDiv.transition()
    .duration(200)
    .style("height", "auto")
    .style("width", "auto")
    .style("opacity", 0.9)
    .style("top", (document.getElementById("timelinePanel").offsetTop + margin.top +
      125) + "px")
  if ((xcoord - margin.left) <= trendwidth / 2) {
    //align the popup div to the right
    popupDiv.style("left", (trendwidth / 2 + 300) + "px");
  } else {
    //align the popup div to the left
    popupDiv.style("left", (trendwidth / 2 - 300) + "px");
  }
  var finalVals = [];
  var data = [];
  for (var tier in dataPointMap) {
    var timePointMap = dataPointMap[tier];
    var correpPoint = timePointMap[date];
    if (correpPoint != undefined) {
      var finalVal;
      if (isCountView) {
        finalVal = parseInt(correpPoint.aggreceived, 10);
      } else {
        for (var i = 0; i < correpPoint.overallLatency.length; i++) {
          if (parseFloat(correpPoint.overallLatency[i].percentile) ==
            percentileForSla) {
            finalVal = parseInt(correpPoint.overallLatency[i].latency, 10);
          }
        }
      }
      finalVals[correpPoint.tier.toLowerCase()] = finalVal;
      data.push(correpPoint);
    }
  }
  var unit;
  if (isCountView) {
    unit = " received<br>";
  } else {
    unit = " minutes latency<br>";
  }
  var htmlText = "" + dateFormat(new Date(date)) + "<br>";
  if (finalVals['publisher'] != undefined) {
    htmlText += "Publisher: " + finalVals['publisher'] + unit;
  }
  if (finalVals['agent'] != undefined) {
    htmlText += "Agent: " + finalVals['agent'] + unit;
  }
  if (finalVals['collector'] != undefined) {
    htmlText += "Collector: " + finalVals['collector'] + unit;
  }
  if (finalVals['hdfs'] != undefined) {
    htmlText += "HDFS: " + finalVals['hdfs'] + unit;
  }
  if (finalVals['local'] != undefined) {
    htmlText += "Local: " + finalVals['local'] + unit;
  }
  if (finalVals['merge'] != undefined) {
    htmlText += "Merge: " + finalVals['merge'] + unit;
  }
  if (finalVals['mirror'] != undefined) {
    htmlText += "Mirror: " + finalVals['mirror'] + unit;
  }
  svg.selectAll("circle.smallindicator")
    .data(data)
    .enter()
    .append("circle")
    .attr("class", "smallindicator")
    .attr("cx", x(new Date(date)))
    .attr("cy", function (d) {
      return y(finalVals[d.tier.toLowerCase()]);
    })
    .attr("r", 4)
    .style("fill", function (d) {
      return tierColorMap[d.tier.toLowerCase()];
    })
    .style("cursor", "pointer")
    .on("click", function (d) {
      event.stopPropagation();
      showPointDetails(d, d3.event.pageX, d3.event.pageY);
    });
  var circle1 = svg.selectAll("circle.bigindicator")
    .data(data)
    .enter()
    .append("circle")
    .attr("class", "bigindicator")
    .attr("cx", x(new Date(date)))
    .attr("cy", function (d) {
      return y(finalVals[d.tier.toLowerCase()]);
    })
    .attr("r", 6)
    .style("stroke", function (d) {
      return tierColorMap[d.tier.toLowerCase()];
    })
    .style("fill", "none")
    .style("cursor", "pointer")
    .on("click", function (d) {
      showPointDetails(d, d3.event.pageX, d3.event.pageY);
    });
  popupDiv.html(htmlText);
}

function appendTierButtonPanel() {
  d3.select("#timelinePanel")
    .append("div")
    .attr("id", "tierButtonPanel")
    .style("width", "100%")
    .style("height", "40px");
  d3.select("#tierButtonPanel").selectAll("input")
    .data(tierbuttonList)
    .enter()
    .append("input")
    .attr("type", "button")
    .attr("class", "tierButton")
    .attr("value", function (d) {
      return d;
    })
    .attr("id", function (d) {
      return "button" + d.toLowerCase();
    })
    .on("click", function (d) {
      d3.selectAll(".tierButton")
        .style("height", "20px")
        .style("width", "100px")
        .style("margin-top", "10px");
      d3.select(this)
        .style("height", "24px")
        .style("width", "110px")
        .style("margin-top", "8px");
      clearTrendSVG();
      saveHistoryAndReload(currentStream, currentCluster, d.toLowerCase());
    })
    .style("background-color", function (d) {
      return tierColorMap[d.toLowerCase()];
    });
}

function highlightTierButton(tier) {
  d3.selectAll(".tierButton")
    .style("height", "20px")
    .style("width", "100px")
    .style("margin-top", "10px");
  d3.select(".tierButton#button" + tier.toLowerCase())
    .style("height", "24px")
    .style("width", "110px")
    .style("margin-top", "8px");
}

function renderTimeLine(result, timeticklength) {
  if (timeticklength != undefined && parseInt(timeticklength, 10) != 0) {
    timeinterval = parseInt(timeticklength, 10) * 60000;
  }
  if (result != undefined && result.length != 0) {
    timelinejson = JSON.parse(result);
  } else {
  	timelinejson = undefined;
  }
  clearTrendGraph();
  appendTierButtonPanel();
  highlightTierButton("All");
  renderTimeLineForTierStreamCluster('all', 'all', 'all');
}