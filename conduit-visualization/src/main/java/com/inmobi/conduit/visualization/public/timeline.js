var timelienjson;
var dataPointList = [];
var timetick = 60; //in minutess
var minDate = 0, maxDate = 0, minCount = 0, maxCount = 0;
var tierbuttonList = ["Mirror", "Merge", "Local", "HDFS", "Collector", "Agent",
  "Publisher", "All"
];
var tierButtonColorList = ["#8E4804", "#FB6183", "#AE8886", "#F7977A", "#FF86C2", "#DD75DD", "#FF9C42", "#659CEF"];
var svg;

function ClusterStats(cluster, received, sent) {
	this.cluster = cluster;
	this.received = received;
	this.sent = sent;
}

function TopicStatsTimeLine(topic) {
	this.topic = topic;
	this.clusterStatsList = [];
}

function DataPoint(time, tier, aggreceived, aggsent) {
	this.time = time;
	this.tier = tier;
	this.aggreceived = aggreceived;
	this.aggsent = aggsent;
	this.topicStatList = [];
}

function buildDataPointList(tier, stream, cluster) {
	console.log("building data point list for tier:"+tier+" stream:"+stream+" and cluster:"+cluster);
	dataPointList.length = 0;
	if (timelienjson != undefined) {
		for (var i = 0; i < timelienjson.datapoints.length; i++) {
			var tierList = timelienjson.datapoints[i];
			var currentTier = tierList.tier;
			if (tier.toLowerCase() == 'all' || (tier.toLowerCase() != 'all' && currentTier.toLowerCase() == tier.toLowerCase())) {
				var tierPointList = [];
				for (var j = 0; j < tierList.tierWisePointList.length; j++) {
					var p = tierList.tierWisePointList[j];
					var actualReceived = 0;
						var actualSent = 0;
						var datapoint = new DataPoint(parseInt(p.time, 10), currentTier, parseInt(p.aggreceived, 10), parseInt(p.aggsent, 10));
						if (minDate == 0 || parseInt(p.time, 10) < minDate) {
							minDate = parseInt(p.time, 10);
						}
						if (maxDate == 0 || parseInt(p.time, 10) > maxDate) {
							maxDate = parseInt(p.time, 10);
						}
						if (minCount == 0 || parseInt(p.aggreceived, 10) < minCount) {
							minCount = parseInt(p.aggreceived, 10);
						}
						if (maxCount == 0 || parseInt(p.aggreceived, 10) > maxCount) {
							maxCount = parseInt(p.aggreceived, 10);
						}
						var streamList = p.topicCountList;
						for (var k = 0; k < streamList.length; k++) {
							var streamEntry = streamList[k];
							var currentStream = streamEntry.topic;
							var newTopicStats = new TopicStatsTimeLine(currentStream);
							if (stream == 'all' || (stream != 'all' && stream == currentStream)) {
								for (var l = 0; l < streamEntry.clusterStats.length; l++) {
									var clusterEntry = streamEntry.clusterStats[l];
									var currentCluster = clusterEntry.cluster;
									if (cluster == 'all' || (cluster != 'all' && cluster == currentCluster)) {
										var newClusterStats = new ClusterStats(currentCluster, parseInt(clusterEntry.received, 10), parseInt(clusterEntry.sent, 10));
										newTopicStats.clusterStatsList.push(newClusterStats);
										actualReceived += parseInt(clusterEntry.received, 10);
										actualSent += parseInt(clusterEntry.sent, 10);
										if (cluster != 'all' && cluster == currentCluster) {
											break;
										}
									}
								}
								datapoint.topicStatList.push(newTopicStats);
								if (stream != 'all' && stream == currentStream) {
									break;
								}
							}
						}
						datapoint.aggreceived = actualReceived;
						datapoint.aggsent = actualSent;
						tierPointList.push(datapoint);
				}
				dataPointList.push(tierPointList);
				if (tier != 'all' && currentTier == tier) {
					break;
				}
			}
		}
	}
	console.log("data point list:");
	console.log(dataPointList);
}

function clearSVG() {
  d3.select("#timelineSVG").remove();
}

function clearTimeLineGraph() {
  clearSVG();
  d3.select("#tierButtonPanel").remove();
}

var margin = {top: 20, right: 20, bottom: 50, left: 100},
    width = 1250 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

var formatDate = d3.time.format("%x-%X");

var timescale = d3.time.scale.utc();
//time-scale to scale time within range [0, width] i.e. over x-axis
var x = d3.time.scale.utc()
    .range([0, width]);

//linear scale to scale count within range [height, 0] i.e. over y-axis
var y = d3.scale.linear()
    .range([height, 0]);

var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
    .tickSize(-height, 0, 0)
    .tickPadding(6);

var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(20)
    .tickSize(-width, 0, 0)
    .tickPadding(6);

var line = d3.svg.line()
    .x(function(d) { return x(new Date(d.time * 1000)); })
    .y(function(d) { return y(d.aggreceived); });

var brush = d3.svg.brush()
    .x(x)
    .on("brush", brushed);

function brushed() {
  console.log(brush.extent());
  x.domain(brush.empty() ? x.domain() : brush.extent());
  svg.selectAll("path").attr("d", line);
  svg.select(".x.grid").call(xAxis);
}

function renderTimeLineForTierStreamCluster(tier, stream, cluster) {

  console.log("Calling renderTimeLine for tier:"+tier+" stream:"+stream+" and cluster:"+cluster);

  minDate = 0, maxDate = 0, minCount = 0, maxCount = 0;

	buildDataPointList(tier, stream, cluster);

  svg = d3.select("#timelinePanel").append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .attr("id", "timelineSVG")
    .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  x.domain(d3.extent([new Date((minDate * 1000) - 3600000), new Date((maxDate * 1000) + 3600000)]));
  y.domain(d3.extent([minCount, maxCount]));

  svg.append("g")
      .attr("class", "grid")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

  svg.append("g")
      .attr("class", "grid")
      .call(yAxis);

  svg.append("g")
      .attr("class", "x brush")
      .call(brush)
    .selectAll("rect")
      .attr("y", -6)
      .attr("height", height + 7);

  dataPointList.forEach(function (t) {
    var color = "steelblue";
    if (t[0].tier.toLowerCase() == "publisher") {
      color = hexcodeList[0];
    } else if (t[0].tier.toLowerCase() == "agent") {
      color = hexcodeList[1];
    } else if (t[0].tier.toLowerCase() == "collector") {
      color = hexcodeList[3];
    } else if (t[0].tier.toLowerCase() == "hdfs") {
      color = hexcodeList[4];
    } else if (t[0].tier.toLowerCase() == "local") {
      color = hexcodeList[5];
    } else if (t[0].tier.toLowerCase() == "merge") {
      color = hexcodeList[6];
    } else if (t[0].tier.toLowerCase() == "mirror") {
      color = hexcodeList[7];
    }
	  svg.append("path")
	      .datum(t)
	      .attr("d", line)
        .attr("fill", "none")
        .attr("stroke", color);

	  svg.selectAll("circle")
	  		.filter(function(d) {
	  			return d.tier.toLowerCase() == t[0].tier.toLowerCase();
	  		})
	  		.data(t)
	  		.enter()
	  		.append("circle")
	  		.attr("cx", function(d) {
	  			return x(new Date(d.time * 1000));
	  		})
	  		.attr("cy", function(d) {
	  			return y(d.aggreceived);
	  		})
	  		.attr("r", 2)
	  		.style("fill", color);
  });
}

function appendTierButtonPanel() {

	console.log("append tier button panel");

  d3.select("#timelinePanel")
    .append("div")
    .attr("id", "tierButtonPanel")
    .style("width", "100%")
    .style("height", "30px");

  d3.select("#tierButtonPanel").selectAll("input")
      .data(tierbuttonList)
      .enter()
      .append("input")
      .attr("type","button")
      .attr("class","tierButton")
      .attr("value", function (d) {
          return d;
      })
      .on("click", function (d) {
        clearSVG();
        renderTimeLineForTierStreamCluster(d.toLowerCase(), "all", "all");
      })
      .style("background-color", function (d) {
        var color = "cornflowerblue";
        if (d.toLowerCase() == "publisher") {
          color = hexcodeList[0];
        } else if (d.toLowerCase() == "agent") {
          color = hexcodeList[1];
        } else if (d.toLowerCase() == "collector") {
          color = hexcodeList[3];
        } else if (d.toLowerCase() == "hdfs") {
          color = hexcodeList[4];
        } else if (d.toLowerCase() == "local") {
          color = hexcodeList[5];
        } else if (d.toLowerCase() == "merge") {
          color = hexcodeList[6];
        } else if (d.toLowerCase() == "mirror") {
          color = hexcodeList[7];
        }
        return color;
      });
}

function renderTimeLine(result, timeticklength) {
	console.log("render time line:"+result);
	if (timeticklength != undefined && parseInt(timeticklength, 10) != 0) {
		timetick = parseInt(timeticklength, 10);
	}
	console.log("timetick:"+timetick);
	if(result!= undefined && result.length != 0) {
		timelienjson = JSON.parse(result);
	}
	console.log("timelienJson:"+timelienjson);

  clearTimeLineGraph();
  console.log("cleared graph");
  appendTierButtonPanel();
	renderTimeLineForTierStreamCluster('all', 'all', 'all');
}