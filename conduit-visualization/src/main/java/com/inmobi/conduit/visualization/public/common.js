var publisherSla, agentSla, vipSla, collectorSla, hdfsSla, localSla,
mergeSla, mirrorSla, percentileForSla, percentageForLoss, percentageForWarn,
lossWarnThresholdDiff;

function setConfiguration(pSla, aSla, vSla, cSla, hSla, lSla, meSla, miSla,
percentileFrSla, percentageFrLoss, percentageFrWarn, lossWarnThreshold,
devMode) {
	publisherSla = pSla;
  agentSla = aSla;
  vipSla = vSla;
  collectorSla = cSla;
  hdfsSla = hSla;
  localSla = lSla;
  mergeSla = meSla;
  mirrorSla = miSla;
  percentageForLoss = percentageFrLoss;
  percentageForWarn = percentageFrWarn;
  percentileForSla = percentileFrSla;
  lossWarnThresholdDiff = lossWarnThreshold;
  isDevMode = devMode;
}