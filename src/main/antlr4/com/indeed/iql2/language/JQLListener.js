// Generated from com/indeed/iql2/language/JQL.g4 by ANTLR 4.5.1
// jshint ignore: start
var antlr4 = require('antlr4/index');

// This class defines a complete listener for a parse tree produced by JQLParser.
function JQLListener() {
	antlr4.tree.ParseTreeListener.call(this);
	return this;
}

JQLListener.prototype = Object.create(antlr4.tree.ParseTreeListener.prototype);
JQLListener.prototype.constructor = JQLListener;

// Enter a parse tree produced by JQLParser#identifier.
JQLListener.prototype.enterIdentifier = function(ctx) {
};

// Exit a parse tree produced by JQLParser#identifier.
JQLListener.prototype.exitIdentifier = function(ctx) {
};


// Enter a parse tree produced by JQLParser#identifierTerminal.
JQLListener.prototype.enterIdentifierTerminal = function(ctx) {
};

// Exit a parse tree produced by JQLParser#identifierTerminal.
JQLListener.prototype.exitIdentifierTerminal = function(ctx) {
};


// Enter a parse tree produced by JQLParser#timeUnit.
JQLListener.prototype.enterTimeUnit = function(ctx) {
};

// Exit a parse tree produced by JQLParser#timeUnit.
JQLListener.prototype.exitTimeUnit = function(ctx) {
};


// Enter a parse tree produced by JQLParser#TimePeriodParseable.
JQLListener.prototype.enterTimePeriodParseable = function(ctx) {
};

// Exit a parse tree produced by JQLParser#TimePeriodParseable.
JQLListener.prototype.exitTimePeriodParseable = function(ctx) {
};


// Enter a parse tree produced by JQLParser#TimePeriodStringLiteral.
JQLListener.prototype.enterTimePeriodStringLiteral = function(ctx) {
};

// Exit a parse tree produced by JQLParser#TimePeriodStringLiteral.
JQLListener.prototype.exitTimePeriodStringLiteral = function(ctx) {
};


// Enter a parse tree produced by JQLParser#timePeriodTerminal.
JQLListener.prototype.enterTimePeriodTerminal = function(ctx) {
};

// Exit a parse tree produced by JQLParser#timePeriodTerminal.
JQLListener.prototype.exitTimePeriodTerminal = function(ctx) {
};


// Enter a parse tree produced by JQLParser#integer.
JQLListener.prototype.enterInteger = function(ctx) {
};

// Exit a parse tree produced by JQLParser#integer.
JQLListener.prototype.exitInteger = function(ctx) {
};


// Enter a parse tree produced by JQLParser#number.
JQLListener.prototype.enterNumber = function(ctx) {
};

// Exit a parse tree produced by JQLParser#number.
JQLListener.prototype.exitNumber = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyAggregateDivByConstant.
JQLListener.prototype.enterLegacyAggregateDivByConstant = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyAggregateDivByConstant.
JQLListener.prototype.exitLegacyAggregateDivByConstant = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyAggregatePercentile.
JQLListener.prototype.enterLegacyAggregatePercentile = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyAggregatePercentile.
JQLListener.prototype.exitLegacyAggregatePercentile = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyAggregateDiv.
JQLListener.prototype.enterLegacyAggregateDiv = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyAggregateDiv.
JQLListener.prototype.exitLegacyAggregateDiv = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyAggregateDistinct.
JQLListener.prototype.enterLegacyAggregateDistinct = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyAggregateDistinct.
JQLListener.prototype.exitLegacyAggregateDistinct = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyImplicitSum.
JQLListener.prototype.enterLegacyImplicitSum = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyImplicitSum.
JQLListener.prototype.exitLegacyImplicitSum = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyAggregateParens.
JQLListener.prototype.enterLegacyAggregateParens = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyAggregateParens.
JQLListener.prototype.exitLegacyAggregateParens = function(ctx) {
};


// Enter a parse tree produced by JQLParser#aggregateMetric.
JQLListener.prototype.enterAggregateMetric = function(ctx) {
};

// Exit a parse tree produced by JQLParser#aggregateMetric.
JQLListener.prototype.exitAggregateMetric = function(ctx) {
};


// Enter a parse tree produced by JQLParser#aggregateMetricEof.
JQLListener.prototype.enterAggregateMetricEof = function(ctx) {
};

// Exit a parse tree produced by JQLParser#aggregateMetricEof.
JQLListener.prototype.exitAggregateMetricEof = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateParens.
JQLListener.prototype.enterAggregateParens = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateParens.
JQLListener.prototype.exitAggregateParens = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateParent.
JQLListener.prototype.enterAggregateParent = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateParent.
JQLListener.prototype.exitAggregateParent = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateMetricMin.
JQLListener.prototype.enterAggregateMetricMin = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateMetricMin.
JQLListener.prototype.exitAggregateMetricMin = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateSumAcross2.
JQLListener.prototype.enterAggregateSumAcross2 = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateSumAcross2.
JQLListener.prototype.exitAggregateSumAcross2 = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateLag.
JQLListener.prototype.enterAggregateLag = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateLag.
JQLListener.prototype.exitAggregateLag = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateAvg.
JQLListener.prototype.enterAggregateAvg = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateAvg.
JQLListener.prototype.exitAggregateAvg = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateFieldMin.
JQLListener.prototype.enterAggregateFieldMin = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateFieldMin.
JQLListener.prototype.exitAggregateFieldMin = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateQualified.
JQLListener.prototype.enterAggregateQualified = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateQualified.
JQLListener.prototype.exitAggregateQualified = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateAverageAcross.
JQLListener.prototype.enterAggregateAverageAcross = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateAverageAcross.
JQLListener.prototype.exitAggregateAverageAcross = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregatePlusOrMinus.
JQLListener.prototype.enterAggregatePlusOrMinus = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregatePlusOrMinus.
JQLListener.prototype.exitAggregatePlusOrMinus = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateNamed.
JQLListener.prototype.enterAggregateNamed = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateNamed.
JQLListener.prototype.exitAggregateNamed = function(ctx) {
};


// Enter a parse tree produced by JQLParser#SyntacticallyAtomicAggregateMetric.
JQLListener.prototype.enterSyntacticallyAtomicAggregateMetric = function(ctx) {
};

// Exit a parse tree produced by JQLParser#SyntacticallyAtomicAggregateMetric.
JQLListener.prototype.exitSyntacticallyAtomicAggregateMetric = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateLog.
JQLListener.prototype.enterAggregateLog = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateLog.
JQLListener.prototype.exitAggregateLog = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateStandardDeviation.
JQLListener.prototype.enterAggregateStandardDeviation = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateStandardDeviation.
JQLListener.prototype.exitAggregateStandardDeviation = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateMetricMax.
JQLListener.prototype.enterAggregateMetricMax = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateMetricMax.
JQLListener.prototype.exitAggregateMetricMax = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregatePower.
JQLListener.prototype.enterAggregatePower = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregatePower.
JQLListener.prototype.exitAggregatePower = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateSumAcross.
JQLListener.prototype.enterAggregateSumAcross = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateSumAcross.
JQLListener.prototype.exitAggregateSumAcross = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregatePDiff.
JQLListener.prototype.enterAggregatePDiff = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregatePDiff.
JQLListener.prototype.exitAggregatePDiff = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateSum.
JQLListener.prototype.enterAggregateSum = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateSum.
JQLListener.prototype.exitAggregateSum = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateSingleScorer.
JQLListener.prototype.enterAggregateSingleScorer = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateSingleScorer.
JQLListener.prototype.exitAggregateSingleScorer = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateIfThenElse.
JQLListener.prototype.enterAggregateIfThenElse = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateIfThenElse.
JQLListener.prototype.exitAggregateIfThenElse = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateDocMetricAtom.
JQLListener.prototype.enterAggregateDocMetricAtom = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateDocMetricAtom.
JQLListener.prototype.exitAggregateDocMetricAtom = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateVariance.
JQLListener.prototype.enterAggregateVariance = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateVariance.
JQLListener.prototype.exitAggregateVariance = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateAbs.
JQLListener.prototype.enterAggregateAbs = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateAbs.
JQLListener.prototype.exitAggregateAbs = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateWindow.
JQLListener.prototype.enterAggregateWindow = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateWindow.
JQLListener.prototype.exitAggregateWindow = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateDiff.
JQLListener.prototype.enterAggregateDiff = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateDiff.
JQLListener.prototype.exitAggregateDiff = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateDistinctWindow.
JQLListener.prototype.enterAggregateDistinctWindow = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateDistinctWindow.
JQLListener.prototype.exitAggregateDistinctWindow = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateMultiplyOrDivideOrModulus.
JQLListener.prototype.enterAggregateMultiplyOrDivideOrModulus = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateMultiplyOrDivideOrModulus.
JQLListener.prototype.exitAggregateMultiplyOrDivideOrModulus = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateNegate.
JQLListener.prototype.enterAggregateNegate = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateNegate.
JQLListener.prototype.exitAggregateNegate = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregatePercentile.
JQLListener.prototype.enterAggregatePercentile = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregatePercentile.
JQLListener.prototype.exitAggregatePercentile = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateMedian.
JQLListener.prototype.enterAggregateMedian = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateMedian.
JQLListener.prototype.exitAggregateMedian = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateRatioScorer.
JQLListener.prototype.enterAggregateRatioScorer = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateRatioScorer.
JQLListener.prototype.exitAggregateRatioScorer = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateRunning.
JQLListener.prototype.enterAggregateRunning = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateRunning.
JQLListener.prototype.exitAggregateRunning = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateRMSError.
JQLListener.prototype.enterAggregateRMSError = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateRMSError.
JQLListener.prototype.exitAggregateRMSError = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateRatioDiff.
JQLListener.prototype.enterAggregateRatioDiff = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateRatioDiff.
JQLListener.prototype.exitAggregateRatioDiff = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateFieldMax.
JQLListener.prototype.enterAggregateFieldMax = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateFieldMax.
JQLListener.prototype.exitAggregateFieldMax = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateMetricFilter.
JQLListener.prototype.enterAggregateMetricFilter = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateMetricFilter.
JQLListener.prototype.exitAggregateMetricFilter = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateDistinct.
JQLListener.prototype.enterAggregateDistinct = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateDistinct.
JQLListener.prototype.exitAggregateDistinct = function(ctx) {
};


// Enter a parse tree produced by JQLParser#scopedField.
JQLListener.prototype.enterScopedField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#scopedField.
JQLListener.prototype.exitScopedField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#singlyScopedField.
JQLListener.prototype.enterSinglyScopedField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#singlyScopedField.
JQLListener.prototype.exitSinglyScopedField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#singlyScopedFieldTerminal.
JQLListener.prototype.enterSinglyScopedFieldTerminal = function(ctx) {
};

// Exit a parse tree produced by JQLParser#singlyScopedFieldTerminal.
JQLListener.prototype.exitSinglyScopedFieldTerminal = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateCounts.
JQLListener.prototype.enterAggregateCounts = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateCounts.
JQLListener.prototype.exitAggregateCounts = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateConstant.
JQLListener.prototype.enterAggregateConstant = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateConstant.
JQLListener.prototype.exitAggregateConstant = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateDocMetricAtom2.
JQLListener.prototype.enterAggregateDocMetricAtom2 = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateDocMetricAtom2.
JQLListener.prototype.exitAggregateDocMetricAtom2 = function(ctx) {
};


// Enter a parse tree produced by JQLParser#aggregateFilter.
JQLListener.prototype.enterAggregateFilter = function(ctx) {
};

// Exit a parse tree produced by JQLParser#aggregateFilter.
JQLListener.prototype.exitAggregateFilter = function(ctx) {
};


// Enter a parse tree produced by JQLParser#aggregateFilterEof.
JQLListener.prototype.enterAggregateFilterEof = function(ctx) {
};

// Exit a parse tree produced by JQLParser#aggregateFilterEof.
JQLListener.prototype.exitAggregateFilterEof = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateOr.
JQLListener.prototype.enterAggregateOr = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateOr.
JQLListener.prototype.exitAggregateOr = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateNot.
JQLListener.prototype.enterAggregateNot = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateNot.
JQLListener.prototype.exitAggregateNot = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateTermRegex.
JQLListener.prototype.enterAggregateTermRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateTermRegex.
JQLListener.prototype.exitAggregateTermRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateRegex.
JQLListener.prototype.enterAggregateRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateRegex.
JQLListener.prototype.exitAggregateRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateFalse.
JQLListener.prototype.enterAggregateFalse = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateFalse.
JQLListener.prototype.exitAggregateFalse = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateTermIs.
JQLListener.prototype.enterAggregateTermIs = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateTermIs.
JQLListener.prototype.exitAggregateTermIs = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateNotRegex.
JQLListener.prototype.enterAggregateNotRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateNotRegex.
JQLListener.prototype.exitAggregateNotRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateTrue.
JQLListener.prototype.enterAggregateTrue = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateTrue.
JQLListener.prototype.exitAggregateTrue = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateFilterParens.
JQLListener.prototype.enterAggregateFilterParens = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateFilterParens.
JQLListener.prototype.exitAggregateFilterParens = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateAnd.
JQLListener.prototype.enterAggregateAnd = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateAnd.
JQLListener.prototype.exitAggregateAnd = function(ctx) {
};


// Enter a parse tree produced by JQLParser#AggregateMetricInequality.
JQLListener.prototype.enterAggregateMetricInequality = function(ctx) {
};

// Exit a parse tree produced by JQLParser#AggregateMetricInequality.
JQLListener.prototype.exitAggregateMetricInequality = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomRawField.
JQLListener.prototype.enterDocMetricAtomRawField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomRawField.
JQLListener.prototype.exitDocMetricAtomRawField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasString.
JQLListener.prototype.enterLegacyDocMetricAtomHasString = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasString.
JQLListener.prototype.exitLegacyDocMetricAtomHasString = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasntString.
JQLListener.prototype.enterLegacyDocMetricAtomHasntString = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasntString.
JQLListener.prototype.exitLegacyDocMetricAtomHasntString = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasInt.
JQLListener.prototype.enterLegacyDocMetricAtomHasInt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasInt.
JQLListener.prototype.exitLegacyDocMetricAtomHasInt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasntInt.
JQLListener.prototype.enterLegacyDocMetricAtomHasntInt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasntInt.
JQLListener.prototype.exitLegacyDocMetricAtomHasntInt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasStringQuoted.
JQLListener.prototype.enterLegacyDocMetricAtomHasStringQuoted = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasStringQuoted.
JQLListener.prototype.exitLegacyDocMetricAtomHasStringQuoted = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasIntField.
JQLListener.prototype.enterLegacyDocMetricAtomHasIntField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasIntField.
JQLListener.prototype.exitLegacyDocMetricAtomHasIntField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasStringField.
JQLListener.prototype.enterLegacyDocMetricAtomHasStringField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasStringField.
JQLListener.prototype.exitLegacyDocMetricAtomHasStringField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomHasIntQuoted.
JQLListener.prototype.enterLegacyDocMetricAtomHasIntQuoted = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomHasIntQuoted.
JQLListener.prototype.exitLegacyDocMetricAtomHasIntQuoted = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomFloatScale.
JQLListener.prototype.enterLegacyDocMetricAtomFloatScale = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomFloatScale.
JQLListener.prototype.exitLegacyDocMetricAtomFloatScale = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomLucene.
JQLListener.prototype.enterLegacyDocMetricAtomLucene = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomLucene.
JQLListener.prototype.exitLegacyDocMetricAtomLucene = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricAtomRawField.
JQLListener.prototype.enterLegacyDocMetricAtomRawField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricAtomRawField.
JQLListener.prototype.exitLegacyDocMetricAtomRawField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomFieldEqual.
JQLListener.prototype.enterDocMetricAtomFieldEqual = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomFieldEqual.
JQLListener.prototype.exitDocMetricAtomFieldEqual = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomNotFieldEqual.
JQLListener.prototype.enterDocMetricAtomNotFieldEqual = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomNotFieldEqual.
JQLListener.prototype.exitDocMetricAtomNotFieldEqual = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomHasString.
JQLListener.prototype.enterDocMetricAtomHasString = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomHasString.
JQLListener.prototype.exitDocMetricAtomHasString = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomHasntString.
JQLListener.prototype.enterDocMetricAtomHasntString = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomHasntString.
JQLListener.prototype.exitDocMetricAtomHasntString = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomHasInt.
JQLListener.prototype.enterDocMetricAtomHasInt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomHasInt.
JQLListener.prototype.exitDocMetricAtomHasInt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomHasntInt.
JQLListener.prototype.enterDocMetricAtomHasntInt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomHasntInt.
JQLListener.prototype.exitDocMetricAtomHasntInt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomHasIntField.
JQLListener.prototype.enterDocMetricAtomHasIntField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomHasIntField.
JQLListener.prototype.exitDocMetricAtomHasIntField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomHasStringField.
JQLListener.prototype.enterDocMetricAtomHasStringField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomHasStringField.
JQLListener.prototype.exitDocMetricAtomHasStringField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomIntTermCount.
JQLListener.prototype.enterDocMetricAtomIntTermCount = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomIntTermCount.
JQLListener.prototype.exitDocMetricAtomIntTermCount = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomStrTermCount.
JQLListener.prototype.enterDocMetricAtomStrTermCount = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomStrTermCount.
JQLListener.prototype.exitDocMetricAtomStrTermCount = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomRandomField.
JQLListener.prototype.enterDocMetricAtomRandomField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomRandomField.
JQLListener.prototype.exitDocMetricAtomRandomField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomRandomMetric.
JQLListener.prototype.enterDocMetricAtomRandomMetric = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomRandomMetric.
JQLListener.prototype.exitDocMetricAtomRandomMetric = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomRegex.
JQLListener.prototype.enterDocMetricAtomRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomRegex.
JQLListener.prototype.exitDocMetricAtomRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomFloatScale.
JQLListener.prototype.enterDocMetricAtomFloatScale = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomFloatScale.
JQLListener.prototype.exitDocMetricAtomFloatScale = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomExtract.
JQLListener.prototype.enterDocMetricAtomExtract = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomExtract.
JQLListener.prototype.exitDocMetricAtomExtract = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomLucene.
JQLListener.prototype.enterDocMetricAtomLucene = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomLucene.
JQLListener.prototype.exitDocMetricAtomLucene = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricAtomLen.
JQLListener.prototype.enterDocMetricAtomLen = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricAtomLen.
JQLListener.prototype.exitDocMetricAtomLen = function(ctx) {
};


// Enter a parse tree produced by JQLParser#SyntacticallyAtomicDocMetricAtom.
JQLListener.prototype.enterSyntacticallyAtomicDocMetricAtom = function(ctx) {
};

// Exit a parse tree produced by JQLParser#SyntacticallyAtomicDocMetricAtom.
JQLListener.prototype.exitSyntacticallyAtomicDocMetricAtom = function(ctx) {
};


// Enter a parse tree produced by JQLParser#docMetric.
JQLListener.prototype.enterDocMetric = function(ctx) {
};

// Exit a parse tree produced by JQLParser#docMetric.
JQLListener.prototype.exitDocMetric = function(ctx) {
};


// Enter a parse tree produced by JQLParser#docMetricEof.
JQLListener.prototype.enterDocMetricEof = function(ctx) {
};

// Exit a parse tree produced by JQLParser#docMetricEof.
JQLListener.prototype.exitDocMetricEof = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocCounts.
JQLListener.prototype.enterLegacyDocCounts = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocCounts.
JQLListener.prototype.exitLegacyDocCounts = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocSignum.
JQLListener.prototype.enterLegacyDocSignum = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocSignum.
JQLListener.prototype.exitLegacyDocSignum = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocExp.
JQLListener.prototype.enterLegacyDocExp = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocExp.
JQLListener.prototype.exitLegacyDocExp = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocPlusOrMinus.
JQLListener.prototype.enterLegacyDocPlusOrMinus = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocPlusOrMinus.
JQLListener.prototype.exitLegacyDocPlusOrMinus = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocInt.
JQLListener.prototype.enterLegacyDocInt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocInt.
JQLListener.prototype.exitLegacyDocInt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocAbs.
JQLListener.prototype.enterLegacyDocAbs = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocAbs.
JQLListener.prototype.exitLegacyDocAbs = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMin.
JQLListener.prototype.enterLegacyDocMin = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMin.
JQLListener.prototype.exitLegacyDocMin = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocNegate.
JQLListener.prototype.enterLegacyDocNegate = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocNegate.
JQLListener.prototype.exitLegacyDocNegate = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocLog.
JQLListener.prototype.enterLegacyDocLog = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocLog.
JQLListener.prototype.exitLegacyDocLog = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMax.
JQLListener.prototype.enterLegacyDocMax = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMax.
JQLListener.prototype.exitLegacyDocMax = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocInequality.
JQLListener.prototype.enterLegacyDocInequality = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocInequality.
JQLListener.prototype.exitLegacyDocInequality = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricParens.
JQLListener.prototype.enterLegacyDocMetricParens = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricParens.
JQLListener.prototype.exitLegacyDocMetricParens = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocAtom.
JQLListener.prototype.enterLegacyDocAtom = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocAtom.
JQLListener.prototype.exitLegacyDocAtom = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMultOrDivideOrModulus.
JQLListener.prototype.enterLegacyDocMultOrDivideOrModulus = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMultOrDivideOrModulus.
JQLListener.prototype.exitLegacyDocMultOrDivideOrModulus = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocId.
JQLListener.prototype.enterDocId = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocId.
JQLListener.prototype.exitDocId = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocInt.
JQLListener.prototype.enterDocInt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocInt.
JQLListener.prototype.exitDocInt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocIfThenElse.
JQLListener.prototype.enterDocIfThenElse = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocIfThenElse.
JQLListener.prototype.exitDocIfThenElse = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocNegate.
JQLListener.prototype.enterDocNegate = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocNegate.
JQLListener.prototype.exitDocNegate = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocAbs.
JQLListener.prototype.enterDocAbs = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocAbs.
JQLListener.prototype.exitDocAbs = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMin.
JQLListener.prototype.enterDocMin = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMin.
JQLListener.prototype.exitDocMin = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocPlusOrMinus.
JQLListener.prototype.enterDocPlusOrMinus = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocPlusOrMinus.
JQLListener.prototype.exitDocPlusOrMinus = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricParens.
JQLListener.prototype.enterDocMetricParens = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricParens.
JQLListener.prototype.exitDocMetricParens = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocAtom.
JQLListener.prototype.enterDocAtom = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocAtom.
JQLListener.prototype.exitDocAtom = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocInequality.
JQLListener.prototype.enterDocInequality = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocInequality.
JQLListener.prototype.exitDocInequality = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocExp.
JQLListener.prototype.enterDocExp = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocExp.
JQLListener.prototype.exitDocExp = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricFilter.
JQLListener.prototype.enterDocMetricFilter = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricFilter.
JQLListener.prototype.exitDocMetricFilter = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocCounts.
JQLListener.prototype.enterDocCounts = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocCounts.
JQLListener.prototype.exitDocCounts = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocSignum.
JQLListener.prototype.enterDocSignum = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocSignum.
JQLListener.prototype.exitDocSignum = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMultOrDivideOrModulus.
JQLListener.prototype.enterDocMultOrDivideOrModulus = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMultOrDivideOrModulus.
JQLListener.prototype.exitDocMultOrDivideOrModulus = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocLog.
JQLListener.prototype.enterDocLog = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocLog.
JQLListener.prototype.exitDocLog = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMax.
JQLListener.prototype.enterDocMax = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMax.
JQLListener.prototype.exitDocMax = function(ctx) {
};


// Enter a parse tree produced by JQLParser#termVal.
JQLListener.prototype.enterTermVal = function(ctx) {
};

// Exit a parse tree produced by JQLParser#termVal.
JQLListener.prototype.exitTermVal = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyIntTerm.
JQLListener.prototype.enterLegacyIntTerm = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyIntTerm.
JQLListener.prototype.exitLegacyIntTerm = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyStringTerm.
JQLListener.prototype.enterLegacyStringTerm = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyStringTerm.
JQLListener.prototype.exitLegacyStringTerm = function(ctx) {
};


// Enter a parse tree produced by JQLParser#JqlIntTerm.
JQLListener.prototype.enterJqlIntTerm = function(ctx) {
};

// Exit a parse tree produced by JQLParser#JqlIntTerm.
JQLListener.prototype.exitJqlIntTerm = function(ctx) {
};


// Enter a parse tree produced by JQLParser#JqlStringTerm.
JQLListener.prototype.enterJqlStringTerm = function(ctx) {
};

// Exit a parse tree produced by JQLParser#JqlStringTerm.
JQLListener.prototype.exitJqlStringTerm = function(ctx) {
};


// Enter a parse tree produced by JQLParser#docFilter.
JQLListener.prototype.enterDocFilter = function(ctx) {
};

// Exit a parse tree produced by JQLParser#docFilter.
JQLListener.prototype.exitDocFilter = function(ctx) {
};


// Enter a parse tree produced by JQLParser#docFilterEof.
JQLListener.prototype.enterDocFilterEof = function(ctx) {
};

// Exit a parse tree produced by JQLParser#docFilterEof.
JQLListener.prototype.exitDocFilterEof = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocFieldIn.
JQLListener.prototype.enterLegacyDocFieldIn = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocFieldIn.
JQLListener.prototype.exitLegacyDocFieldIn = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocSample.
JQLListener.prototype.enterLegacyDocSample = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocSample.
JQLListener.prototype.exitLegacyDocSample = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyLucene.
JQLListener.prototype.enterLegacyLucene = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyLucene.
JQLListener.prototype.exitLegacyLucene = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocFieldIs.
JQLListener.prototype.enterLegacyDocFieldIs = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocFieldIs.
JQLListener.prototype.exitLegacyDocFieldIs = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocBetween.
JQLListener.prototype.enterLegacyDocBetween = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocBetween.
JQLListener.prototype.exitLegacyDocBetween = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocAnd.
JQLListener.prototype.enterLegacyDocAnd = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocAnd.
JQLListener.prototype.exitLegacyDocAnd = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocLuceneFieldIs.
JQLListener.prototype.enterLegacyDocLuceneFieldIs = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocLuceneFieldIs.
JQLListener.prototype.exitLegacyDocLuceneFieldIs = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocNot.
JQLListener.prototype.enterLegacyDocNot = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocNot.
JQLListener.prototype.exitLegacyDocNot = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocRegex.
JQLListener.prototype.enterLegacyDocRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocRegex.
JQLListener.prototype.exitLegacyDocRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocFieldIsnt.
JQLListener.prototype.enterLegacyDocFieldIsnt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocFieldIsnt.
JQLListener.prototype.exitLegacyDocFieldIsnt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocFalse.
JQLListener.prototype.enterLegacyDocFalse = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocFalse.
JQLListener.prototype.exitLegacyDocFalse = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocNotRegex.
JQLListener.prototype.enterLegacyDocNotRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocNotRegex.
JQLListener.prototype.exitLegacyDocNotRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocFilterParens.
JQLListener.prototype.enterLegacyDocFilterParens = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocFilterParens.
JQLListener.prototype.exitLegacyDocFilterParens = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocTrue.
JQLListener.prototype.enterLegacyDocTrue = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocTrue.
JQLListener.prototype.exitLegacyDocTrue = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocMetricInequality.
JQLListener.prototype.enterLegacyDocMetricInequality = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocMetricInequality.
JQLListener.prototype.exitLegacyDocMetricInequality = function(ctx) {
};


// Enter a parse tree produced by JQLParser#LegacyDocOr.
JQLListener.prototype.enterLegacyDocOr = function(ctx) {
};

// Exit a parse tree produced by JQLParser#LegacyDocOr.
JQLListener.prototype.exitLegacyDocOr = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocBetween.
JQLListener.prototype.enterDocBetween = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocBetween.
JQLListener.prototype.exitDocBetween = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocSampleMetric.
JQLListener.prototype.enterDocSampleMetric = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocSampleMetric.
JQLListener.prototype.exitDocSampleMetric = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocFieldIn.
JQLListener.prototype.enterDocFieldIn = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocFieldIn.
JQLListener.prototype.exitDocFieldIn = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocFieldIsnt.
JQLListener.prototype.enterDocFieldIsnt = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocFieldIsnt.
JQLListener.prototype.exitDocFieldIsnt = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocSample.
JQLListener.prototype.enterDocSample = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocSample.
JQLListener.prototype.exitDocSample = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocNot.
JQLListener.prototype.enterDocNot = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocNot.
JQLListener.prototype.exitDocNot = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocRegex.
JQLListener.prototype.enterDocRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocRegex.
JQLListener.prototype.exitDocRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocFieldIs.
JQLListener.prototype.enterDocFieldIs = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocFieldIs.
JQLListener.prototype.exitDocFieldIs = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocOr.
JQLListener.prototype.enterDocOr = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocOr.
JQLListener.prototype.exitDocOr = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocTrue.
JQLListener.prototype.enterDocTrue = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocTrue.
JQLListener.prototype.exitDocTrue = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocNotFieldEqual.
JQLListener.prototype.enterDocNotFieldEqual = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocNotFieldEqual.
JQLListener.prototype.exitDocNotFieldEqual = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocMetricInequality.
JQLListener.prototype.enterDocMetricInequality = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocMetricInequality.
JQLListener.prototype.exitDocMetricInequality = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocAnd.
JQLListener.prototype.enterDocAnd = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocAnd.
JQLListener.prototype.exitDocAnd = function(ctx) {
};


// Enter a parse tree produced by JQLParser#Lucene.
JQLListener.prototype.enterLucene = function(ctx) {
};

// Exit a parse tree produced by JQLParser#Lucene.
JQLListener.prototype.exitLucene = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocNotRegex.
JQLListener.prototype.enterDocNotRegex = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocNotRegex.
JQLListener.prototype.exitDocNotRegex = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocFilterParens.
JQLListener.prototype.enterDocFilterParens = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocFilterParens.
JQLListener.prototype.exitDocFilterParens = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocFalse.
JQLListener.prototype.enterDocFalse = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocFalse.
JQLListener.prototype.exitDocFalse = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocFieldInQuery.
JQLListener.prototype.enterDocFieldInQuery = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocFieldInQuery.
JQLListener.prototype.exitDocFieldInQuery = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DocFieldEqual.
JQLListener.prototype.enterDocFieldEqual = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DocFieldEqual.
JQLListener.prototype.exitDocFieldEqual = function(ctx) {
};


// Enter a parse tree produced by JQLParser#groupByEntry.
JQLListener.prototype.enterGroupByEntry = function(ctx) {
};

// Exit a parse tree produced by JQLParser#groupByEntry.
JQLListener.prototype.exitGroupByEntry = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DayOfWeekGroupBy.
JQLListener.prototype.enterDayOfWeekGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DayOfWeekGroupBy.
JQLListener.prototype.exitDayOfWeekGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#QuantilesGroupBy.
JQLListener.prototype.enterQuantilesGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#QuantilesGroupBy.
JQLListener.prototype.exitQuantilesGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#TopTermsGroupBy.
JQLListener.prototype.enterTopTermsGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#TopTermsGroupBy.
JQLListener.prototype.exitTopTermsGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#GroupByFieldIn.
JQLListener.prototype.enterGroupByFieldIn = function(ctx) {
};

// Exit a parse tree produced by JQLParser#GroupByFieldIn.
JQLListener.prototype.exitGroupByFieldIn = function(ctx) {
};


// Enter a parse tree produced by JQLParser#GroupByFieldInQuery.
JQLListener.prototype.enterGroupByFieldInQuery = function(ctx) {
};

// Exit a parse tree produced by JQLParser#GroupByFieldInQuery.
JQLListener.prototype.exitGroupByFieldInQuery = function(ctx) {
};


// Enter a parse tree produced by JQLParser#MetricGroupBy.
JQLListener.prototype.enterMetricGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#MetricGroupBy.
JQLListener.prototype.exitMetricGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#TimeGroupBy.
JQLListener.prototype.enterTimeGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#TimeGroupBy.
JQLListener.prototype.exitTimeGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#FieldGroupBy.
JQLListener.prototype.enterFieldGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#FieldGroupBy.
JQLListener.prototype.exitFieldGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#DatasetGroupBy.
JQLListener.prototype.enterDatasetGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#DatasetGroupBy.
JQLListener.prototype.exitDatasetGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#PredicateGroupBy.
JQLListener.prototype.enterPredicateGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#PredicateGroupBy.
JQLListener.prototype.exitPredicateGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#RandomGroupBy.
JQLListener.prototype.enterRandomGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#RandomGroupBy.
JQLListener.prototype.exitRandomGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#RandomMetricGroupBy.
JQLListener.prototype.enterRandomMetricGroupBy = function(ctx) {
};

// Exit a parse tree produced by JQLParser#RandomMetricGroupBy.
JQLListener.prototype.exitRandomMetricGroupBy = function(ctx) {
};


// Enter a parse tree produced by JQLParser#groupByElementEof.
JQLListener.prototype.enterGroupByElementEof = function(ctx) {
};

// Exit a parse tree produced by JQLParser#groupByElementEof.
JQLListener.prototype.exitGroupByElementEof = function(ctx) {
};


// Enter a parse tree produced by JQLParser#topTermsGroupByElem.
JQLListener.prototype.enterTopTermsGroupByElem = function(ctx) {
};

// Exit a parse tree produced by JQLParser#topTermsGroupByElem.
JQLListener.prototype.exitTopTermsGroupByElem = function(ctx) {
};


// Enter a parse tree produced by JQLParser#groupByMetric.
JQLListener.prototype.enterGroupByMetric = function(ctx) {
};

// Exit a parse tree produced by JQLParser#groupByMetric.
JQLListener.prototype.exitGroupByMetric = function(ctx) {
};


// Enter a parse tree produced by JQLParser#groupByTime.
JQLListener.prototype.enterGroupByTime = function(ctx) {
};

// Exit a parse tree produced by JQLParser#groupByTime.
JQLListener.prototype.exitGroupByTime = function(ctx) {
};


// Enter a parse tree produced by JQLParser#groupByField.
JQLListener.prototype.enterGroupByField = function(ctx) {
};

// Exit a parse tree produced by JQLParser#groupByField.
JQLListener.prototype.exitGroupByField = function(ctx) {
};


// Enter a parse tree produced by JQLParser#dateTime.
JQLListener.prototype.enterDateTime = function(ctx) {
};

// Exit a parse tree produced by JQLParser#dateTime.
JQLListener.prototype.exitDateTime = function(ctx) {
};


// Enter a parse tree produced by JQLParser#aliases.
JQLListener.prototype.enterAliases = function(ctx) {
};

// Exit a parse tree produced by JQLParser#aliases.
JQLListener.prototype.exitAliases = function(ctx) {
};


// Enter a parse tree produced by JQLParser#dataset.
JQLListener.prototype.enterDataset = function(ctx) {
};

// Exit a parse tree produced by JQLParser#dataset.
JQLListener.prototype.exitDataset = function(ctx) {
};


// Enter a parse tree produced by JQLParser#FullDataset.
JQLListener.prototype.enterFullDataset = function(ctx) {
};

// Exit a parse tree produced by JQLParser#FullDataset.
JQLListener.prototype.exitFullDataset = function(ctx) {
};


// Enter a parse tree produced by JQLParser#PartialDataset.
JQLListener.prototype.enterPartialDataset = function(ctx) {
};

// Exit a parse tree produced by JQLParser#PartialDataset.
JQLListener.prototype.exitPartialDataset = function(ctx) {
};


// Enter a parse tree produced by JQLParser#fromContents.
JQLListener.prototype.enterFromContents = function(ctx) {
};

// Exit a parse tree produced by JQLParser#fromContents.
JQLListener.prototype.exitFromContents = function(ctx) {
};


// Enter a parse tree produced by JQLParser#whereContents.
JQLListener.prototype.enterWhereContents = function(ctx) {
};

// Exit a parse tree produced by JQLParser#whereContents.
JQLListener.prototype.exitWhereContents = function(ctx) {
};


// Enter a parse tree produced by JQLParser#groupByContents.
JQLListener.prototype.enterGroupByContents = function(ctx) {
};

// Exit a parse tree produced by JQLParser#groupByContents.
JQLListener.prototype.exitGroupByContents = function(ctx) {
};


// Enter a parse tree produced by JQLParser#formattedAggregateMetric.
JQLListener.prototype.enterFormattedAggregateMetric = function(ctx) {
};

// Exit a parse tree produced by JQLParser#formattedAggregateMetric.
JQLListener.prototype.exitFormattedAggregateMetric = function(ctx) {
};


// Enter a parse tree produced by JQLParser#selectContents.
JQLListener.prototype.enterSelectContents = function(ctx) {
};

// Exit a parse tree produced by JQLParser#selectContents.
JQLListener.prototype.exitSelectContents = function(ctx) {
};


// Enter a parse tree produced by JQLParser#query.
JQLListener.prototype.enterQuery = function(ctx) {
};

// Exit a parse tree produced by JQLParser#query.
JQLListener.prototype.exitQuery = function(ctx) {
};


// Enter a parse tree produced by JQLParser#queryNoSelect.
JQLListener.prototype.enterQueryNoSelect = function(ctx) {
};

// Exit a parse tree produced by JQLParser#queryNoSelect.
JQLListener.prototype.exitQueryNoSelect = function(ctx) {
};



exports.JQLListener = JQLListener;