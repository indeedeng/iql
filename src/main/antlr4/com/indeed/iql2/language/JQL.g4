grammar JQL;

LAG : 'LAG' ;
RUNNING : 'RUNNING' ;
PARENT : 'PARENT' ;
DISTINCT : 'DISTINCT' ;
DISTINCT_WINDOW : 'DISTINCT_WINDOW' ;
WINDOW : 'WINDOW' ;
PERCENTILE : 'PERCENTILE' ;
MEDIAN : 'MEDIAN' ;
PDIFF : 'PDIFF' ;
DIFF : 'DIFF' ;
RATIODIFF: 'RATIODIFF';
SINGLESCORE : 'SINGLESCORE' ;
RATIOSCORE : 'RATIOSCORE' ;
RMSERROR : 'RMSERROR' ;
LOGLOSS : 'LOGLOSS' ;
AVG: 'AVG' ;
VARIANCE : 'VARIANCE' ;
STDEV : 'STDEV' ;
LOG : 'LOG' ;
ABS : 'ABS' ;
FLOOR: 'FLOOR' ;
CEIL: 'CEIL' ;
ROUND: 'ROUND' ;
SUM_OVER : 'SUM_OVER' ;
AVG_OVER : 'AVG_OVER' ;
WHERE : 'WHERE' ;
HASSTR : 'HASSTR' ;
HASINT : 'HASINT' ;
SELECT : 'SELECT' ;
FROM : 'FROM' ;
GROUP : 'GROUP' ;
BY : 'BY' ;
AGO : 'AGO' ;
COUNT : 'COUNT' ;
AS : 'AS' ;
NOT : 'NOT' ;
LUCENE : 'LUCENE' ;
QUERY : 'QUERY' ;
TOP : 'TOP' ;
BOTTOM : 'BOTTOM' ;
WITH : 'WITH' ;
DEFAULT : 'DEFAULT' ;
TIME : 'TIME' ;
TIMEBUCKETS : 'TIMEBUCKETS' ;
BUCKETS : 'BUCKETS' ;
BUCKET : 'BUCKET' ;
B : 'B' ;
IN : 'IN' ;
DESCENDING : 'DESCENDING' ;
DESC : 'DESC' ;
ASCENDING : 'ASCENDING' ;
ASC : 'ASC' ;
DAYOFWEEK : 'DAYOFWEEK' ;
QUANTILES : 'QUANTILES' ;
BETWEEN : 'BETWEEN' ;
SAMPLE : 'SAMPLE' ;
AND : 'AND' ;
OR : 'OR' ;
TRUE : 'TRUE' ;
FALSE : 'FALSE' ;
IF : 'IF' ;
THEN : 'THEN' ;
ELSE : 'ELSE' ;
FLOATSCALE : 'FLOATSCALE' ;
SIGNUM : 'SIGNUM' ;
LIMIT : 'LIMIT';
HAVING : 'HAVING';
FIELD_MIN : 'FIELD_MIN';
FIELD_MAX : 'FIELD_MAX';
ALIASING : 'ALIASING';
HASSTRFIELD : 'HASSTRFIELD' ;
HASINTFIELD : 'HASINTFIELD' ;
LEN : 'LEN' ;
INTTERMCOUNT : 'INTTERMCOUNT' ;
STRTERMCOUNT : 'STRTERMCOUNT' ;
SAME : 'SAME' ;
EXP : 'EXP' ;
WINDOW_SUM : 'WINDOW_SUM' ;
MIN : 'MIN' ;
MAX : 'MAX' ;
PRINTF : 'PRINTF' ;
ROUNDING: 'ROUNDING' ;
EXTRACT : 'EXTRACT' ;
RELATIVE: 'RELATIVE' ;
DATASET: 'DATASET' ;
RANDOM: 'RANDOM' ;
OPTIONS: 'OPTIONS' ;
DOCID: 'DOCID' ;

M: 'M' ;
Y : 'Y' ;
TODAYS : 'T' | 'TO' | 'TOD' | 'TODA' | 'TODAY' ;
TOMORROWS : 'TOM' | 'TOMO' | 'TOMOR' | 'TOMORR' | 'TOMORRO' | 'TOMORROW' ;
// Note that M term goes before TIME_UNIT so we have to handle M separatelly
// We cannot put M after TIME_UNIT because in that case metric M will not parse.
YESTERDAYS : Y | 'YE' | 'YES' | 'YEST' | 'YESTE' | 'YESTER' | 'YESTERD' | 'YESTERDA' | 'YESTERDAY' ;
TIME_UNIT : [SHDW]|M|Y|'SECOND'|'SECONDS'|'MINUTE'|'MINUTES'|'HOUR'|'HOURS'|'DAY'|'DAYS'|'WEEK'|'WEEKS'|'MO'|'MONTH'|'MONTHS'|'YEAR'|'YEARS';
BUCKET_ATOM : [0-9]* (BUCKET|BUCKETS|B);

TIME_INTERVAL_ATOM : ([0-9]* (TIME_UNIT|M|Y))+; // time interval without spaces like '1d1week'
timeIntervalOneWord : TIME_INTERVAL_ATOM | TIME_UNIT | M | Y ;
timeIntervalAtom: (coeff=NAT unit=(TIME_UNIT|M | Y)) ; // time interval with spaces like '5 days'

timeInterval : (withoutSpaces+=timeIntervalOneWord | withSpaces+=timeIntervalAtom)+;
bucket : BUCKET_ATOM | (coeff=NAT? (BUCKET|BUCKETS|B));

NAT : [0-9]+ ;
DOUBLE: [0-9]+ ('.' [0-9]*)? ;

fragment DIGIT : [0-9] ;
fragment SINGLE_DOUBLE_DIGITS : ( DIGIT DIGIT | DIGIT ) ;
// note that 4-digit terms like '2015' will be parsed as NAT since NAT is before DATETIME_TOKEN
// There must be special processing for this corner case.
DATETIME_TOKEN
 : DIGIT DIGIT DIGIT DIGIT
    ('-' SINGLE_DOUBLE_DIGITS
        ('-' SINGLE_DOUBLE_DIGITS
            (('T'|' ') SINGLE_DOUBLE_DIGITS
                (':' SINGLE_DOUBLE_DIGITS
                    (':' SINGLE_DOUBLE_DIGITS
                        ('.' DIGIT DIGIT DIGIT
                            (('+'|'-') DIGIT DIGIT ':' DIGIT DIGIT)?
                        )?
                    )?
                )
            )?
        )?
    )? ;

ID : [a-zA-Z_][a-zA-Z0-9_]* ;

BACKQUOTED_ID : '`' ~[`]+ '`';

// TODO: How to keep this up to date with new lexer tokens..?
identifier
    : ID | LAG | RUNNING | PARENT | DISTINCT | DISTINCT_WINDOW | WINDOW | PERCENTILE | MEDIAN | PDIFF | DIFF | RATIODIFF | SINGLESCORE
    | RATIOSCORE | RMSERROR | LOGLOSS | AVG | VARIANCE | STDEV | LOG | ABS | SUM_OVER | AVG_OVER | WHERE | HASSTR | HASINT | FROM | GROUP | BY | FLOOR | CEIL | ROUND
    | AGO | COUNT | AS | NOT | LUCENE | QUERY | TOP | BOTTOM | WITH | DEFAULT| TIME | TIMEBUCKETS
    | BUCKETS | BUCKET | B | IN | DESCENDING | DESC | ASCENDING | ASC | DAYOFWEEK | QUANTILES | BETWEEN
    | SAMPLE | AND | OR | TRUE | FALSE | IF | THEN | ELSE | FLOATSCALE | SIGNUM | LIMIT | HAVING
    | FIELD_MIN | FIELD_MAX | ALIASING | HASINTFIELD | HASSTRFIELD | INTTERMCOUNT | STRTERMCOUNT | SAME | EXP | WINDOW_SUM | MIN | MAX
    | PRINTF | EXTRACT | RANDOM | OPTIONS
    | M | Y | TODAYS | TOMORROWS | YESTERDAYS | TIME_UNIT | TIME_INTERVAL_ATOM
    | RELATIVE | DATASET
    | BACKQUOTED_ID | LEN | DOCID
    ;
identifierTerminal : identifier EOF ;

// used in group by time(timeBucket)
timeBucket
    : timeInterval
    | bucket
    | STRING_LITERAL ; // unquoted string must be parseable by timeBucketTerminal rule.

timeBucketTerminal : timeBucket EOF ;

// This rule is used for unquoted time intervals
// like 'from dataset 1w1d ago 1d where ...'
// We don't allow here queries like 'from dataset 1w 1d ago 1d where ...'
relativeTime
    : TODAYS
    | TOMORROWS
    | YESTERDAYS | Y // Special case for 'Y' since will be parsed with timeIntervalOneWord rule otherwise.
    | timeIntervalOneWord AGO?
    ;

// This rule is used for quoted parsed intervals.
// It's ok to have spaces between time interval part inside string literals
// like 'from dataset "1 week 5days ago" "1 d" where ...'
relativeTimeTerminal : (relativeTime | timeInterval AGO?) EOF;

WS : [ \t\r\n]+ -> channel(HIDDEN) ;
COMMENT : '/*' .*? '*/' -> channel(HIDDEN) ;
LINE_COMMENT : '--' .*? ~[\r\n]* -> channel(HIDDEN) ;

integer : neg='-'? NAT ;
number : (neg='-'? NAT) | DOUBLE ;

fragment ESCAPED_SINGLE_QUOTE : '\\\'';
fragment SINGLE_QUOTED_CONTENTS : ( ESCAPED_SINGLE_QUOTE | ~('\n'|'\r') )*? ;
fragment SINGLE_QUOTED_STRING : '\'' SINGLE_QUOTED_CONTENTS '\'';

fragment ESCAPED_DOUBLE_QUOTE : '\\"';
fragment DOUBLE_QUOTED_CONTENTS : ( ESCAPED_DOUBLE_QUOTE | ~('\n'|'\r') )*? ;
fragment DOUBLE_QUOTED_STRING : '"' DOUBLE_QUOTED_CONTENTS '"';

STRING_LITERAL : SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING ;

legacyAggregateMetric
    : DISTINCT '(' identifier ')' # LegacyAggregateDistinct
    | PERCENTILE '(' identifier ',' number ')' # LegacyAggregatePercentile
    | legacyAggregateMetric '/' number # LegacyAggregateDivByConstant
    | legacyDocMetric '/' legacyDocMetric # LegacyAggregateDiv
    | '(' legacyAggregateMetric ')' # LegacyAggregateParens
    | legacyDocMetric # LegacyImplicitSum
    ;

aggregateMetric [boolean useLegacy]
    : {$ctx.useLegacy}? legacyAggregateMetric
    | {!$ctx.useLegacy}? jqlAggregateMetric
    ;

aggregateMetricEof [boolean useLegacy]
    : aggregateMetric[$ctx.useLegacy] EOF
    ;

jqlAggregateMetric
    : field=identifier '.' syntacticallyAtomicJqlAggregateMetric # AggregateQualified
    | IF filter=jqlAggregateFilter THEN trueCase=jqlAggregateMetric ELSE falseCase=jqlAggregateMetric # AggregateIfThenElse
    | LAG '(' NAT ',' jqlAggregateMetric ')' # AggregateLag
    | RUNNING '(' jqlAggregateMetric ')' # AggregateRunning
    | PARENT '(' jqlAggregateMetric ')' # AggregateParent
    | DISTINCT '(' scopedField (HAVING jqlAggregateFilter)? ')' # AggregateDistinct
    | DISTINCT_WINDOW '(' NAT ',' scopedField (HAVING jqlAggregateFilter)? ')' # AggregateDistinctWindow
    | (old=WINDOW | WINDOW_SUM) '(' NAT ',' jqlAggregateMetric ')' # AggregateWindow
    | PERCENTILE '(' scopedField ',' number ')' # AggregatePercentile
    | MEDIAN '(' scopedField ')' # AggregateMedian
    | PDIFF '(' expected=jqlAggregateMetric ',' actual=jqlAggregateMetric ')' # AggregatePDiff
    | DIFF '(' controlGrp = jqlAggregateMetric ',' testGrp = jqlAggregateMetric ')' #AggregateDiff
    | RATIODIFF '(' controlClcMetric = jqlAggregateMetric ',' controlImpMetric=jqlAggregateMetric ',' testClcMetric=jqlAggregateMetric ',' testImpMetric=jqlAggregateMetric ')' #AggregateRatioDiff
    | SINGLESCORE '(' controlGrp=jqlAggregateMetric ',' testGrp=jqlAggregateMetric ')' # AggregateSingleScorer
    | RATIOSCORE '(' controlClcMetric = jqlAggregateMetric ',' controlImpMetric=jqlAggregateMetric ',' testClcMetric=jqlAggregateMetric ',' testImpMetric=jqlAggregateMetric ')' #AggregateRatioScorer
    | RMSERROR '('  predictedVal = jqlAggregateMetric ',' actualVal=jqlAggregateMetric ',' total=jqlAggregateMetric ','  grouping=jqlDocMetric ',' lowerLimit=integer ',' upperLimit=integer ',' stepSize=integer (',' useRatio=identifier)?')' #AggregateRMSError
    | LOGLOSS '('  label=jqlDocFilter ',' score=jqlDocMetric ',' scale=number ')' # AggregateLogLoss
    | AVG '(' jqlAggregateMetric ')' # AggregateAvg
    | VARIANCE '(' jqlDocMetric ')' # AggregateVariance
    | STDEV '(' jqlDocMetric ')' # AggregateStandardDeviation
    | LOG '(' jqlAggregateMetric ')' # AggregateLog
    | ABS '(' jqlAggregateMetric ')' # AggregateAbs
    | FLOOR '(' jqlAggregateMetric (',' digits = integer)? ')' # AggregateFloor
    | CEIL '(' jqlAggregateMetric (',' digits = integer)? ')' # AggregateCeil
    | ROUND '(' jqlAggregateMetric (',' digits = integer)? ')' # AggregateRound
    | FIELD_MIN '(' scopedField (BY aggregate=jqlAggregateMetric)? (HAVING filter=jqlAggregateFilter)? ')' # AggregateFieldMin
    | FIELD_MAX '(' scopedField (BY aggregate=jqlAggregateMetric)? (HAVING filter=jqlAggregateFilter)? ')' # AggregateFieldMax
    | MIN '(' metrics+=jqlAggregateMetric (',' metrics+=jqlAggregateMetric)* ')' # AggregateMetricMin
    | MAX '(' metrics+=jqlAggregateMetric (',' metrics+=jqlAggregateMetric)* ')' # AggregateMetricMax
    | SUM_OVER '(' groupByElement[false] ',' jqlAggregateMetric ')' # AggregateSumAcross
    | SUM_OVER '(' field=scopedField (HAVING jqlAggregateFilter)? ',' jqlAggregateMetric ')' # AggregateSumAcross2
    | AVG_OVER '(' field=scopedField ((havingBrackets='[' HAVING jqlAggregateFilter ']')|(HAVING jqlAggregateFilter))? ',' jqlAggregateMetric ')' # AggregateAverageAcross
    | M '(' jqlAggregateFilter ')' # AggregateMetricFilter
    | jqlDocMetricAtom # AggregateDocMetricAtom
    | '-' jqlAggregateMetric # AggregateNegate
    | <assoc=right> jqlAggregateMetric '^' jqlAggregateMetric # AggregatePower
    | jqlAggregateMetric (multiply='*'|divide='/'|modulus='%') jqlAggregateMetric # AggregateMultiplyOrDivideOrModulus
    | jqlAggregateMetric (plus='+'|minus='-') jqlAggregateMetric # AggregatePlusOrMinus
    | jqlAggregateMetric AS name=identifier # AggregateNamed
    | '[' jqlDocMetric ']' # AggregateSum
    | '(' jqlAggregateMetric ')' # AggregateParens
    | syntacticallyAtomicJqlAggregateMetric # SyntacticallyAtomicAggregateMetric
    ;

scopedField
    : field=identifier
    | oneScope=identifier '.' field=identifier
    | '[' manyScope+=identifier (',' manyScope+=identifier)* ']' '.' field=identifier
    ;

singlyScopedField
    : field=identifier
    | oneScope=identifier '.' field=identifier
    ;
singlyScopedFieldTerminal : singlyScopedField EOF ;

syntacticallyAtomicJqlAggregateMetric
    : (COUNT '(' ')') # AggregateCounts
    | number # AggregateConstant
    | jqlSyntacticallyAtomicDocMetricAtom # AggregateDocMetricAtom2
    ;

aggregateFilter [boolean useLegacy]
    : {$ctx.useLegacy}? {false}? // No such thing
    | {!$ctx.useLegacy}? jqlAggregateFilter
    ;

aggregateFilterEof [boolean useLegacy]
    : aggregateFilter[$ctx.useLegacy] EOF
    ;

jqlAggregateFilter
    : field=identifier '=~' STRING_LITERAL # AggregateRegex
    | field=identifier '!=~' STRING_LITERAL # AggregateNotRegex
    | 'TERM()' '=' jqlTermVal # AggregateTermIs
    | 'TERM()' '=~' STRING_LITERAL # AggregateTermRegex
    | jqlAggregateMetric op=('='|'!='|'<'|'<='|'>'|'>=') jqlAggregateMetric # AggregateMetricInequality
    | '!' jqlAggregateFilter # AggregateNot
    | NOT '(' jqlAggregateFilter ')' # AggregateNot
    | jqlAggregateFilter (AND | '&&') jqlAggregateFilter # AggregateAnd
    | jqlAggregateFilter (OR | '||') jqlAggregateFilter # AggregateOr
    | '(' jqlAggregateFilter ')' # AggregateFilterParens
    | TRUE # AggregateTrue
    | FALSE # AggregateFalse
    ;

jqlSyntacticallyAtomicDocMetricAtom
    : singlyScopedField # DocMetricAtomRawField
    ;

legacyDocMetricAtom
    : field=identifier '=' (quotedTerm=STRING_LITERAL | idTerm=identifier) # LegacyDocMetricAtomHasString
    | HASSTR '(' field=identifier ',' (quotedTerm=STRING_LITERAL | idTerm=identifier  | numTerm=number) ')' # LegacyDocMetricAtomHasString
    | field=identifier '!=' (quotedTerm=STRING_LITERAL | idTerm=identifier) # LegacyDocMetricAtomHasntString
    | field=identifier '=' term=integer # LegacyDocMetricAtomHasInt
    | HASINT '(' field=identifier ',' term=integer ')' # LegacyDocMetricAtomHasInt
    | field=identifier '!=' integer # LegacyDocMetricAtomHasntInt
    | HASSTR '(' STRING_LITERAL ')' # LegacyDocMetricAtomHasStringQuoted
    | HASINTFIELD '(' field=identifier ')' # LegacyDocMetricAtomHasIntField
    | HASSTRFIELD '(' field=identifier ')' # LegacyDocMetricAtomHasStringField
    | HASINT '(' STRING_LITERAL ')' # LegacyDocMetricAtomHasIntQuoted
    | FLOATSCALE '(' field=identifier (',' mult=number (',' add=number)?)?')' # LegacyDocMetricAtomFloatScale
    | LUCENE '(' queryField=STRING_LITERAL ')' # LegacyDocMetricAtomLucene
    | identifier # LegacyDocMetricAtomRawField
    ;

jqlDocMetricAtom
    : singlyScopedField '=' singlyScopedField # DocMetricAtomFieldEqual
    | singlyScopedField '!=' singlyScopedField # DocMetricAtomNotFieldEqual
    | singlyScopedField '=' term=STRING_LITERAL # DocMetricAtomHasString
    | HASSTR '(' singlyScopedField ',' term=STRING_LITERAL ')' # DocMetricAtomHasString
    | singlyScopedField '!=' term=STRING_LITERAL # DocMetricAtomHasntString
    | singlyScopedField '=' term=integer # DocMetricAtomHasInt
    | HASINT '(' singlyScopedField ',' term=integer ')' # DocMetricAtomHasInt
    | singlyScopedField '!=' integer # DocMetricAtomHasntInt
    | HASINTFIELD '(' singlyScopedField ')' # DocMetricAtomHasIntField
    | HASSTRFIELD '(' singlyScopedField ')' # DocMetricAtomHasStringField
    | INTTERMCOUNT '(' singlyScopedField ')' # DocMetricAtomIntTermCount
    | STRTERMCOUNT '(' singlyScopedField ')' # DocMetricAtomStrTermCount
    | RANDOM '(' singlyScopedField ',' max=integer (',' seed=STRING_LITERAL)? ')' # DocMetricAtomRandomField
    | RANDOM '(' jqlDocMetric ',' max=integer (',' seed=STRING_LITERAL)? ')' # DocMetricAtomRandomMetric
    | singlyScopedField '=~' regex=STRING_LITERAL # DocMetricAtomRegex
    | FLOATSCALE '(' singlyScopedField (',' mult=number (',' add=number)?)? ')' # DocMetricAtomFloatScale
    | EXTRACT '(' singlyScopedField ',' regex=STRING_LITERAL (',' groupNumber=NAT)? ')' # DocMetricAtomExtract
    | (LUCENE | QUERY) '(' queryField=STRING_LITERAL ')' # DocMetricAtomLucene
    | LEN '(' singlyScopedField ')' # DocMetricAtomLen
    | jqlSyntacticallyAtomicDocMetricAtom # SyntacticallyAtomicDocMetricAtom
    ;

docMetric [boolean useLegacy]
    : {$ctx.useLegacy}? legacyDocMetric
    | {!$ctx.useLegacy}? jqlDocMetric
    ;

docMetricEof [boolean useLegacy]
    : docMetric[$ctx.useLegacy] EOF
    ;

legacyDocMetric
    : COUNT '(' ')' # LegacyDocCounts
    | ABS '(' legacyDocMetric ')' # LegacyDocAbs
    | SIGNUM '(' legacyDocMetric ')' # LegacyDocSignum
    | LOG '(' legacyDocMetric (',' scaleFactor = integer)? ')' # LegacyDocLog
    | EXP '(' legacyDocMetric (',' scaleFactor = integer)? ')' # LegacyDocExp
    | MIN '(' arg1=legacyDocMetric ',' arg2=legacyDocMetric ')' # LegacyDocMin
    | MAX '(' arg1=legacyDocMetric ',' arg2=legacyDocMetric ')' # LegacyDocMax
    | '-' legacyDocMetric # LegacyDocNegate
    | legacyDocMetric (multiply='*'|divide='\\'|modulus='%') legacyDocMetric # LegacyDocMultOrDivideOrModulus
    | legacyDocMetric (plus='+'|minus='-') legacyDocMetric # LegacyDocPlusOrMinus
    | legacyDocMetric (gte='>='|gt='>'|lte='<='|lt='<'|eq='='|neq='!=') legacyDocMetric # LegacyDocInequality
    | '(' legacyDocMetric ')' # LegacyDocMetricParens
    | legacyDocMetricAtom # LegacyDocAtom
    | integer # LegacyDocInt
    ;

jqlDocMetric
    : COUNT '(' ')' # DocCounts
    | ABS '(' jqlDocMetric ')' # DocAbs
    | SIGNUM '(' jqlDocMetric ')' # DocSignum
    | LOG '(' jqlDocMetric (',' scaleFactor = integer)? ')' # DocLog
    | EXP '(' jqlDocMetric (',' scaleFactor = integer)? ')' # DocExp
    | MIN '(' metrics+=jqlDocMetric (',' metrics += jqlDocMetric)* ')' # DocMin
    | MAX '(' metrics+=jqlDocMetric (',' metrics += jqlDocMetric)* ')' # DocMax
    | M '(' jqlDocFilter ')' # DocMetricFilter
    | IF filter=jqlDocFilter THEN trueCase=jqlDocMetric ELSE falseCase=jqlDocMetric # DocIfThenElse
    | '-' jqlDocMetric # DocNegate
    | jqlDocMetric (multiply='*'|divide='/'|modulus='%') jqlDocMetric # DocMultOrDivideOrModulus
    | jqlDocMetric (plus='+'|minus='-') jqlDocMetric # DocPlusOrMinus
    | jqlDocMetric (gte='>='|gt='>'|lte='<='|lt='<'|eq='='|neq='!=') jqlDocMetric # DocInequality
    | '(' jqlDocMetric ')' # DocMetricParens
    | jqlDocMetricAtom # DocAtom
    | integer # DocInt
    | DOCID '(' ')' # DocId
    ;

termVal [boolean useLegacy]
    : {$ctx.useLegacy}? legacyTermVal
    | {!$ctx.useLegacy}? jqlTermVal
    ;

legacyTermVal
    : integer # LegacyIntTerm
    | STRING_LITERAL # LegacyStringTerm
    | identifier # LegacyStringTerm
    ;

jqlTermVal
    : integer # JqlIntTerm
    | STRING_LITERAL # JqlStringTerm
    ;

// DUPLICATION OF docFilter IS A HACK to work around https://github.com/antlr/antlr4/issues/773 , which does not seem fixed
// using master as of 2015-06-10
docFilter [boolean useLegacy]
    : {$ctx.useLegacy}? legacyDocFilter
    | {!$ctx.useLegacy}? jqlDocFilter
    ;

docFilterEof [boolean useLegacy]
    : docFilter[$ctx.useLegacy] EOF
    ;

legacyDocFilter
    : field=identifier '=~' STRING_LITERAL # LegacyDocRegex
    | field=identifier '!=~' STRING_LITERAL # LegacyDocNotRegex
    | field=identifier '=' legacyTermVal # LegacyDocFieldIs
    | (negate='-')? field=identifier ':' legacyTermVal # LegacyDocLuceneFieldIs
    | field=identifier '!=' legacyTermVal # LegacyDocFieldIsnt
    | field=identifier not=NOT? IN '(' (terms += legacyTermVal)? (',' terms += legacyTermVal)* ')' # LegacyDocFieldIn
    | legacyDocMetric op=('='|'!='|'<'|'<='|'>'|'>=') legacyDocMetric # LegacyDocMetricInequality
    | (LUCENE | QUERY) '(' STRING_LITERAL ')' # LegacyLucene
    | BETWEEN '(' field=identifier ',' lowerBound=integer ',' upperBound=integer ')' # LegacyDocBetween
    | SAMPLE '(' field=identifier ',' numerator=NAT (',' denominator=NAT (',' seed=(STRING_LITERAL | NAT))?)? ')' # LegacyDocSample
    | ('!' | '-') legacyDocFilter # LegacyDocNot
    | NOT '(' legacyDocFilter ')' # LegacyDocNot
    | legacyDocFilter (AND|'&&') legacyDocFilter # LegacyDocAnd
    | legacyDocFilter (OR|'||') legacyDocFilter # LegacyDocOr
    | '(' legacyDocFilter ')' # LegacyDocFilterParens
    | TRUE # LegacyDocTrue
    | FALSE # LegacyDocFalse
    ;

jqlDocFilter
    : singlyScopedField '=~' STRING_LITERAL # DocRegex
    | singlyScopedField '!=~' STRING_LITERAL # DocNotRegex
    | singlyScopedField '=' singlyScopedField # DocFieldEqual
    | singlyScopedField '!=' singlyScopedField # DocNotFieldEqual
    | singlyScopedField '=' jqlTermVal # DocFieldIs
    | singlyScopedField '!=' jqlTermVal # DocFieldIsnt
    | singlyScopedField not=NOT? IN '(' (terms += jqlTermVal)? (',' terms += jqlTermVal)* ')' # DocFieldIn
    | singlyScopedField not=NOT? IN '(' queryNoSelect ')' # DocFieldInQuery
    | jqlDocMetric op=('='|'!='|'<'|'<='|'>'|'>=') jqlDocMetric # DocMetricInequality
    | (LUCENE | QUERY) '(' STRING_LITERAL ')' # Lucene
    | BETWEEN '(' singlyScopedField ',' lowerBound=integer ',' upperBound=integer ')' # DocBetween
    | SAMPLE '(' singlyScopedField ',' numerator=NAT (',' denominator=NAT (',' seed=(STRING_LITERAL | NAT))?)? ')' # DocSample
    | SAMPLE '(' jqlDocMetric ',' numerator=NAT (',' denominator=NAT (',' seed=(STRING_LITERAL | NAT))?)? ')' # DocSampleMetric
    | '!' jqlDocFilter # DocNot
    | NOT '(' jqlDocFilter ')' # DocNot
    | jqlDocFilter (AND|'&&') jqlDocFilter # DocAnd
    | jqlDocFilter (OR|'||') jqlDocFilter # DocOr
    | '(' jqlDocFilter ')' # DocFilterParens
    | TRUE # DocTrue
    | FALSE # DocFalse
    ;

groupByEntry [boolean useLegacy]
    : groupByElement[$ctx.useLegacy] ({!$ctx.useLegacy}? HAVING filter=aggregateFilter[$ctx.useLegacy])? ({!$ctx.useLegacy}? AS alias=identifier)?
    ;

groupByElement [boolean useLegacy]
    : DAYOFWEEK (hasParens='(' ')')? # DayOfWeekGroupBy
    | QUANTILES '(' field=identifier ',' NAT ')' # QuantilesGroupBy
    | topTermsGroupByElem[$ctx.useLegacy] # TopTermsGroupBy
    | field=identifier not=NOT? IN '(' (terms += termVal[$ctx.useLegacy])? (',' terms += termVal[$ctx.useLegacy])* ')' (withDefault=WITH DEFAULT)? # GroupByFieldIn
    | field=identifier not=NOT? IN '(' queryNoSelect ')' (withDefault=WITH DEFAULT)? # GroupByFieldInQuery
    | groupByMetric[$ctx.useLegacy] # MetricGroupBy
    | groupByTime[$ctx.useLegacy] # TimeGroupBy
    | groupByField[$ctx.useLegacy] # FieldGroupBy
    | {!$ctx.useLegacy}? DATASET '(' ')' # DatasetGroupBy
    | {!$ctx.useLegacy}? jqlDocFilter # PredicateGroupBy
    | {!$ctx.useLegacy}? RANDOM '(' field=identifier ',' k=NAT (',' salt=STRING_LITERAL)? ')' # RandomGroupBy
    | {!$ctx.useLegacy}? RANDOM '(' jqlDocMetric ',' k=NAT (',' salt=STRING_LITERAL)? ')' # RandomMetricGroupBy
    ;

groupByElementEof [boolean useLegacy]
    : groupByElement[$ctx.useLegacy] EOF
    ;

// TODO: Make TOPTERMS a valid identifier
topTermsGroupByElem [boolean useLegacy]
    : 'TOPTERMS'
        '('
            field=identifier
            (',' limit=NAT
                (',' metric=aggregateMetric[$ctx.useLegacy]
                    (',' order=(BOTTOM | DESCENDING | DESC | TOP | ASCENDING | ASC))?
                )?
            )?
        ')'
    ;

groupByMetric [boolean useLegacy]
    : (BUCKET | BUCKETS) '(' docMetric[$ctx.useLegacy] ',' min=integer ',' max=integer ',' interval=NAT (',' (gutterID=identifier | gutterNumber=number))? ')' (withDefault=WITH DEFAULT)?
    ;

groupByTime [boolean useLegacy]
    : (TIME | ({$ctx.useLegacy}? TIMEBUCKETS)) ('(' (timeBucket (',' timeFormat=(DEFAULT | STRING_LITERAL) (',' timeField=identifier)?)?)? (isRelative=RELATIVE)? ')')?
    ;

groupByField [boolean useLegacy]
    : field=identifier
        (   ('['
                order=(TOP | BOTTOM)?
                limit=NAT?
                (BY metric=aggregateMetric[$ctx.useLegacy])?
                (HAVING filter=aggregateFilter[$ctx.useLegacy])?
             ']'
             (withDefault=WITH DEFAULT)?
            )
          |
            (withDefault=WITH DEFAULT)
        )?
    ;

dateTime
    : DATETIME_TOKEN
    | STRING_LITERAL // unquoted literal must be parseable by dateTimeTerminal or relativeTimeTerminal
    | NAT // This is for unix timestamps.
    | relativeTime
    ;

dateTimeTerminal : dateTime EOF;

aliases
    : ALIASING '(' actual+=identifier AS virtual+=identifier (',' actual+=identifier AS virtual+=identifier)* ')'
    ;

dataset [boolean useLegacy]
    : index=identifier ({!$ctx.useLegacy}? '(' whereContents[$ctx.useLegacy] ')')? start=dateTime end=dateTime (AS name=identifier)? aliases?
    ;

datasetOptTime [boolean useLegacy]
    : dataset[$ctx.useLegacy] # FullDataset
    | index=identifier ({!$ctx.useLegacy}? '(' whereContents[$ctx.useLegacy] ')')? (AS name=identifier)? aliases? # PartialDataset
    ;

fromContents [boolean useLegacy]
    : dataset[$ctx.useLegacy] (',' datasetOptTime[$ctx.useLegacy])*
    ;

whereContents [boolean useLegacy]
    : (docFilters+=docFilter[$ctx.useLegacy])*
    ;

groupByContents [boolean useLegacy]
    : (groupByEntry[$ctx.useLegacy] (',' groupByEntry[$ctx.useLegacy])*)?
    ;

formattedAggregateMetric [boolean useLegacy]
    : aggregateMetric[$ctx.useLegacy]
    | PRINTF '(' STRING_LITERAL ',' aggregateMetric[$ctx.useLegacy] ')'
    ;

selectContents [boolean useLegacy]
    : (formattedAggregateMetric[$ctx.useLegacy] (',' formattedAggregateMetric[$ctx.useLegacy])*)? (ROUNDING precision=NAT)?
    ;

query [boolean useLegacy]
    : (SELECT selects+=selectContents[$ctx.useLegacy])?
      FROM fromContents[$ctx.useLegacy]
      (WHERE whereContents[$ctx.useLegacy])?
      (GROUP BY groupByContents[$ctx.useLegacy])?
      (SELECT selects+=selectContents[$ctx.useLegacy])?
      (OPTIONS '[' (options+=STRING_LITERAL (',' options+=STRING_LITERAL)*)? ']')?
      (LIMIT limit=NAT)?
      (OPTIONS '[' (options+=STRING_LITERAL (',' options+=STRING_LITERAL)*)? ']')?
      EOF
    ;

queryNoSelect
    : FROM (same=SAME | fromContents[false])
      (WHERE whereContents[false])?
      GROUP BY groupByContents[false]
    ;
