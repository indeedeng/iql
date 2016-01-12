grammar JQL;

LAG : 'LAG' ;
RUNNING : 'RUNNING' ;
PARENT : 'PARENT' ;
DISTINCT : 'DISTINCT' ;
DISTINCT_WINDOW : 'DISTINCT_WINDOW' ;
WINDOW : 'WINDOW' ;
PERCENTILE : 'PERCENTILE' ;
PDIFF : 'PDIFF' ;
AVG : 'AVG' ;
VARIANCE : 'VARIANCE' ;
STDEV : 'STDEV' ;
LOG : 'LOG' ;
ABS : 'ABS' ;
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
TO : 'TO' ;
BUCKETS : 'BUCKETS' ;
BUCKET : 'BUCKET' ;
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
SAME : 'SAME' ;
EXP : 'EXP' ;
WINDOW_SUM : 'WINDOW_SUM' ;

Y : 'Y' ;

TIME_UNIT : [SMHDWYB]|'SECOND'|'SECONDS'|'MINUTE'|'MINUTES'|'HOUR'|'HOURS'|'DAY'|'DAYS'|'WEEK'|'WEEKS'|'MO'|'MONTH'|'MONTHS'|'YEAR'|'YEARS';

TIME_PERIOD_ATOM : ([0-9]+ (TIME_UNIT|BUCKET|BUCKETS|[sSmMhHdDwWyYbB]))+ ;

NAT : [0-9]+ ;
DOUBLE: [0-9]+ ('.' [0-9]*)? ;

fragment DIGIT : [0-9] ;
DATETIME_TOKEN
 : DIGIT DIGIT DIGIT DIGIT
    ('-' DIGIT DIGIT
        ('-' DIGIT DIGIT
            (('T'|' ') DIGIT DIGIT
                (':' DIGIT DIGIT
                    (':' DIGIT DIGIT
                        ('.' DIGIT DIGIT DIGIT
                            (('+'|'-') DIGIT DIGIT ':' DIGIT DIGIT)?
                        )?
                    )?
                )
            )?
        )?
    ) ;
DATE_TOKEN : DIGIT DIGIT DIGIT DIGIT ('-' DIGIT DIGIT ('-' DIGIT DIGIT)?)? ;

ID : [a-zA-Z_][a-zA-Z0-9_]* ;

// TODO: How to keep this up to date with new lexer tokens..?
identifier
    : TIME_UNIT | Y | ID | LAG | RUNNING | PARENT | DISTINCT | DISTINCT_WINDOW | WINDOW | PERCENTILE | PDIFF | AVG
    | VARIANCE | STDEV | LOG | ABS | SUM_OVER | AVG_OVER | WHERE | HASSTR | HASINT | SELECT | FROM | GROUP | BY
    | AGO | COUNT | AS | NOT | LUCENE | QUERY | TOP | BOTTOM | WITH | DEFAULT | TIME | TIMEBUCKETS | TO
    | BUCKETS | BUCKET | IN | DESCENDING | DESC | ASCENDING | ASC | DAYOFWEEK | QUANTILES | BETWEEN
    | SAMPLE | AND | OR | TRUE | FALSE | IF | THEN | ELSE | FLOATSCALE | SIGNUM | LIMIT | HAVING
    | FIELD_MIN | FIELD_MAX | ALIASING | HASINTFIELD | HASSTRFIELD | SAME | EXP | WINDOW_SUM
    ;
timePeriod : (atoms+=TIME_PERIOD_ATOM | (coeffs+=NAT units+=(TIME_UNIT | Y | BUCKET | BUCKETS)))+ AGO? #TimePeriodParseable
           | STRING_LITERAL # TimePeriodStringLiteral ;
timePeriodTerminal : timePeriod EOF ;

WS : [ \t\r\n]+ -> channel(HIDDEN) ;
COMMENT : '/*' .*? '*/' -> channel(HIDDEN) ;
LINE_COMMENT : '--' .*? ~[\r\n]* -> channel(HIDDEN) ;

integer : neg='-'? NAT ;

number : NAT | DOUBLE ;


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
    | PDIFF '(' expected=jqlAggregateMetric ',' actual=jqlAggregateMetric ')' # AggregatePDiff
    | AVG '(' jqlAggregateMetric ')' # AggregateAvg
    | VARIANCE '(' jqlDocMetric ')' # AggregateVariance
    | STDEV '(' jqlDocMetric ')' # AggregateStandardDeviation
    | LOG '(' jqlAggregateMetric ')' # AggregateLog
    | ABS '(' jqlAggregateMetric ')' # AggregateAbs
    | FIELD_MIN '(' scopedField ')' # AggregateFieldMin
    | FIELD_MAX '(' scopedField ')' # AggregateFieldMax
    | SUM_OVER '(' groupByElement[false] ',' jqlAggregateMetric ')' # AggregateSumAcross
    | AVG_OVER '(' field=scopedField ((havingBrackets='[' HAVING jqlAggregateFilter ']')|(HAVING jqlAggregateFilter))? ',' jqlAggregateMetric ')' # AggregateAverageAcross
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

syntacticallyAtomicJqlAggregateMetric
    : (COUNT '(' ')') # AggregateCounts
    | number # AggregateConstant
    | jqlSyntacticallyAtomicDocMetricAtom # AggregateDocMetricAtom2
    ;

aggregateFilter [boolean useLegacy]
    : {$ctx.useLegacy}? {false}? // No such thing
    | {!$ctx.useLegacy}? jqlAggregateFilter
    ;

jqlAggregateFilter
    : field=identifier '=~' STRING_LITERAL # AggregateRegex
    | field=identifier '!=~' STRING_LITERAL # AggregateNotRegex
    | 'TERM()' '=' jqlTermVal # AggregateTermIs
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
    : field=identifier '=' term=(STRING_LITERAL | ID | TIME_UNIT) # LegacyDocMetricAtomHasString
    | HASSTR '(' field=identifier ',' term=(STRING_LITERAL | ID | TIME_UNIT) ')' # LegacyDocMetricAtomHasString
    | field=identifier '!=' term=(STRING_LITERAL | ID | TIME_UNIT) # LegacyDocMetricAtomHasntString
    | field=identifier '=' term=integer # LegacyDocMetricAtomHasInt
    | HASINT '(' field=identifier ',' term=integer ')' # LegacyDocMetricAtomHasInt
    | field=identifier '!=' integer # LegacyDocMetricAtomHasntInt
    | HASSTR '(' STRING_LITERAL ')' # LegacyDocMetricAtomHasStringQuoted
    | HASINT '(' STRING_LITERAL ')' # LegacyDocMetricAtomHasIntQuoted
    | FLOATSCALE '(' field=identifier ',' mult=number ',' add=number ')' # LegacyDocMetricAtomFloatScale
    | identifier # LegacyDocMetricAtomRawField
    ;

jqlDocMetricAtom
    : singlyScopedField '=' term=STRING_LITERAL # DocMetricAtomHasString
    | HASSTR '(' singlyScopedField ',' term=STRING_LITERAL ')' # DocMetricAtomHasString
    | singlyScopedField '!=' term=STRING_LITERAL # DocMetricAtomHasntString
    | singlyScopedField '=' term=integer # DocMetricAtomHasInt
    | HASINT '(' singlyScopedField ',' term=integer ')' # DocMetricAtomHasInt
    | singlyScopedField '!=' integer # DocMetricAtomHasntInt
    | HASINTFIELD '(' singlyScopedField ')' # DocMetricAtomHasIntField
    | HASSTRFIELD '(' singlyScopedField ')' # DocMetricAtomHasStringField
    | FLOATSCALE '(' singlyScopedField ',' mult=number ',' add=number ')' # DocMetricAtomFloatScale
    | jqlSyntacticallyAtomicDocMetricAtom # SyntacticallyAtomicDocMetricAtom
    ;

docMetric [boolean useLegacy]
    : {$ctx.useLegacy}? legacyDocMetric
    | {!$ctx.useLegacy}? jqlDocMetric
    ;

legacyDocMetric
    : COUNT '(' ')' # LegacyDocCounts
    | ABS '(' legacyDocMetric ')' # LegacyDocAbs
    | SIGNUM '(' legacyDocMetric ')' # LegacyDocSignum
    | LOG '(' legacyDocMetric (',' scaleFactor = integer)? ')' # LegacyDocLog
    | EXP '(' legacyDocMetric (',' scaleFactor = integer)? ')' # LegacyDocExp
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
    | IF filter=jqlDocFilter THEN trueCase=jqlDocMetric ELSE falseCase=jqlDocMetric # DocIfThenElse
    | '-' jqlDocMetric # DocNegate
    | jqlDocMetric (multiply='*'|divide='/'|modulus='%') jqlDocMetric # DocMultOrDivideOrModulus
    | jqlDocMetric (plus='+'|minus='-') jqlDocMetric # DocPlusOrMinus
    | jqlDocMetric (gte='>='|gt='>'|lte='<='|lt='<'|eq='='|neq='!=') jqlDocMetric # DocInequality
    | '(' jqlDocMetric ')' # DocMetricParens
    | jqlDocMetricAtom # DocAtom
    | integer # DocInt
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
    | '!' legacyDocFilter # LegacyDocNot
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
    | singlyScopedField '=' jqlTermVal # DocFieldIs
    | singlyScopedField '!=' jqlTermVal # DocFieldIsnt
    | singlyScopedField not=NOT? IN '(' (terms += jqlTermVal)? (',' terms += jqlTermVal)* ')' # DocFieldIn
    | singlyScopedField not=NOT? IN '(' queryNoSelect ')' # DocFieldInQuery
    | jqlDocMetric op=('='|'!='|'<'|'<='|'>'|'>=') jqlDocMetric # DocMetricInequality
    | (LUCENE | QUERY) '(' STRING_LITERAL ')' # Lucene
    | BETWEEN '(' singlyScopedField ',' lowerBound=integer ',' upperBound=integer ')' # DocBetween
    | SAMPLE '(' singlyScopedField ',' numerator=NAT (',' denominator=NAT (',' seed=(STRING_LITERAL | NAT))?)? ')' # DocSample
    | '!' jqlDocFilter # DocNot
    | NOT '(' jqlDocFilter ')' # DocNot
    | jqlDocFilter (AND|'&&') jqlDocFilter # DocAnd
    | jqlDocFilter (OR|'||') jqlDocFilter # DocOr
    | '(' jqlDocFilter ')' # DocFilterParens
    | TRUE # DocTrue
    | FALSE # DocFalse
    ;

groupByElementWithHaving [boolean useLegacy]
    : groupByElement[$ctx.useLegacy] ({!$ctx.useLegacy}? HAVING filter=aggregateFilter[$ctx.useLegacy])?
    ;

groupByElement [boolean useLegacy]
    : DAYOFWEEK (hasParens='(' ')')? # DayOfWeekGroupBy
    | QUANTILES '(' field=identifier ',' NAT ')' # QuantilesGroupBy
    | topTermsGroupByElem[$ctx.useLegacy] # TopTermsGroupBy
    | field=identifier not=NOT? IN '(' (terms += termVal[$ctx.useLegacy])? (',' terms += termVal[$ctx.useLegacy])* ')' (withDefault=WITH DEFAULT)? # GroupByFieldIn
    | groupByMetric[$ctx.useLegacy] # MetricGroupBy
    | groupByMetricEnglish[$ctx.useLegacy] # MetricGroupBy
    | groupByTime[$ctx.useLegacy] # TimeGroupBy
    | groupByField[$ctx.useLegacy] # FieldGroupBy
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

groupByMetricEnglish [boolean useLegacy]
    : docMetric[$ctx.useLegacy] FROM min=integer TO max=integer BY interval=NAT (withDefault=WITH DEFAULT)?
    ;

groupByTime [boolean useLegacy]
    : (TIME | ({$ctx.useLegacy}? TIMEBUCKETS)) ('(' (timePeriod (',' timeFormat=(DEFAULT | STRING_LITERAL) (',' timeField=identifier)?)?)? ')')?
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
            (forceNonStreaming='*')
          |
            (withDefault=WITH DEFAULT)
        )?
    ;

dateTime
    : DATETIME_TOKEN
    | DATE_TOKEN
    | STRING_LITERAL
    | NAT // This is for unix timestamps.
    | timePeriod
    // Oh god I hate myself:
    | 'TODAY'
    | 'TODA'
    | 'TOD'
    | 'TOMORROW'
    | 'TOMORRO'
    | 'TOMORR'
    | 'TOMOR'
    | 'TOMO'
    | 'TOM'
    | 'YESTERDAY'
    | 'YESTERDA'
    | 'YESTERD'
    | 'YESTER'
    | 'YESTE'
    | 'YEST'
    | 'YES'
    | 'YE'
    | Y
    ;

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
    : (docFilters+=docFilter[$ctx.useLegacy])+
    ;

groupByContents [boolean useLegacy]
    : (groupByElementWithHaving[$ctx.useLegacy] (',' groupByElementWithHaving[$ctx.useLegacy])*)?
    ;

selectContents [boolean useLegacy]
    : (aggregateMetric[$ctx.useLegacy] (',' aggregateMetric[$ctx.useLegacy])*)?
    ;

query [boolean useLegacy]
    : (SELECT selects+=selectContents[$ctx.useLegacy])?
      FROM fromContents[$ctx.useLegacy]
      (WHERE whereContents[$ctx.useLegacy])?
      (GROUP BY groupByContents[$ctx.useLegacy])?
      (SELECT selects+=selectContents[$ctx.useLegacy])?
      (LIMIT limit=NAT)?
      EOF
    ;

queryNoSelect
    : FROM (same=SAME | fromContents[false])
      (WHERE whereContents[false])?
      GROUP BY groupByContents[false]
    ;