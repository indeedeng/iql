grammar JQL;

TIME_UNIT : [smhdwMyb]|'second'|'seconds'|'minute'|'minutes'|'hour'|'hours'|'day'|'days'|'week'|'weeks'|'month'|'months'|'year'|'years'|'bucket'|'buckets' ;

LAG : 'lag' ;
RUNNING : 'running' ;
PARENT : 'parent' ;
DISTINCT : 'distinct' ;
DISTINCT_WINDOW : 'distinct_window' ;
WINDOW : 'window' ;
PERCENTILE : 'percentile' ;
PDIFF : 'pdiff' ;
AVG : 'avg' ;
VARIANCE : 'variance' ;
STDEV : 'stdev' ;
LOG : 'log' ;
ABS : 'abs' ;
SUM_OVER : 'sum_over' ;
AVG_OVER : 'avg_over' ;
WHERE : 'where' ;
HASSTR : 'hasstr' ;
HASINT : 'hasint' ;
SELECT : 'select' ;
FROM : 'from' ;
GROUP : 'group' ;
BY : 'by' ;
AGO : 'ago' ;
COUNT : 'count' ;
AS : 'as' ;
NOT : 'not' ;
LUCENE : 'lucene' ;
QUERY : 'query' ;
TOP : 'top' ;
BOTTOM : 'bottom' ;
WITH : 'with' ;
DEFAULT : 'default' ;
TIME : 'time' ;
TIMEBUCKETS : 'timebuckets' ;
TO : 'to' ;
BUCKETS : 'buckets' ;
BUCKET : 'bucket' ;
IN : 'in' ;
DESCENDING : 'descending' ;
DESC : 'desc' ;
ASCENDING : 'ascending' ;
ASC : 'asc' ;
DAYOFWEEK : 'dayofweek' ;
QUANTILES : 'quantiles' ;
BETWEEN : 'between' ;
SAMPLE : 'sample' ;
AND : 'and' ;
OR : 'or' ;
TRUE : 'true' ;
FALSE : 'false' ;
IF : 'if' ;
THEN : 'then' ;
ELSE : 'else' ;
FLOATSCALE : 'floatscale' ;
SIGNUM : 'signum' ;

ID : [a-zA-Z_][a-zA-Z0-9_]* ;

identifier
    : TIME_UNIT | ID | LAG | RUNNING | PARENT | DISTINCT | DISTINCT_WINDOW | WINDOW | PERCENTILE | PDIFF | AVG
    | VARIANCE | STDEV | LOG | ABS | SUM_OVER | AVG_OVER | WHERE | HASSTR | HASINT | SELECT | FROM | GROUP | BY
    | AGO | COUNT | AS | NOT | LUCENE | QUERY | TOP | BOTTOM | WITH | DEFAULT | TIME | TIMEBUCKETS | TO
    | BUCKETS | BUCKET | IN | DESCENDING | DESC | ASCENDING | ASC | DAYOFWEEK | QUANTILES | BETWEEN
    | SAMPLE | AND | OR | TRUE | FALSE | IF | THEN | ELSE | FLOATSCALE | SIGNUM
    ;
timePeriod : (coeffs+=INT units+=TIME_UNIT)+ AGO?;

INT : [0-9]+ ;
DOUBLE: [0-9]+ ('.' [0-9]*)? ;

fragment DIGIT : [0-9] ;
DATETIME_TOKEN
 : DIGIT DIGIT DIGIT DIGIT
    ('-' DIGIT DIGIT
        ('-' DIGIT DIGIT
            (('T'|' ') DIGIT DIGIT
                (':' DIGIT DIGIT
                    (':' DIGIT DIGIT
                        ('.' DIGIT DIGIT DIGIT)?
                    )?
                )
            )?
        )?
    ) ;
DATE_TOKEN : DIGIT DIGIT DIGIT DIGIT ('-' DIGIT DIGIT ('-' DIGIT DIGIT)?)? ;

WS : [ \t\r\n]+ -> skip ;
COMMENT : '/*' .* '*/' -> skip ;

number : INT | DOUBLE ;


fragment ESCAPED_SINGLE_QUOTE : '\\\'';
fragment SINGLE_QUOTED_CONTENTS : ( ESCAPED_SINGLE_QUOTE | ~('\n'|'\r') )*? ;
fragment SINGLE_QUOTED_STRING : '\'' SINGLE_QUOTED_CONTENTS '\'';

fragment ESCAPED_DOUBLE_QUOTE : '\\"';
fragment DOUBLE_QUOTED_CONTENTS : ( ESCAPED_DOUBLE_QUOTE | ~('\n'|'\r') )*? ;
fragment DOUBLE_QUOTED_STRING : '"' DOUBLE_QUOTED_CONTENTS '"';

STRING_LITERAL : SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING ;

aggregateMetric
    : (COUNT '(' ')') # AggregateCounts
    | LAG '(' INT ',' aggregateMetric ')' # AggregateLag
    | RUNNING '(' aggregateMetric ')' # AggregateRunning
    | PARENT '(' aggregateMetric ')' # AggregateParent
    | DISTINCT '(' identifier (WHERE aggregateFilter)? ')' # AggregateDistinct
    | DISTINCT_WINDOW '(' INT ',' identifier (WHERE aggregateFilter) ')' # AggregateDistinctWindow
    | WINDOW '(' INT ',' aggregateMetric ')' # AggregateWindow
    | PERCENTILE '(' identifier ',' number ')' # AggregatePercentile
    | PDIFF '(' expected=aggregateMetric ',' actual=aggregateMetric ')' # AggregatePDiff
    | AVG '(' aggregateMetric ')' # AggregateAvg
    | VARIANCE '(' docMetric ')' # AggregateVariance
    | STDEV '(' docMetric ')' # AggregateStandardDeviation
    | LOG '(' aggregateMetric ')' # AggregateLog
    | ABS '(' aggregateMetric ')' # AggregateAbs
    | SUM_OVER '(' groupByElement ',' aggregateMetric ')' # AggregateSumAcross
    | AVG_OVER '(' field=identifier (WHERE aggregateFilter)? ',' aggregateMetric ')' # AggregateAverageAcross
    | scope ':' '(' aggregateMetric ')' # AggregateQualified
    | identifier # AggregateRawField
    | '[' docMetric ']' # AggregateSum
    | '-' aggregateMetric # AggregateNegate
    | <assoc=right> aggregateMetric '^' aggregateMetric # AggregatePower
    | aggregateMetric '*' aggregateMetric # AggregateMult
    | aggregateMetric '/' aggregateMetric # AggregateDiv
    | aggregateMetric '%' aggregateMetric # AggregateMod
    | aggregateMetric '+' aggregateMetric # AggregatePlus
    | aggregateMetric '-' aggregateMetric # AggregateMinus
    | '(' aggregateMetric ')' # AggregateParens
    | number # AggregateConstant
    | aggregateMetric AS name=identifier # AggregateNamed
    ;

scope : '[' datasets+=identifier (',' datasets+=identifier)* ']' ;

aggregateFilter
    : field=identifier '=~' STRING_LITERAL # AggregateRegex
    | field=identifier '!=~' STRING_LITERAL # AggregateNotRegex
    | 'term()' '=' termVal # AggregateTermIs
    | aggregateMetric op=('='|'!='|'<'|'<='|'>'|'>=') aggregateMetric # AggregateMetricInequality
    | (NOT|'-'|'!') aggregateFilter # AggregateNot
    | aggregateFilter ('and' | '&&') aggregateFilter # AggregateAnd
    | aggregateFilter ('or' | '||') aggregateFilter # AggregateOr
    | '(' aggregateFilter ')' # AggregateFilterParens
    | TRUE # AggregateTrue
    | FALSE # AggregateFalse
    ;

docMetric
    : COUNT '(' ')' # DocCounts
    | ABS '(' docMetric ')' # DocAbs
    | SIGNUM '(' docMetric ')' # DocSignum
    /* TODO: identifier */
    | field=identifier ('='|':') term=(STRING_LITERAL | ID | TIME_UNIT | INT) # DocHasString
    /* TODO: identifier */
    | HASSTR '(' field=identifier ',' term=(STRING_LITERAL | ID | TIME_UNIT | INT) ')' # DocHasString
    | HASSTR '(' STRING_LITERAL ')' # DocHasStringQuoted
    /* TODO: identifier */
    | field=identifier '!=' term=(STRING_LITERAL | ID | TIME_UNIT) # DocHasntString
    | field=identifier ('='|':') term=INT # DocHasInt
    | HASINT '(' field=identifier ',' term=INT ')' # DocHasInt
    | HASINT '(' STRING_LITERAL ')' # DocHasIntQuoted
    | field=identifier '!=' INT # DocHasntInt
    | FLOATSCALE '(' field=identifier ',' mult=INT ',' add=INT ')' # DocFloatScale
    | IF filter=docFilter THEN trueCase=docMetric ELSE falseCase=docMetric # DocIfThenElse
    | '-' docMetric # DocNegate
    | docMetric '*' docMetric # DocMult
    | docMetric '/' docMetric # DocDiv
    | docMetric '%' docMetric # DocMod
    | docMetric '+' docMetric # DocPlus
    | docMetric '-' docMetric # DocMinus
    | '(' docMetric ')' # DocMetricParens
    | identifier # DocRawField
    | INT # DocInt
    ;

termVal
    : INT # IntTerm
    | STRING_LITERAL # StringTerm
    | identifier # StringTerm
    ;

docFilter
    : field=identifier '=~' STRING_LITERAL # DocRegex
    | field=identifier '!=~' STRING_LITERAL # DocNotRegex
    | field=identifier ('='|':') termVal # DocFieldIs
    | field=identifier '!=' termVal # DocFieldIsnt
    | field=identifier not=NOT? IN '(' (terms += termVal)? (',' terms += termVal)* ')' # DocFieldIn
    | docMetric op=('='|'!='|'<'|'<='|'>'|'>=') docMetric # DocMetricInequality
    | (LUCENE | QUERY) '(' STRING_LITERAL ')' # Lucene
    | BETWEEN '(' field=identifier ',' lowerBound=INT ',' upperBound=INT ')' # DocBetween
    | SAMPLE '(' field=identifier ',' numerator=INT (',' denominator=INT (',' seed=(STRING_LITERAL | INT))?)? ')' # DocSample
    | ('-'|'!'|NOT) docFilter # DocNot
    | docFilter (AND|'&&') docFilter # DocAnd
    | docFilter (OR|'||') docFilter # DocOr
    | '(' docFilter ')' # DocFilterParens
    | TRUE # DocTrue
    | FALSE # DocFalse
    ;

groupByElement
    : DAYOFWEEK # DayOfWeekGroupBy
    | QUANTILES '(' field=identifier ',' INT ')' # QuantilesGroupBy
    | topTermsGroupByElem # TopTermsGroupBy
    | field=identifier not=NOT? IN '(' (terms += termVal)? (',' terms += termVal)* ')' (withDefault=WITH DEFAULT)? # GroupByFieldIn
    | groupByMetric # MetricGroupBy
    | groupByMetricEnglish # MetricGroupBy
    | groupByTime # TimeGroupBy
    | groupByField # FieldGroupBy
    ;

topTermsGroupByElem
    : 'topterms'
        '('
            field=identifier
            (',' limit=INT
                (',' metric=aggregateMetric
                    (',' order=(BOTTOM | DESCENDING | DESC | TOP | ASCENDING | ASC))?
                )?
            )?
        ')'
    ;

groupByMetric
    : (BUCKETS | BUCKET) '(' docMetric ',' min=INT ',' max=INT ',' interval=INT ')'
    ;

groupByMetricEnglish
    : docMetric FROM min=INT TO max=INT BY interval=INT
    ;

groupByTime
    : (TIME | TIMEBUCKETS) ('(' timePeriod (',' timeField=identifier)? ')')?
    ;

groupByField
    : field=identifier ('[' order=(TOP | BOTTOM)? limit=INT? (BY metric=aggregateMetric)? (WHERE filter=aggregateFilter)? ']')? (withDefault=WITH DEFAULT)?
    ;

dateTime
    : DATETIME_TOKEN
    | DATE_TOKEN
    | STRING_LITERAL
    | timePeriod
    // Oh god I hate myself:
    | 'today'
    | 'toda'
    | 'tod'
    | 'tomorrow'
    | 'tomorro'
    | 'tomorr'
    | 'tomor'
    | 'tomo'
    | 'tom'
    | 'yesterday'
    | 'yesterda'
    | 'yesterd'
    | 'yester'
    | 'yeste'
    | 'yest'
    | 'yes'
    | 'ye'
    | 'y'
    ;

dataset
    : index=identifier start=dateTime end=dateTime (AS name=identifier)?
    ;

datasetOptTime
    : dataset # FullDataset
    | index=identifier (AS name=identifier)? # PartialDataset
    ;

fromContents
    : dataset (',' datasetOptTime)*
    ;

groupByContents
    : (groupByElement (',' groupByElement)*)?
    ;

selectContents
    : (aggregateMetric (',' aggregateMetric)*)?
    ;

query
    : (SELECT selects+=selectContents)?
      FROM fromContents
      (WHERE docFilter+)?
      (GROUP BY groupByContents)?
      (SELECT selects+=selectContents)?
    ;