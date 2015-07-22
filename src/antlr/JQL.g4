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

Y : 'Y' ;

TIME_UNIT : [SMHDWYB]|'SECOND'|'SECONDS'|'MINUTE'|'MINUTES'|'HOUR'|'HOURS'|'DAY'|'DAYS'|'WEEK'|'WEEKS'|'MO'|'MONTH'|'MONTHS'|'YEAR'|'YEARS';

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

identifier
    : TIME_UNIT | Y | ID | LAG | RUNNING | PARENT | DISTINCT | DISTINCT_WINDOW | WINDOW | PERCENTILE | PDIFF | AVG
    | VARIANCE | STDEV | LOG | ABS | SUM_OVER | AVG_OVER | WHERE | HASSTR | HASINT | SELECT | FROM | GROUP | BY
    | AGO | COUNT | AS | NOT | LUCENE | QUERY | TOP | BOTTOM | WITH | DEFAULT | TIME | TIMEBUCKETS | TO
    | BUCKETS | BUCKET | IN | DESCENDING | DESC | ASCENDING | ASC | DAYOFWEEK | QUANTILES | BETWEEN
    | SAMPLE | AND | OR | TRUE | FALSE | IF | THEN | ELSE | FLOATSCALE | SIGNUM | LIMIT | HAVING
    ;
timePeriod : (coeffs+=INT units+=(TIME_UNIT | Y | BUCKET | BUCKETS))+ AGO? #TimePeriodParseable
           | STRING_LITERAL # TimePeriodStringLiteral ;

WS : [ \t\r\n]+ -> channel(HIDDEN) ;
COMMENT : '/*' .*? '*/' -> channel(HIDDEN) ;

number : INT | DOUBLE ;


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

// THIS IS A HACK to work around https://github.com/antlr/antlr4/issues/773
jqlSumOverMetric : SUM_OVER '(' groupByElement[false] ',' jqlAggregateMetric ')' ;

jqlAggregateMetric
    : (COUNT '(' ')') # AggregateCounts
    | LAG '(' INT ',' jqlAggregateMetric ')' # AggregateLag
    | RUNNING '(' jqlAggregateMetric ')' # AggregateRunning
    | PARENT '(' jqlAggregateMetric ')' # AggregateParent
    | DISTINCT '(' identifier (HAVING jqlAggregateFilter)? ')' # AggregateDistinct
    | DISTINCT_WINDOW '(' INT ',' identifier (HAVING jqlAggregateFilter) ')' # AggregateDistinctWindow
    | WINDOW '(' INT ',' jqlAggregateMetric ')' # AggregateWindow
    | PERCENTILE '(' identifier ',' number ')' # AggregatePercentile
    | PDIFF '(' expected=jqlAggregateMetric ',' actual=jqlAggregateMetric ')' # AggregatePDiff
    | AVG '(' jqlAggregateMetric ')' # AggregateAvg
    | VARIANCE '(' jqlDocMetric ')' # AggregateVariance
    | STDEV '(' jqlDocMetric ')' # AggregateStandardDeviation
    | LOG '(' jqlAggregateMetric ')' # AggregateLog
    | ABS '(' jqlAggregateMetric ')' # AggregateAbs
    | jqlSumOverMetric # AggregateSumAcross
    | AVG_OVER '(' field=identifier ('[' HAVING jqlAggregateFilter ']')? ',' jqlAggregateMetric ')' # AggregateAverageAcross
    | scope ':' '(' jqlAggregateMetric ')' # AggregateQualified
    | docMetricAtom # AggregateDocMetricAtom
    | '[' jqlDocMetric ']' # AggregateSum
    | '-' jqlAggregateMetric # AggregateNegate
    | <assoc=right> jqlAggregateMetric '^' jqlAggregateMetric # AggregatePower
    | jqlAggregateMetric '*' jqlAggregateMetric # AggregateMult
    | jqlAggregateMetric '/' jqlAggregateMetric # AggregateDiv
    | jqlAggregateMetric '%' jqlAggregateMetric # AggregateMod
    | jqlAggregateMetric '+' jqlAggregateMetric # AggregatePlus
    | jqlAggregateMetric '-' jqlAggregateMetric # AggregateMinus
    | '(' jqlAggregateMetric ')' # AggregateParens
    | number # AggregateConstant
    | jqlAggregateMetric AS name=identifier # AggregateNamed
    ;

scope : '[' datasets+=identifier (',' datasets+=identifier)* ']' # MultiScope
      | identifier # SingleScope
      ;

aggregateFilter [boolean useLegacy]
    : {$ctx.useLegacy}? {false}? // No such thing
    | {!$ctx.useLegacy}? jqlAggregateFilter
    ;

jqlAggregateFilter
    : field=identifier '=~' STRING_LITERAL # AggregateRegex
    | field=identifier '!=~' STRING_LITERAL # AggregateNotRegex
    | 'TERM()' '=' termVal # AggregateTermIs
    | jqlAggregateMetric op=('='|'!='|'<'|'<='|'>'|'>=') jqlAggregateMetric # AggregateMetricInequality
    | '!' jqlAggregateFilter # AggregateNot
    | NOT '(' jqlAggregateFilter ')' # AggregateNot
    | jqlAggregateFilter (AND | '&&') jqlAggregateFilter # AggregateAnd
    | jqlAggregateFilter (OR | '||') jqlAggregateFilter # AggregateOr
    | '(' jqlAggregateFilter ')' # AggregateFilterParens
    | TRUE # AggregateTrue
    | FALSE # AggregateFalse
    ;

docMetricAtom
    /* TODO: identifier */
    : field=identifier '=' term=(STRING_LITERAL | ID | TIME_UNIT) # DocMetricAtomHasString
    /* TODO: identifier */
    | HASSTR '(' field=identifier ',' term=(STRING_LITERAL | ID | TIME_UNIT) ')' # DocMetricAtomHasString
    | HASSTR '(' STRING_LITERAL ')' # DocMetricAtomHasStringQuoted
    /* TODO: identifier */
    | field=identifier '!=' term=(STRING_LITERAL | ID | TIME_UNIT) # DocMetricAtomHasntString
    | field=identifier '=' term=INT # DocMetricAtomHasInt
    | HASINT '(' field=identifier ',' term=INT ')' # DocMetricAtomHasInt
    | HASINT '(' STRING_LITERAL ')' # DocMetricAtomHasIntQuoted
    | field=identifier '!=' INT # DocMetricAtomHasntInt
    | FLOATSCALE '(' field=identifier ',' mult=INT ',' add=INT ')' # DocMetricAtomFloatScale
    | identifier # DocMetricAtomRawField
    ;

docMetric [boolean useLegacy]
    : {$ctx.useLegacy}? legacyDocMetric
    | {!$ctx.useLegacy}? jqlDocMetric
    ;

legacyDocMetric
    : COUNT '(' ')' # LegacyDocCounts
    | ABS '(' legacyDocMetric ')' # LegacyDocAbs
    | SIGNUM '(' legacyDocMetric ')' # LegacyDocSignum
    | '-' legacyDocMetric # LegacyDocNegate
    | legacyDocMetric '*' legacyDocMetric # LegacyDocMult
    | legacyDocMetric '\\' legacyDocMetric # LegacyDocDiv
    | legacyDocMetric '%' legacyDocMetric # LegacyDocMod
    | legacyDocMetric '+' legacyDocMetric # LegacyDocPlus
    | legacyDocMetric '-' legacyDocMetric # LegacyDocMinus
    | legacyDocMetric '>=' legacyDocMetric # LegacyDocGTE
    | legacyDocMetric '>' legacyDocMetric # LegacyDocGT
    | legacyDocMetric '<=' legacyDocMetric # LegacyDocLTE
    | legacyDocMetric '<' legacyDocMetric # LegacyDocLT
    | legacyDocMetric '=' legacyDocMetric # LegacyDocEQ
    | legacyDocMetric '!=' legacyDocMetric # LegacyDocNEQ
    | '(' legacyDocMetric ')' # LegacyDocMetricParens
    | docMetricAtom # LegacyDocAtom
    | INT # LegacyDocInt
    ;

jqlDocMetric
    : COUNT '(' ')' # DocCounts
    | ABS '(' jqlDocMetric ')' # DocAbs
    | SIGNUM '(' jqlDocMetric ')' # DocSignum
    | IF filter=jqlDocFilter THEN trueCase=jqlDocMetric ELSE falseCase=jqlDocMetric # DocIfThenElse
    | '-' jqlDocMetric # DocNegate
    | jqlDocMetric '*' jqlDocMetric # DocMult
    | jqlDocMetric '/' jqlDocMetric # DocDiv
    | jqlDocMetric '%' jqlDocMetric # DocMod
    | jqlDocMetric '+' jqlDocMetric # DocPlus
    | jqlDocMetric '-' jqlDocMetric # DocMinus
    | jqlDocMetric '>=' jqlDocMetric # DocGTE
    | jqlDocMetric '>' jqlDocMetric # DocGT
    | jqlDocMetric '<=' jqlDocMetric # DocLTE
    | jqlDocMetric '<' jqlDocMetric # DocLT
    | jqlDocMetric '=' jqlDocMetric # DocEQ
    | jqlDocMetric '!=' jqlDocMetric # DocNEQ
    | '(' jqlDocMetric ')' # DocMetricParens
    | docMetricAtom # DocAtom
    | INT # DocInt
    ;

termVal
    : INT # IntTerm
    | STRING_LITERAL # StringTerm
    | identifier # StringTerm
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
    | field=identifier '=' termVal # LegacyDocFieldIs
    | (negate='-')? field=identifier ':' termVal # LegacyDocLuceneFieldIs
    | field=identifier '!=' termVal # LegacyDocFieldIsnt
    | field=identifier not=NOT? IN '(' (terms += termVal)? (',' terms += termVal)* ')' # LegacyDocFieldIn
    | legacyDocMetric op=('='|'!='|'<'|'<='|'>'|'>=') legacyDocMetric # LegacyDocMetricInequality
    | (LUCENE | QUERY) '(' STRING_LITERAL ')' # LegacyLucene
    | BETWEEN '(' field=identifier ',' lowerBound=INT ',' upperBound=INT ')' # LegacyDocBetween
    | SAMPLE '(' field=identifier ',' numerator=INT (',' denominator=INT (',' seed=(STRING_LITERAL | INT))?)? ')' # LegacyDocSample
    | '!' legacyDocFilter # LegacyDocNot
    | NOT '(' legacyDocFilter ')' # LegacyDocNot
    | legacyDocFilter (AND|'&&') legacyDocFilter # LegacyDocAnd
    | legacyDocFilter (OR|'||') legacyDocFilter # LegacyDocOr
    | '(' legacyDocFilter ')' # LegacyDocFilterParens
    | TRUE # LegacyDocTrue
    | FALSE # LegacyDocFalse
    ;

jqlDocFilter
    : field=identifier '=~' STRING_LITERAL # DocRegex
    | field=identifier '!=~' STRING_LITERAL # DocNotRegex
    | field=identifier '=' termVal # DocFieldIs
    | (negate='-')? field=identifier ':' termVal # DocLuceneFieldIs
    | field=identifier '!=' termVal # DocFieldIsnt
    | field=identifier not=NOT? IN '(' (terms += termVal)? (',' terms += termVal)* ')' # DocFieldIn
    | jqlDocMetric op=('='|'!='|'<'|'<='|'>'|'>=') jqlDocMetric # DocMetricInequality
    | (LUCENE | QUERY) '(' STRING_LITERAL ')' # Lucene
    | BETWEEN '(' field=identifier ',' lowerBound=INT ',' upperBound=INT ')' # DocBetween
    | SAMPLE '(' field=identifier ',' numerator=INT (',' denominator=INT (',' seed=(STRING_LITERAL | INT))?)? ')' # DocSample
    | '!' jqlDocFilter # DocNot
    | NOT '(' jqlDocFilter ')' # DocNot
    | jqlDocFilter (AND|'&&') jqlDocFilter # DocAnd
    | jqlDocFilter (OR|'||') jqlDocFilter # DocOr
    | '(' jqlDocFilter ')' # DocFilterParens
    | TRUE # DocTrue
    | FALSE # DocFalse
    ;

groupByElement [boolean useLegacy]
    : DAYOFWEEK # DayOfWeekGroupBy
    | QUANTILES '(' field=identifier ',' INT ')' # QuantilesGroupBy
    | topTermsGroupByElem[$ctx.useLegacy] # TopTermsGroupBy
    | field=identifier not=NOT? IN '(' (terms += termVal)? (',' terms += termVal)* ')' (withDefault=WITH DEFAULT)? # GroupByFieldIn
    | groupByMetric[$ctx.useLegacy] # MetricGroupBy
    | groupByMetricEnglish[$ctx.useLegacy] # MetricGroupBy
    | groupByTime # TimeGroupBy
    | groupByField[$ctx.useLegacy] # FieldGroupBy
    ;

topTermsGroupByElem [boolean useLegacy]
    : 'TOPTERMS'
        '('
            field=identifier
            (',' limit=INT
                (',' metric=aggregateMetric[$ctx.useLegacy]
                    (',' order=(BOTTOM | DESCENDING | DESC | TOP | ASCENDING | ASC))?
                )?
            )?
        ')'
    ;

groupByMetric [boolean useLegacy]
    : (BUCKETS | BUCKET) '(' docMetric[$ctx.useLegacy] ',' min=INT ',' max=INT ',' interval=INT (',' (gutterID=identifier | gutterNumber=number))? ')'
    ;

groupByMetricEnglish [boolean useLegacy]
    : docMetric[$ctx.useLegacy] FROM min=INT TO max=INT BY interval=INT
    ;

groupByTime
    : (TIME | TIMEBUCKETS) ('(' (timePeriod (',' timeFormat=(DEFAULT | STRING_LITERAL) (',' timeField=identifier)?)?)? ')')?
    ;

groupByField [boolean useLegacy]
    : field=identifier
        (   ('['
                order=(TOP | BOTTOM)?
                limit=INT?
                (BY metric=aggregateMetric[$ctx.useLegacy])?
                (HAVING filter=aggregateFilter[$ctx.useLegacy])?
             ']'
             (withDefault=WITH DEFAULT)?
            )
          |
            (forceNonStreaming='*')
        )?
    ;

dateTime
    : DATETIME_TOKEN
    | DATE_TOKEN
    | STRING_LITERAL
    | INT // This is for unix timestamps.
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

whereContents [boolean useLegacy]
    : (docFilters+=docFilter[$ctx.useLegacy])+
    ;

groupByContents [boolean useLegacy]
    : (groupByElement[$ctx.useLegacy] (',' groupByElement[$ctx.useLegacy])*)?
    ;

selectContents [boolean useLegacy]
    : (aggregateMetric[$ctx.useLegacy] (',' aggregateMetric[$ctx.useLegacy])*)?
    ;

query [boolean useLegacy]
    : (SELECT selects+=selectContents[$ctx.useLegacy])?
      FROM fromContents
      (WHERE whereContents[$ctx.useLegacy])?
      (GROUP BY groupByContents[$ctx.useLegacy])?
      (SELECT selects+=selectContents[$ctx.useLegacy])?
      (LIMIT limit=INT)?
    ;