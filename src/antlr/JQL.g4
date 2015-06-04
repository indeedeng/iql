grammar JQL;

r : 'hello' identifier ;

TIME_UNIT : [smhdwMyb]|'second'|'seconds'|'minute'|'minutes'|'hour'|'hours'|'day'|'days'|'week'|'weeks'|'month'|'months'|'year'|'years'|'bucket'|'buckets' ;
ID : [a-zA-Z_][a-zA-Z0-9_]* ;

identifier : TIME_UNIT | ID ;

timePeriod : (coeffs+=INT units+=TIME_UNIT)+ 'ago'?;

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
                )?
            )?
        )?
    )? ;
DATE_TOKEN : DIGIT DIGIT DIGIT DIGIT ('-' DIGIT DIGIT ('-' DIGIT DIGIT)?)? ;

WS : [ \t\r\n]+ -> skip ;

INT : [0-9]+ ;
DOUBLE: [0-9]+ ('.' [0-9]*)? ;

number : INT | DOUBLE ;

fragment ESCAPED_SINGLE_QUOTE : '\\\'';
fragment SINGLE_QUOTED_CONTENTS : ( ESCAPED_SINGLE_QUOTE | ~('\n'|'\r') )*? ;
fragment SINGLE_QUOTED_STRING : '\'' SINGLE_QUOTED_CONTENTS '\'';

fragment ESCAPED_DOUBLE_QUOTE : '\\"';
fragment DOUBLE_QUOTED_CONTENTS : ( ESCAPED_DOUBLE_QUOTE | ~('\n'|'\r') )*? ;
fragment DOUBLE_QUOTED_STRING : '"' DOUBLE_QUOTED_CONTENTS '"';

STRING_LITERAL : SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING ;

aggregateMetric
    : ('count()' | 'count' '(' ')') # AggregateCounts
    | 'lag' '(' INT ',' aggregateMetric ')' # AggregateLag
    | 'running' '(' aggregateMetric ')' # AggregateRunning
    | 'parent' '(' aggregateMetric ')' # AggregateParent
    | 'distinct' '(' identifier ('where' aggregateFilter)? ')' # AggregateDistinct
    | 'distinct_window' '(' INT ',' identifier ('where' aggregateFilter) ')' # AggregateDistinctWindow
    | 'window' '(' INT ',' aggregateMetric ')' # AggregateWindow
    | 'percentile' '(' identifier ',' DOUBLE ')' # AggregatePercentile
    | 'pdiff' '(' expected=aggregateMetric ',' actual=aggregateMetric ')' # AggregatePDiff
    | 'avg' '(' aggregateMetric ')' # AggregateAvg
    | 'variance' '(' docMetric ')' # AggregateVariance
    | 'stdev' '(' docMetric ')' # AggregateStandardDeviation
    | 'log' '(' aggregateMetric ')' # AggregateLog
    | 'abs' '(' aggregateMetric ')' # AggregateAbs
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
    ;

scope : '[' datasets+=identifier (',' datasets+=identifier)* ']' ;

aggregateFilter
    : field=identifier '=~' STRING_LITERAL # AggregateRegex
    | field=identifier '!=~' STRING_LITERAL # AggregateNotRegex
    | 'term()' '=' termVal # AggregateTermIs
    | aggregateMetric op=('='|'!='|'<'|'<='|'>'|'>=') aggregateMetric # AggregateMetricInequality
    | ('not'|'-'|'!') aggregateFilter # AggregateNot
    | aggregateFilter ('and' | '&&') aggregateFilter # AggregateAnd
    | aggregateFilter ('or' | '||') aggregateFilter # AggregateOr
    | '(' aggregateFilter ')' # AggregateFilterParens
    | 'true' # AggregateTrue
    | 'false' # AggregateFalse
    ;

docMetric
    : 'count()' # DocCounts
    | 'abs' '(' docMetric ')' # DocAbs
    | 'signum' '(' docMetric ')' # DocSignum
    /* TODO: identifier */
    | field=identifier ('='|':') term=(STRING_LITERAL | ID | TIME_UNIT | INT) # DocHasString
    /* TODO: identifier */
    | 'hasstr' '(' field=identifier ',' term=(STRING_LITERAL | ID | TIME_UNIT | INT) ')' # DocHasString
    | 'hasstr' '(' STRING_LITERAL ')' # DocHasStringQuoted
    /* TODO: identifier */
    | field=identifier '!=' term=(STRING_LITERAL | ID | TIME_UNIT) # DocHasntString
    | field=identifier ('='|':') term=INT # DocHasInt
    | 'hasint' '(' field=identifier ',' term=INT ')' # DocHasInt
    | 'hasint' '(' STRING_LITERAL ')' # DocHasIntQuoted
    | field=identifier '!=' INT # DocHasntInt
    | 'floatscale' '(' field=identifier ',' mult=INT ',' add=INT ')' # DocFloatScale
    | 'if' filter=docFilter 'then' trueCase=docMetric 'else' falseCase=docMetric # DocIfThenElse
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
    | field=identifier not='not'? 'in' '(' (terms += termVal)? (',' terms += termVal)* ')' # DocFieldIn
    | docMetric op=('='|'!='|'<'|'<='|'>'|'>=') docMetric # DocMetricInequality
    | ('lucene' | 'query') '(' STRING_LITERAL ')' # Lucene
    | 'between' '(' field=identifier ',' lowerBound=INT ',' upperBound=INT ')' # DocBetween
    | 'sample' '(' field=identifier ',' numerator=INT (',' denominator=INT (',' seed=(STRING_LITERAL | INT))?)? ')' # DocSample
    | ('-'|'!'|'not') docFilter # DocNot
    | docFilter ('and'|'&&') docFilter # DocAnd
    | docFilter ('or'|'||') docFilter # DocOr
    | '(' docFilter ')' # DocFilterParens
    | 'true' # DocTrue
    | 'false' # DocFalse
    ;

groupByElement
    : 'dayofweek' # DayOfWeekGroupBy
    | 'quantiles' '(' field=identifier ',' INT ')' # QuantilesGroupBy
    | topTermsGroupByElem # TopTermsGroupBy
    | field=identifier not='not'? 'in' '(' (terms += termVal)? (',' terms += termVal)* ')' (withDefault='with' 'default')? # GroupByFieldIn
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
                    (',' order=('bottom' | 'descending' | 'desc' | 'top' | 'ascending' | 'asc'))?
                )?
            )?
        ')'
    ;

groupByMetric
    : ('buckets' | 'bucket') '(' docMetric ',' min=INT ',' max=INT ',' interval=INT ')'
    ;

groupByMetricEnglish
    : docMetric 'from' min=INT 'to' max=INT 'by' interval=INT
    ;

groupByTime
    : ('time' | 'timebuckets') '(' timePeriod (',' timeField=identifier)? ')'
    ;

groupByField
    : field=identifier ('[' order=('top' | 'bottom')? limit=INT? ('by' metric=aggregateMetric)? ('where' filter=aggregateFilter)? ']')? (withDefault='with' 'default')?
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
    : index=identifier start=dateTime end=dateTime ('as' name=identifier)?
    ;

datasetOptTime
    : dataset # FullDataset
    | index=identifier ('as' name=identifier)? # PartialDataset
    ;

fromContents
    : dataset (',' datasetOptTime)*
    ;

groupByContents
    : groupByElement (',' groupByElement)*
    ;

selectContents
    : aggregateMetric (',' aggregateMetric)*
    ;

query
    : ('select' selects+=selectContents)?
      'from' fromContents
      ('where' docFilter+)?
      ('group' 'by' groupByContents)?
      ('select' selects+=selectContents)?
    ;