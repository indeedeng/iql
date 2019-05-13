const antlr4 = require('antlr4');
const JQLLexer = require('./JQLLexer');
const JQLParser = require('./JQLParser');

const ErrorListener = require('antlr4').error.ErrorListener;

const moment = require('moment');

const DEFAULT_UTC_OFFSET = -6;

import autobind from 'autobind-decorator';

function failure(err, expected) {
    return {errors: err, expected: expected, assumeSuccess: () => {throw err[0].msg}};
}

function success(result) {
    return {success: result, assumeSuccess: () => result};
}

function CollectingErrorListener() {
    ErrorListener.call(this);
    this.errors = [];
    this.expected = null;
    return this;
}

CollectingErrorListener.prototype = Object.create(ErrorListener.prototype);
CollectingErrorListener.prototype.constructor = CollectingErrorListener;

CollectingErrorListener.prototype.syntaxError = function(recognizer, offendingSymbol, line, column, msg, e) {
    this.errors.push({line: line, col: column, msg: msg});

    if (recognizer && recognizer._ctx) {
        const parser = recognizer._ctx.parser;

        const intervals = parser.getExpectedTokens().intervals;
        const expectedTokens = [];
        for (let i = 0; i < intervals.length; i++) {
            for (let token = intervals[i].start; token < intervals[i].stop; token++) {
                if (token === -1) {
                    expectedTokens.push('<EOF>');
                } else {
                    expectedTokens.push(parser.literalNames[token] || parser.symbolicNames[token]);
                }
            }
        }

        this.expected = expectedTokens;
    }

};

function runParser(parserName, input, parserArgs) {
    const chars = new antlr4.InputStream(input);
    const originalLA = chars.LA;
    chars.LA = function(x) {
        const result = originalLA.call(this, x);
        if (result <= 0) {
            return result;
        } else {
            return String.fromCharCode(result).toUpperCase().charCodeAt(0);
        }
    };

    const lexer = new JQLLexer.JQLLexer(chars);
    const errorListener = new CollectingErrorListener();
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);
    const tokens = new antlr4.CommonTokenStream(lexer);
    const parser = new JQLParser.JQLParser(tokens);
    parser.buildParseTrees = true;
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    const result = parser[parserName].apply(parser, parserArgs || []);
    if (errorListener.errors.length > 0) {
        return failure(errorListener.errors, errorListener.expected);
    } else if (result.parser._input.index === 0) {
        return failure("consumed no tokens -- first token likely invalid", []);
    } else {
        return success(result);
    }
}

class Parser {
    constructor(parserVersion) {
        if (parserVersion === 1) {
            this.isLegacy = true;
        } else if (parserVersion === 2) {
            this.isLegacy = false;
        } else {
            throw new Error("Invalid parser version: " + parserVersion);
        }
    }

    @autobind
    query(q) {
        return runParser('query', q, [this.isLegacy]);
    }

    @autobind
    from(q) {
        return runParser('fromContents', q, [this.isLegacy]);
    }

    @autobind
    where(q) {
        if (q.trim().length === 0) {
            return success("");
        }
        return runParser('whereContents', q, [this.isLegacy]);
    }

    @autobind
    groupBy(q) {
        if (q.trim().length === 0) {
            return success("");
        }
        return runParser('groupByContents', q, [this.isLegacy]);
    }

    @autobind
    select(q) {
        if (q.trim().length === 0) {
            return success("");
        }
        return runParser('selectContents', q, [this.isLegacy]);
    }

    @autobind
    queryParts(q) {
        const parseResult = this.query(q);
        if (parseResult.errors) return parseResult;

        const parsed = parseResult.success;

        const getText = makeGetText(parsed);

        const datasets = getText(parsed.fromContents());
        const where = parsed.whereContents() ? parsed.whereContents().docFilter().map(getText).join(' ') : "";
        const groupBy = parsed.groupByContents() ? getText(parsed.groupByContents()) : "";
        const select = parsed.selectContents().map(getText).join(' ');

        const result = { datasets, where, groupBy, select };

        if (parsed.fromContents().datasetOptTime().length === 0) {
            const dataset = parsed.fromContents().dataset();
            result.dataset = getText(dataset.index);
            result.start = getText(dataset.startTime);
            result.end = getText(dataset.endTime);
        }

        return success(result);
    }

    @autobind
    groupByRaws(groupByRaw) {
        if (groupByRaw.trim().length === 0) {
            return success([]);
        }
        const parseResult = runParser('groupByContents', groupByRaw, [this.isLegacy]);
        if (parseResult.errors) return parseResult;
        const parsed = parseResult.success;
        const getText = makeGetText(parsed);
        return success(parsed.groupByEntry().map(getText));
    }

    @autobind
    groupByAliases(groupByRaw) {
        if (groupByRaw.trim().length === 0) {
            return success([]);
        }
        const parseResult = runParser('groupByContents', groupByRaw, [this.isLegacy]);
        if (parseResult.errors) return parseResult;
        const parsed = parseResult.success;
        const getText = makeGetText(parsed);
        function getAlias(groupByElement) {
            let as = groupByElement.alias;
            if (as !== null) {
                return getText(as);
            }
            return "";
        }
        return success(parsed.groupByEntry().map(getAlias));
    }

    @autobind
    selectRaws(selectRaw) {
        if (selectRaw.trim().length === 0) {
            return success([]);
        }
        const parseResult = runParser('selectContents', selectRaw, [this.isLegacy]);
        if (parseResult.errors) return parseResult;
        const parsed = parseResult.success;
        const getText = makeGetText(parsed);
        return success(parsed.formattedAggregateMetric().map(getText));
    }

    @autobind
    selectDisplayValues(selectRaw) {
        if (selectRaw.trim().length === 0) {
            return success([]);
        }
        const parseResult = runParser('selectContents', selectRaw, [this.isLegacy]);
        if (parseResult.errors) return parseResult;
        const parsed = parseResult.success;
        const getText = makeGetText(parsed);
        return success(parsed.formattedAggregateMetric().map(x => {
            const base = x.aggregateMetric();
            const jqlMetric = base.jqlAggregateMetric();
            if (jqlMetric === null) {
                return getText(base);
            } else {
                if (typeof jqlMetric.name !== "undefined") {
                    return getText(jqlMetric.name);
                } else {
                    return getText(base);
                }
            }
        }));
    }

    @autobind
    fromDatasets(fromRaw) {
        const parseResult = runParser('fromContents', fromRaw, [this.isLegacy]);
        if (parseResult.errors) return parseResult;
        const parsed = parseResult.success;
        const getText = makeGetText(parsed);

        const datasets = [];

        datasets.push(getText(parsed.dataset().index));
        parsed.datasetOptTime().forEach(x => {
            if ("dataset" in x) {
                datasets.push(getText(x.dataset().index));
            } else {
                datasets.push(getText(x.index));
            }
        });

        return success(datasets);
    }

    /*
     Is this a time() group by?
     */
    @autobind
    isTimeRegroup(groupBy) {
        const parseResult = runParser('groupByTime', groupBy, [this.isLegacy]);
        if (parseResult.errors) return false;
        return true;
    }

    /*
     Is this group by a metric group by with interval != 1?
     */
    @autobind
    isInterval(groupBy) {
        const info = this.getBucketedMinMaxInterval(groupBy);
        if (info.errors) return info;
        const infoDetails = info.success;
        if (infoDetails === null) {
            return false;
        } else {
            return infoDetails.interval !== 1;
        }
    }

    /*
     If this group by is a metric regroup, {min:number,max:number,interval:number}
     Else, null
     */
    @autobind
    getBucketedMinMaxInterval(groupBy) {
        const parseResult = runParser('groupByEntry', groupBy, [this.isLegacy]);
        if (parseResult.errors) return parseResult;
        const parsed = parseResult.success;
        const getText = makeGetText(parsed);

        let min = null;
        let max = null;
        let interval = null;
        const groupByElement = parsed.groupByElement();
        if ((typeof groupByElement.groupByMetric === "function") && groupByElement.groupByMetric() !== null) {
            min = Number(getText(groupByElement.groupByMetric().min));
            max = Number(getText(groupByElement.groupByMetric().max));
            interval = Number(getText(groupByElement.groupByMetric().interval));
        } else if ((typeof groupByElement.groupByMetricEnglish === "function") && groupByElement.groupByMetricEnglish() !== null) {
            min = Number(getText(groupByElement.groupByMetricEnglish().min));
            max = Number(getText(groupByElement.groupByMetricEnglish().max));
            interval = Number(getText(groupByElement.groupByMetricEnglish().interval));
        }

        if (min !== null) {
            return success({min, max, interval});
        } else {
            return success(null);
        }
    }

    @autobind
    iqlDateToMomentDate(rawDate) {
        function successIfValid(d) {
            if (d.isValid()) {
                return success(d);
            } else {
                return failure('invalid date', 'a valid date');
            }
        }
        const parseResult = runParser('dateTimeTerminal', rawDate);
        if (parseResult.errors) {
            return parseResult;
        }
        const parsed = parseResult.success.dateTime();
        const getText = makeGetText(parsed);
        const timeAtStartOfDay = moment().utcOffset(DEFAULT_UTC_OFFSET).startOf('day');
        if (rawDate.length >= 3 && 'TODAY'.startsWith(rawDate.toUpperCase())) {
            return successIfValid(timeAtStartOfDay);
        } else if (rawDate.length >= 3 && 'TOMORROW'.startsWith(rawDate.toUpperCase())) {
            return successIfValid(timeAtStartOfDay.add(1, 'days'));
        } else if (rawDate.length >= 1 && 'YESTERDAY'.startsWith(rawDate.toUpperCase())) {
            return successIfValid(timeAtStartOfDay.subtract(1, 'days'));
        } else if (parsed.DATETIME_TOKEN() !== null) {
            return successIfValid(moment(rawDate));
        } else if (parsed.NAT() !== null) {
            // year or timestamp
            const value = Number(rawDate);
            if ((value > 2010) && (value < 2050)) {
                return successIfValid(moment().year(value).month(1).day(1).startOf('day'));
            }
            return successIfValid(moment(value * 1000 /*seconds to milliseconds*/));
        } else {
            // quoted dateTime or quoted relativeTime or relativeTime
            if (rawDate.startsWith("\"") || rawDate.startsWith("'")) {
                // unquote and try to parse as dateTime
                rawDate = unquote(rawDate);
                const result = this.iqlDateToMomentDate(rawDate);
                if ("success" in result) {
                    return result;
                }
            }
            // it's relative time
            rawDate = rawDate.trim();

            if (rawDate.toUpperCase().endsWith(" AGO")) {
                rawDate = rawDate.substring(0, rawDate.length - 4).trim();
            }
            const res = timeAtStartOfDay;

            let current = 0;
            const raw = rawDate;
            while (current < raw.length) {
                // skipping spaces
                while (current < raw.length && raw.charAt(current) === ' ') {
                    current++;
                }

                // parsing number
                const numberStart = current;
                while (current < raw.length && raw.charAt(current).match(/[0-9]/)) {
                    current++;
                }
                const numberEndExcl = current;
                const coeff = numberEndExcl > numberStart ? Number(raw.substring(numberStart, numberEndExcl)) : Number(1);

                // skipping spaces
                while (raw.charAt(current) === ' ') {
                    current++;
                }

                // parsing unit
                const periodStart = current;
                while (current < raw.length && raw.charAt(current).match(/[a-zA-Z]/)) {
                    current++;
                }
                const periodEndExcl = current;

                let unit = raw.substring(periodStart, periodEndExcl);
                if (unit.toUpperCase() === 'MO') {
                    unit = 'MONTH';
                }

                if (typeof res.get(unit) !== 'number') {
                    return failure('invalid unit: ' + unit, 'a valid unit');
                }
                res.subtract(coeff, unit);
            }

            return success(res);
        }
    }

    @autobind
    addSimpleWhereFilter(q, field, term, orIfPresent) {
        if (!orIfPresent) {
            return this.addWhereFilter(q, field + "=" + term);
        }
        const parseResult = this.query(q);
        if (parseResult.errors) {
            return parseResult;
        }
        const parsedQuery = parseResult.success;
        if (parsedQuery.whereContents() === null) {
            return this.addWhereFilter(q, field + '=' + term);
        }
        const getText = makeGetText(parsedQuery);
        const input = parsedQuery.parser._input.tokenSource._input;
        const whereContents = parsedQuery.whereContents();

        function findAndReplace(docFilter) {
            if (docFilter.constructor === JQLParser.JQLParser.DocFieldIsContext) {
                // IQL2 field=term
                const filterField = docFilter.singlyScopedField();
                if (getText(filterField.field) === field) {
                    return success(replaceElement(input, docFilter, field + ' IN (' + getText(docFilter.jqlTermVal()) + ', ' + term + ')'));
                }
            } else if (docFilter.constructor === JQLParser.JQLParser.DocFieldInContext) {
                // IQL2 field in (terms)
                const filterField = docFilter.singlyScopedField();
                if (getText(filterField.field) === field && docFilter.not === null) {
                    const terms = docFilter.terms;
                    const termsRaw = terms.map(x => getText(x));
                    termsRaw.push(term);
                    return success(replaceElement(input, docFilter, field + ' IN (' + termsRaw.join(', ') + ')'));
                }
            } else if (docFilter.constructor === JQLParser.JQLParser.LegacyDocFieldIsContext) {
                // IQL1 field=term
                if (getText(docFilter.field) === field) {
                    return success(replaceElement(input, docFilter, field + ' IN (' + getText(docFilter.legacyTermVal()) + ', ' + term + ')'));
                }
            } else if (docFilter.constructor === JQLParser.JQLParser.LegacyDocFieldInContext) {
                // IQL1 field in (terms)
                if (getText(docFilter.field) === field && docFilter.not === null) {
                    const termsRaw = docFilter.terms.map(x => getText(x));
                    termsRaw.push(term);
                    return success(replaceElement(input, docFilter, field + ' IN (' + termsRaw.join(', ') + ')'));
                }
            } else if (docFilter.constructor === JQLParser.JQLParser.DocAndContext) {
                // Recurse into ANDs
                const leftResult = findAndReplace(docFilter.jqlDocFilter(0));
                if (typeof leftResult !== 'undefined') {
                    return leftResult;
                }
                const rightResult = findAndReplace(docFilter.jqlDocFilter(1));
                if (typeof rightResult !== 'undefined') {
                    return rightResult;
                }
            } else if (docFilter.constructor === JQLParser.JQLParser.DocFilterParensContext) {
                // Recurse into parentheses
                const result = findAndReplace(docFilter.jqlDocFilter());
                if (typeof result !== 'undefined') {
                    return result;
                }
            }
        }

        for (var i = 0; i < whereContents.docFilters.length; i++) {
            const docFilter = whereContents.docFilters[i].children[0];
            const result = findAndReplace(docFilter);
            if (typeof result !== 'undefined') {
                return result;
            }
        }

        return this.addWhereFilter(q, field + '=' + term);
    }

    @autobind
    addWhereFilter(q, filter) {
        const parseResult = this.query(q);
        if (parseResult.errors) {
            return parseResult;
        }
        const parsedQuery = parseResult.success;
        const input = parsedQuery.parser._input.tokenSource._input;
        if (parsedQuery.whereContents() !== null) {
            const whereContents = parsedQuery.whereContents();
            const elems = whereContents.docFilters;
            const lastElem = elems[elems.length - 1];
            const whereEnd = getStartStop(lastElem).stop;
            const before = input.getText(0, whereEnd);
            const after = input.getText(whereEnd + 1, input._size);
            const middle = after.charAt(0) === ' ' ? '' : ' ';
            return success(before + ' AND ' + filter + middle + after);
        }
        const end = getStartStop(parsedQuery.fromContents()).stop;
        const before = input.getText(0, end);
        const after = input.getText(end + 1, input._size);
        let addNewline;
        if (after.search(/\s*\n/) === 0) {
            addNewline = false;
        } else {
            addNewline = true;
        }
        return success(before + '\nWHERE ' + filter + (addNewline ? '\n' : '') + after);
    }

    @autobind
    addGroupBy(q, groupBy) {
        const parseResult = this.query(q);
        if (parseResult.errors) {
            return parseResult;
        }
        const parsedQuery = parseResult.success;
        const input = parsedQuery.parser._input.tokenSource._input;
        if (parsedQuery.groupByContents() !== null) {
            const groupByContents = parsedQuery.groupByContents();
            const elems = groupByContents.groupByEntry();
            const lastElem = elems[elems.length - 1];
            const groupByEnd = getStartStop(lastElem).stop;
            const before = input.getText(0, groupByEnd);
            const after = input.getText(groupByEnd + 1, input._size);
            let middle = '';
            if (after.charAt(0) !== ' ') {
                middle = ' ';
            }
            return success(before + ', ' + groupBy + middle + after);
        }
        const prevElem = parsedQuery.whereContents() !== null ? parsedQuery.whereContents() : parsedQuery.fromContents();
        const end = getStartStop(prevElem).stop;
        const before = input.getText(0, end);
        const after = input.getText(end + 1, input._size);
        return success(before + '\nGROUP BY ' + groupBy + '\n' + after);
    }

    @autobind
    removeGroupBy(q, groupByIndex) {
        const parseResult = this.query(q);
        if (parseResult.errors) {
            return parseResult;
        }
        const parsedQuery = parseResult.success;
        if (parsedQuery.groupByContents() !== null) {
            const groupByContents = parsedQuery.groupByContents();
            const elems = groupByContents.groupByEntry();
            if (groupByIndex >= elems.length) {
                return failure('query has only ' + elems.length + ' group bys.', 'a query with at least ' + (groupByIndex + 1) + ' group bys.');
            }

            let firstToDelete, lastToDelete;
            if (elems.length === 1) {
                firstToDelete = parsedQuery.GROUP().symbol;
                lastToDelete = groupByContents;
            } else {
                if (groupByIndex === 0) {
                    firstToDelete = elems[0];
                    lastToDelete = nextChildOfParent(elems[0]); // comma after
                } else {
                    firstToDelete = prevChildOfParent(elems[groupByIndex]); // comma before
                    lastToDelete = elems[groupByIndex];
                }
            }

            const input = parsedQuery.parser._input.tokenSource._input;

            const deleteStart = getStartStop(firstToDelete).start;
            const deleteEnd = getStartStop(lastToDelete).stop;

            const before = input.getText(0, deleteStart - 1);
            const after = deleteEnd === input._size - 1 ? '' : input.getText(deleteEnd + 1, input._size);
            return success(before + after);
        }
        return failure('query has no group-by: ' + q, 'a query with at least ' + (groupByIndex + 1) + ' group bys.');
    }

    // return original query if we fail to parse it
    @autobind
    convertQueryDateToAbsoluteIfValid(q) {
        const queryParseResult = this.query(q);
        if (!queryParseResult.success) {
            return q;
        }
        let queryCtx = queryParseResult.success;
        let dates = [];
        this.collectQueryDates(queryCtx, dates);

        let convertedQuery = '';
        let lastPos = 0;
        for (const date of dates) {
            convertedQuery += q.slice(lastPos, date[0]);
            let originDateStr = q.substring(date[0], date[1] + 1);
            const rawDateMoment = this.iqlDateToMomentDate(originDateStr);
            if (rawDateMoment.success) {
                let formattedDate;
                const dateMoment = rawDateMoment.success;
                if (dateMoment.seconds() !== 0) {
                    formattedDate = dateMoment.format("YYYY-MM-DD HH:mm:ss");
                } else if (dateMoment.minutes() !== 0 || dateMoment.hours() !== 0) {
                    formattedDate = dateMoment.format("YYYY-MM-DD HH:mm");
                } else {
                    formattedDate = dateMoment.format("YYYY-MM-DD");
                }
                convertedQuery += formattedDate;
            } else {
                convertedQuery += originDateStr;
            }
            lastPos = date[1] + 1;
        }
        convertedQuery += q.slice(lastPos);
        return convertedQuery;
    }


    collectQueryDates(ctx, dates) {
        if (ctx.constructor === JQLParser.JQLParser.DateTimeContext) {
            dates.push([ctx.start.start, ctx.stop.stop]);
        }
        if (ctx.children) {
            for (const child of ctx.children) {
                this.collectQueryDates(child, dates);
            }
        }
    }

}

function replaceElement(input, element, replacement) {
    const startStop = getStartStop(element);
    const before = input.getText(0, startStop.start - 1);
    const after = input.getText(startStop.stop + 1, input._size);
    return before + replacement + after;
}

function getStartStop(x) {
    if (typeof x.start === 'number') {
        return {start: x.start, stop: x.stop};
    } else if('symbol' in x) {
        return { start: x.symbol.start, stop: x.symbol.stop };
    } else {
        return { start: x.start.start, stop: x.stop.stop };
    }
}

function prevChildOfParent(x) {
    const parent = x.parentCtx;
    for (let i = 0; i < parent.children.length; i++) {
        if (parent.children[i] === x) {
            return parent.children[i - 1];
        }
    }
    return null;
}

function nextChildOfParent(x) {
    const parent = x.parentCtx;
    for (let i = 0; i < parent.children.length; i++) {
        if (parent.children[i] === x) {
            return parent.children[i + 1];
        }
    }
    return null;
}

function makeGetText(parsed) {
    return x => {
        const {start, stop} = getStartStop(x);
        return parsed.parser._input.tokenSource._input.getText(start, stop);
    }
}


function walkIt(tree, walker) {
    antlr4.tree.ParseTreeWalker.DEFAULT.walk(walker, tree);
}

function unquote(string) {
    if ((string.charAt(0) === '"' && string.charAt(string.length - 1) === '"')
        || (string.charAt(0) === "'" && string.charAt(string.length - 1) === "'")) {
        return string.substring(1, string.length - 1);
    } else {
        return string;
    }
}

module.exports = {
    Parser,
    IQL1Parser: new Parser(1),
    IQL2Parser: new Parser(2),
    walkIt,
    makeGetText,
    JQLParser,
    JQLLexer,
};
