package com.indeed.imhotep.sql.parser;

import com.google.common.base.Strings;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.Token;
import org.codehaus.jparsec.pattern.Patterns;

import java.util.List;
import java.util.Map;

/**
 * Takes care of applying field aliases / expansions before the main parser runs.
 * @author vladimir
 */
public class Preprocessor {
    public static final Parser<String> wordParser = Scanners.IDENTIFIER;
    public static final Parser<String> nonWordParser = Scanners.pattern(Patterns.regex("[^a-zA-Z]"), "nonword").source();
    public static final Parser<String> termsTokenizer = Parsers.or(Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
            Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER, wordParser, nonWordParser).source();
    final static  Parser<List<Token>> tokenizer = termsTokenizer.lexer(Scanners.SQL_DELIMITER); // could use Scanners.WHITESPACES instead of SQL_DELIMITER

    public static String applyAliases(String clause, Map<String, String> aliases) {
        if(Strings.isNullOrEmpty(clause)) {
            return clause;
        }
        // tokenize while ignoring quoted strings
        final List<Token> tokens = tokenizer.parse(clause);

        // for each token, look up the NameExpression in a map and if found replace with the provided replacement expression
        for(int i = tokens.size()-1; i >= 0; i--) {
            final Token token = tokens.get(i);
            final String tokenStr = tokenAsString(token);
            final String replacement = aliases.get(tokenStr);
            if(replacement != null) {
                // TODO: optimize?
                clause = clause.substring(0, token.index()) + replacement + clause.substring(token.index() + token.length());
            }
        }

        return clause;
    }

    private static String tokenAsString(Token token) {
        return token != null ? (String) token.value() : "";
    }
}
