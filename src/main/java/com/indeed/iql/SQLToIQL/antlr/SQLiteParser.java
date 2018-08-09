// Generated from /Users/tren/Desktop/SQLTOIQL/src/main/java/com/indeed/sqltoiql/SQLite.g4 by ANTLR 4.7
package com.indeed.iql.SQLToIQL.antlr;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SQLiteParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
			new PredictionContextCache();
	public static final int
			SCOL=1, DOT=2, OPEN_PAR=3, CLOSE_PAR=4, COMMA=5, ASSIGN=6, STAR=7, PLUS=8,
			MINUS=9, TILDE=10, PIPE2=11, DIV=12, MOD=13, LT2=14, GT2=15, AMP=16, PIPE=17,
			LT=18, LT_EQ=19, GT=20, GT_EQ=21, EQ=22, NOT_EQ1=23, NOT_EQ2=24, K_ABORT=25,
			K_ACTION=26, K_ADD=27, K_AFTER=28, K_ALL=29, K_ALTER=30, K_ANALYZE=31,
			K_AND=32, K_AS=33, K_ASC=34, K_ATTACH=35, K_AUTOINCREMENT=36, K_BEFORE=37,
			K_BEGIN=38, K_BETWEEN=39, K_BY=40, K_CASCADE=41, K_CASE=42, K_CAST=43,
			K_CHECK=44, K_COLLATE=45, K_COLUMN=46, K_COMMIT=47, K_CONFLICT=48, K_CONSTRAINT=49,
			K_CREATE=50, K_CROSS=51, K_CURRENT_DATE=52, K_CURRENT_TIME=53, K_CURRENT_TIMESTAMP=54,
			K_DATABASE=55, K_DEFAULT=56, K_DEFERRABLE=57, K_DEFERRED=58, K_DELETE=59,
			K_DESC=60, K_DETACH=61, K_DISTINCT=62, K_DROP=63, K_EACH=64, K_ELSE=65,
			K_END=66, K_ESCAPE=67, K_EXCEPT=68, K_EXCLUSIVE=69, K_EXISTS=70, K_EXPLAIN=71,
			K_FAIL=72, K_FOR=73, K_FOREIGN=74, K_FROM=75, K_FULL=76, K_GLOB=77, K_GROUP=78,
			K_HAVING=79, K_IF=80, K_IGNORE=81, K_IMMEDIATE=82, K_IN=83, K_INDEX=84,
			K_INDEXED=85, K_INITIALLY=86, K_INNER=87, K_INSERT=88, K_INSTEAD=89, K_INTERSECT=90,
			K_INTO=91, K_IS=92, K_ISNULL=93, K_JOIN=94, K_KEY=95, K_LEFT=96, K_LIKE=97,
			K_LIMIT=98, K_MATCH=99, K_NATURAL=100, K_NO=101, K_NOT=102, K_NOTNULL=103,
			K_NULL=104, K_OF=105, K_OFFSET=106, K_ON=107, K_OR=108, K_ORDER=109, K_OUTER=110,
			K_PLAN=111, K_PRAGMA=112, K_PRIMARY=113, K_QUERY=114, K_RAISE=115, K_RECURSIVE=116,
			K_REFERENCES=117, K_REGEXP=118, K_REINDEX=119, K_RELEASE=120, K_RENAME=121,
			K_REPLACE=122, K_RESTRICT=123, K_RIGHT=124, K_ROLLBACK=125, K_ROW=126,
			K_SAVEPOINT=127, K_SELECT=128, K_SET=129, K_TABLE=130, K_TEMP=131, K_TEMPORARY=132,
			K_THEN=133, K_TO=134, K_TRANSACTION=135, K_TRIGGER=136, K_UNION=137, K_UNIQUE=138,
			K_UPDATE=139, K_USING=140, K_VACUUM=141, K_VALUES=142, K_VIEW=143, K_VIRTUAL=144,
			K_WHEN=145, K_WHERE=146, K_WITH=147, K_WITHOUT=148, IDENTIFIER=149, NUMERIC_LITERAL=150,
			BIND_PARAMETER=151, STRING_LITERAL=152, BLOB_LITERAL=153, SINGLE_LINE_COMMENT=154,
			MULTILINE_COMMENT=155, SPACES=156, UNEXPECTED_CHAR=157;
	public static final int
			RULE_parse = 0, RULE_error = 1, RULE_sql_stmt_list = 2, RULE_sql_stmt = 3,
			RULE_alter_table_stmt = 4, RULE_analyze_stmt = 5, RULE_attach_stmt = 6,
			RULE_begin_stmt = 7, RULE_commit_stmt = 8, RULE_compound_select_stmt = 9,
			RULE_create_index_stmt = 10, RULE_create_table_stmt = 11, RULE_create_trigger_stmt = 12,
			RULE_create_view_stmt = 13, RULE_create_virtual_table_stmt = 14, RULE_delete_stmt = 15,
			RULE_delete_stmt_limited = 16, RULE_detach_stmt = 17, RULE_drop_index_stmt = 18,
			RULE_drop_table_stmt = 19, RULE_drop_trigger_stmt = 20, RULE_drop_view_stmt = 21,
			RULE_factored_select_stmt = 22, RULE_insert_stmt = 23, RULE_pragma_stmt = 24,
			RULE_reindex_stmt = 25, RULE_release_stmt = 26, RULE_rollback_stmt = 27,
			RULE_savepoint_stmt = 28, RULE_simple_select_stmt = 29, RULE_select_stmt = 30,
			RULE_select_or_values = 31, RULE_update_stmt = 32, RULE_update_stmt_limited = 33,
			RULE_vacuum_stmt = 34, RULE_column_def = 35, RULE_type_name = 36, RULE_column_constraint = 37,
			RULE_conflict_clause = 38, RULE_expr = 39, RULE_foreign_key_clause = 40,
			RULE_raise_function = 41, RULE_indexed_column = 42, RULE_table_constraint = 43,
			RULE_with_clause = 44, RULE_qualified_table_name = 45, RULE_ordering_term = 46,
			RULE_pragma_value = 47, RULE_common_table_expression = 48, RULE_result_column = 49,
			RULE_table_or_subquery = 50, RULE_join_clause = 51, RULE_join_operator = 52,
			RULE_join_constraint = 53, RULE_select_core = 54, RULE_group_by = 55,
			RULE_compound_operator = 56, RULE_cte_table_name = 57, RULE_signed_number = 58,
			RULE_literal_value = 59, RULE_unary_operator = 60, RULE_error_message = 61,
			RULE_module_argument = 62, RULE_column_alias = 63, RULE_keyword = 64,
			RULE_name = 65, RULE_function_name = 66, RULE_database_name = 67, RULE_table_name = 68,
			RULE_table_or_index_name = 69, RULE_new_table_name = 70, RULE_column_name = 71,
			RULE_collation_name = 72, RULE_foreign_table = 73, RULE_index_name = 74,
			RULE_trigger_name = 75, RULE_view_name = 76, RULE_module_name = 77, RULE_pragma_name = 78,
			RULE_savepoint_name = 79, RULE_table_alias = 80, RULE_transaction_name = 81,
			RULE_any_name = 82;
	public static final String[] ruleNames = {
			"parse", "error", "sql_stmt_list", "sql_stmt", "alter_table_stmt", "analyze_stmt",
			"attach_stmt", "begin_stmt", "commit_stmt", "compound_select_stmt", "create_index_stmt",
			"create_table_stmt", "create_trigger_stmt", "create_view_stmt", "create_virtual_table_stmt",
			"delete_stmt", "delete_stmt_limited", "detach_stmt", "drop_index_stmt",
			"drop_table_stmt", "drop_trigger_stmt", "drop_view_stmt", "factored_select_stmt",
			"insert_stmt", "pragma_stmt", "reindex_stmt", "release_stmt", "rollback_stmt",
			"savepoint_stmt", "simple_select_stmt", "select_stmt", "select_or_values",
			"update_stmt", "update_stmt_limited", "vacuum_stmt", "column_def", "type_name",
			"column_constraint", "conflict_clause", "expr", "foreign_key_clause",
			"raise_function", "indexed_column", "table_constraint", "with_clause",
			"qualified_table_name", "ordering_term", "pragma_value", "common_table_expression",
			"result_column", "table_or_subquery", "join_clause", "join_operator",
			"join_constraint", "select_core", "group_by", "compound_operator", "cte_table_name",
			"signed_number", "literal_value", "unary_operator", "error_message", "module_argument",
			"column_alias", "keyword", "name", "function_name", "database_name", "table_name",
			"table_or_index_name", "new_table_name", "column_name", "collation_name",
			"foreign_table", "index_name", "trigger_name", "view_name", "module_name",
			"pragma_name", "savepoint_name", "table_alias", "transaction_name", "any_name"
	};

	private static final String[] _LITERAL_NAMES = {
			null, "';'", "'.'", "'('", "')'", "','", "'='", "'*'", "'+'", "'-'", "'~'",
			"'||'", "'/'", "'%'", "'<<'", "'>>'", "'&'", "'|'", "'<'", "'<='", "'>'",
			"'>='", "'=='", "'!='", "'<>'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
			null, "SCOL", "DOT", "OPEN_PAR", "CLOSE_PAR", "COMMA", "ASSIGN", "STAR",
			"PLUS", "MINUS", "TILDE", "PIPE2", "DIV", "MOD", "LT2", "GT2", "AMP",
			"PIPE", "LT", "LT_EQ", "GT", "GT_EQ", "EQ", "NOT_EQ1", "NOT_EQ2", "K_ABORT",
			"K_ACTION", "K_ADD", "K_AFTER", "K_ALL", "K_ALTER", "K_ANALYZE", "K_AND",
			"K_AS", "K_ASC", "K_ATTACH", "K_AUTOINCREMENT", "K_BEFORE", "K_BEGIN",
			"K_BETWEEN", "K_BY", "K_CASCADE", "K_CASE", "K_CAST", "K_CHECK", "K_COLLATE",
			"K_COLUMN", "K_COMMIT", "K_CONFLICT", "K_CONSTRAINT", "K_CREATE", "K_CROSS",
			"K_CURRENT_DATE", "K_CURRENT_TIME", "K_CURRENT_TIMESTAMP", "K_DATABASE",
			"K_DEFAULT", "K_DEFERRABLE", "K_DEFERRED", "K_DELETE", "K_DESC", "K_DETACH",
			"K_DISTINCT", "K_DROP", "K_EACH", "K_ELSE", "K_END", "K_ESCAPE", "K_EXCEPT",
			"K_EXCLUSIVE", "K_EXISTS", "K_EXPLAIN", "K_FAIL", "K_FOR", "K_FOREIGN",
			"K_FROM", "K_FULL", "K_GLOB", "K_GROUP", "K_HAVING", "K_IF", "K_IGNORE",
			"K_IMMEDIATE", "K_IN", "K_INDEX", "K_INDEXED", "K_INITIALLY", "K_INNER",
			"K_INSERT", "K_INSTEAD", "K_INTERSECT", "K_INTO", "K_IS", "K_ISNULL",
			"K_JOIN", "K_KEY", "K_LEFT", "K_LIKE", "K_LIMIT", "K_MATCH", "K_NATURAL",
			"K_NO", "K_NOT", "K_NOTNULL", "K_NULL", "K_OF", "K_OFFSET", "K_ON", "K_OR",
			"K_ORDER", "K_OUTER", "K_PLAN", "K_PRAGMA", "K_PRIMARY", "K_QUERY", "K_RAISE",
			"K_RECURSIVE", "K_REFERENCES", "K_REGEXP", "K_REINDEX", "K_RELEASE", "K_RENAME",
			"K_REPLACE", "K_RESTRICT", "K_RIGHT", "K_ROLLBACK", "K_ROW", "K_SAVEPOINT",
			"K_SELECT", "K_SET", "K_TABLE", "K_TEMP", "K_TEMPORARY", "K_THEN", "K_TO",
			"K_TRANSACTION", "K_TRIGGER", "K_UNION", "K_UNIQUE", "K_UPDATE", "K_USING",
			"K_VACUUM", "K_VALUES", "K_VIEW", "K_VIRTUAL", "K_WHEN", "K_WHERE", "K_WITH",
			"K_WITHOUT", "IDENTIFIER", "NUMERIC_LITERAL", "BIND_PARAMETER", "STRING_LITERAL",
			"BLOB_LITERAL", "SINGLE_LINE_COMMENT", "MULTILINE_COMMENT", "SPACES",
			"UNEXPECTED_CHAR"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SQLite.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SQLiteParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class ParseContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(SQLiteParser.EOF, 0); }
		public List<Sql_stmt_listContext> sql_stmt_list() {
			return getRuleContexts(Sql_stmt_listContext.class);
		}
		public Sql_stmt_listContext sql_stmt_list(int i) {
			return getRuleContext(Sql_stmt_listContext.class,i);
		}
		public List<ErrorContext> error() {
			return getRuleContexts(ErrorContext.class);
		}
		public ErrorContext error(int i) {
			return getRuleContext(ErrorContext.class,i);
		}
		public ParseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterParse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitParse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitParse(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseContext parse() throws RecognitionException {
		ParseContext _localctx = new ParseContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_parse);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(170);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SCOL) | (1L << K_ALTER) | (1L << K_ANALYZE) | (1L << K_ATTACH) | (1L << K_BEGIN) | (1L << K_COMMIT) | (1L << K_CREATE) | (1L << K_DELETE) | (1L << K_DETACH) | (1L << K_DROP))) != 0) || ((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & ((1L << (K_END - 66)) | (1L << (K_EXPLAIN - 66)) | (1L << (K_INSERT - 66)) | (1L << (K_PRAGMA - 66)) | (1L << (K_REINDEX - 66)) | (1L << (K_RELEASE - 66)) | (1L << (K_REPLACE - 66)) | (1L << (K_ROLLBACK - 66)) | (1L << (K_SAVEPOINT - 66)) | (1L << (K_SELECT - 66)))) != 0) || ((((_la - 139)) & ~0x3f) == 0 && ((1L << (_la - 139)) & ((1L << (K_UPDATE - 139)) | (1L << (K_VACUUM - 139)) | (1L << (K_VALUES - 139)) | (1L << (K_WITH - 139)) | (1L << (UNEXPECTED_CHAR - 139)))) != 0)) {
					{
						setState(168);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
							case SCOL:
							case K_ALTER:
							case K_ANALYZE:
							case K_ATTACH:
							case K_BEGIN:
							case K_COMMIT:
							case K_CREATE:
							case K_DELETE:
							case K_DETACH:
							case K_DROP:
							case K_END:
							case K_EXPLAIN:
							case K_INSERT:
							case K_PRAGMA:
							case K_REINDEX:
							case K_RELEASE:
							case K_REPLACE:
							case K_ROLLBACK:
							case K_SAVEPOINT:
							case K_SELECT:
							case K_UPDATE:
							case K_VACUUM:
							case K_VALUES:
							case K_WITH:
							{
								setState(166);
								sql_stmt_list();
							}
							break;
							case UNEXPECTED_CHAR:
							{
								setState(167);
								error();
							}
							break;
							default:
								throw new NoViableAltException(this);
						}
					}
					setState(172);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(173);
				match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorContext extends ParserRuleContext {
		public Token UNEXPECTED_CHAR;
		public TerminalNode UNEXPECTED_CHAR() { return getToken(SQLiteParser.UNEXPECTED_CHAR, 0); }
		public ErrorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_error; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterError(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitError(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitError(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorContext error() throws RecognitionException {
		ErrorContext _localctx = new ErrorContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_error);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(175);
				((ErrorContext)_localctx).UNEXPECTED_CHAR = match(UNEXPECTED_CHAR);

				throw new RuntimeException("UNEXPECTED_CHAR=" + (((ErrorContext)_localctx).UNEXPECTED_CHAR!=null?((ErrorContext)_localctx).UNEXPECTED_CHAR.getText():null));

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sql_stmt_listContext extends ParserRuleContext {
		public List<Sql_stmtContext> sql_stmt() {
			return getRuleContexts(Sql_stmtContext.class);
		}
		public Sql_stmtContext sql_stmt(int i) {
			return getRuleContext(Sql_stmtContext.class,i);
		}
		public Sql_stmt_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_stmt_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSql_stmt_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSql_stmt_list(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSql_stmt_list(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Sql_stmt_listContext sql_stmt_list() throws RecognitionException {
		Sql_stmt_listContext _localctx = new Sql_stmt_listContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_sql_stmt_list);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(181);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==SCOL) {
					{
						{
							setState(178);
							match(SCOL);
						}
					}
					setState(183);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(184);
				sql_stmt();
				setState(193);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
							{
								setState(186);
								_errHandler.sync(this);
								_la = _input.LA(1);
								do {
									{
										{
											setState(185);
											match(SCOL);
										}
									}
									setState(188);
									_errHandler.sync(this);
									_la = _input.LA(1);
								} while ( _la==SCOL );
								setState(190);
								sql_stmt();
							}
						}
					}
					setState(195);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
				}
				setState(199);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
							{
								setState(196);
								match(SCOL);
							}
						}
					}
					setState(201);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,5,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sql_stmtContext extends ParserRuleContext {
		public Alter_table_stmtContext alter_table_stmt() {
			return getRuleContext(Alter_table_stmtContext.class,0);
		}
		public Analyze_stmtContext analyze_stmt() {
			return getRuleContext(Analyze_stmtContext.class,0);
		}
		public Attach_stmtContext attach_stmt() {
			return getRuleContext(Attach_stmtContext.class,0);
		}
		public Begin_stmtContext begin_stmt() {
			return getRuleContext(Begin_stmtContext.class,0);
		}
		public Commit_stmtContext commit_stmt() {
			return getRuleContext(Commit_stmtContext.class,0);
		}
		public Compound_select_stmtContext compound_select_stmt() {
			return getRuleContext(Compound_select_stmtContext.class,0);
		}
		public Create_index_stmtContext create_index_stmt() {
			return getRuleContext(Create_index_stmtContext.class,0);
		}
		public Create_table_stmtContext create_table_stmt() {
			return getRuleContext(Create_table_stmtContext.class,0);
		}
		public Create_trigger_stmtContext create_trigger_stmt() {
			return getRuleContext(Create_trigger_stmtContext.class,0);
		}
		public Create_view_stmtContext create_view_stmt() {
			return getRuleContext(Create_view_stmtContext.class,0);
		}
		public Create_virtual_table_stmtContext create_virtual_table_stmt() {
			return getRuleContext(Create_virtual_table_stmtContext.class,0);
		}
		public Delete_stmtContext delete_stmt() {
			return getRuleContext(Delete_stmtContext.class,0);
		}
		public Delete_stmt_limitedContext delete_stmt_limited() {
			return getRuleContext(Delete_stmt_limitedContext.class,0);
		}
		public Detach_stmtContext detach_stmt() {
			return getRuleContext(Detach_stmtContext.class,0);
		}
		public Drop_index_stmtContext drop_index_stmt() {
			return getRuleContext(Drop_index_stmtContext.class,0);
		}
		public Drop_table_stmtContext drop_table_stmt() {
			return getRuleContext(Drop_table_stmtContext.class,0);
		}
		public Drop_trigger_stmtContext drop_trigger_stmt() {
			return getRuleContext(Drop_trigger_stmtContext.class,0);
		}
		public Drop_view_stmtContext drop_view_stmt() {
			return getRuleContext(Drop_view_stmtContext.class,0);
		}
		public Factored_select_stmtContext factored_select_stmt() {
			return getRuleContext(Factored_select_stmtContext.class,0);
		}
		public Insert_stmtContext insert_stmt() {
			return getRuleContext(Insert_stmtContext.class,0);
		}
		public Pragma_stmtContext pragma_stmt() {
			return getRuleContext(Pragma_stmtContext.class,0);
		}
		public Reindex_stmtContext reindex_stmt() {
			return getRuleContext(Reindex_stmtContext.class,0);
		}
		public Release_stmtContext release_stmt() {
			return getRuleContext(Release_stmtContext.class,0);
		}
		public Rollback_stmtContext rollback_stmt() {
			return getRuleContext(Rollback_stmtContext.class,0);
		}
		public Savepoint_stmtContext savepoint_stmt() {
			return getRuleContext(Savepoint_stmtContext.class,0);
		}
		public Simple_select_stmtContext simple_select_stmt() {
			return getRuleContext(Simple_select_stmtContext.class,0);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public Update_stmtContext update_stmt() {
			return getRuleContext(Update_stmtContext.class,0);
		}
		public Update_stmt_limitedContext update_stmt_limited() {
			return getRuleContext(Update_stmt_limitedContext.class,0);
		}
		public Vacuum_stmtContext vacuum_stmt() {
			return getRuleContext(Vacuum_stmtContext.class,0);
		}
		public TerminalNode K_EXPLAIN() { return getToken(SQLiteParser.K_EXPLAIN, 0); }
		public TerminalNode K_QUERY() { return getToken(SQLiteParser.K_QUERY, 0); }
		public TerminalNode K_PLAN() { return getToken(SQLiteParser.K_PLAN, 0); }
		public Sql_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSql_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSql_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSql_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Sql_stmtContext sql_stmt() throws RecognitionException {
		Sql_stmtContext _localctx = new Sql_stmtContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_sql_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(207);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_EXPLAIN) {
					{
						setState(202);
						match(K_EXPLAIN);
						setState(205);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_QUERY) {
							{
								setState(203);
								match(K_QUERY);
								setState(204);
								match(K_PLAN);
							}
						}

					}
				}

				setState(239);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
					case 1:
					{
						setState(209);
						alter_table_stmt();
					}
					break;
					case 2:
					{
						setState(210);
						analyze_stmt();
					}
					break;
					case 3:
					{
						setState(211);
						attach_stmt();
					}
					break;
					case 4:
					{
						setState(212);
						begin_stmt();
					}
					break;
					case 5:
					{
						setState(213);
						commit_stmt();
					}
					break;
					case 6:
					{
						setState(214);
						compound_select_stmt();
					}
					break;
					case 7:
					{
						setState(215);
						create_index_stmt();
					}
					break;
					case 8:
					{
						setState(216);
						create_table_stmt();
					}
					break;
					case 9:
					{
						setState(217);
						create_trigger_stmt();
					}
					break;
					case 10:
					{
						setState(218);
						create_view_stmt();
					}
					break;
					case 11:
					{
						setState(219);
						create_virtual_table_stmt();
					}
					break;
					case 12:
					{
						setState(220);
						delete_stmt();
					}
					break;
					case 13:
					{
						setState(221);
						delete_stmt_limited();
					}
					break;
					case 14:
					{
						setState(222);
						detach_stmt();
					}
					break;
					case 15:
					{
						setState(223);
						drop_index_stmt();
					}
					break;
					case 16:
					{
						setState(224);
						drop_table_stmt();
					}
					break;
					case 17:
					{
						setState(225);
						drop_trigger_stmt();
					}
					break;
					case 18:
					{
						setState(226);
						drop_view_stmt();
					}
					break;
					case 19:
					{
						setState(227);
						factored_select_stmt();
					}
					break;
					case 20:
					{
						setState(228);
						insert_stmt();
					}
					break;
					case 21:
					{
						setState(229);
						pragma_stmt();
					}
					break;
					case 22:
					{
						setState(230);
						reindex_stmt();
					}
					break;
					case 23:
					{
						setState(231);
						release_stmt();
					}
					break;
					case 24:
					{
						setState(232);
						rollback_stmt();
					}
					break;
					case 25:
					{
						setState(233);
						savepoint_stmt();
					}
					break;
					case 26:
					{
						setState(234);
						simple_select_stmt();
					}
					break;
					case 27:
					{
						setState(235);
						select_stmt();
					}
					break;
					case 28:
					{
						setState(236);
						update_stmt();
					}
					break;
					case 29:
					{
						setState(237);
						update_stmt_limited();
					}
					break;
					case 30:
					{
						setState(238);
						vacuum_stmt();
					}
					break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Alter_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_ALTER() { return getToken(SQLiteParser.K_ALTER, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_RENAME() { return getToken(SQLiteParser.K_RENAME, 0); }
		public TerminalNode K_TO() { return getToken(SQLiteParser.K_TO, 0); }
		public New_table_nameContext new_table_name() {
			return getRuleContext(New_table_nameContext.class,0);
		}
		public TerminalNode K_ADD() { return getToken(SQLiteParser.K_ADD, 0); }
		public Column_defContext column_def() {
			return getRuleContext(Column_defContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_COLUMN() { return getToken(SQLiteParser.K_COLUMN, 0); }
		public Alter_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAlter_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAlter_table_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitAlter_table_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Alter_table_stmtContext alter_table_stmt() throws RecognitionException {
		Alter_table_stmtContext _localctx = new Alter_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_alter_table_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(241);
				match(K_ALTER);
				setState(242);
				match(K_TABLE);
				setState(246);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
					case 1:
					{
						setState(243);
						database_name();
						setState(244);
						match(DOT);
					}
					break;
				}
				setState(248);
				table_name();
				setState(257);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_RENAME:
					{
						setState(249);
						match(K_RENAME);
						setState(250);
						match(K_TO);
						setState(251);
						new_table_name();
					}
					break;
					case K_ADD:
					{
						setState(252);
						match(K_ADD);
						setState(254);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
							case 1:
							{
								setState(253);
								match(K_COLUMN);
							}
							break;
						}
						setState(256);
						column_def();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Analyze_stmtContext extends ParserRuleContext {
		public TerminalNode K_ANALYZE() { return getToken(SQLiteParser.K_ANALYZE, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Table_or_index_nameContext table_or_index_name() {
			return getRuleContext(Table_or_index_nameContext.class,0);
		}
		public Analyze_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_analyze_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAnalyze_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAnalyze_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitAnalyze_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Analyze_stmtContext analyze_stmt() throws RecognitionException {
		Analyze_stmtContext _localctx = new Analyze_stmtContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_analyze_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(259);
				match(K_ANALYZE);
				setState(266);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
					case 1:
					{
						setState(260);
						database_name();
					}
					break;
					case 2:
					{
						setState(261);
						table_or_index_name();
					}
					break;
					case 3:
					{
						setState(262);
						database_name();
						setState(263);
						match(DOT);
						setState(264);
						table_or_index_name();
					}
					break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Attach_stmtContext extends ParserRuleContext {
		public TerminalNode K_ATTACH() { return getToken(SQLiteParser.K_ATTACH, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_DATABASE() { return getToken(SQLiteParser.K_DATABASE, 0); }
		public Attach_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attach_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAttach_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAttach_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitAttach_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Attach_stmtContext attach_stmt() throws RecognitionException {
		Attach_stmtContext _localctx = new Attach_stmtContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_attach_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(268);
				match(K_ATTACH);
				setState(270);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
					case 1:
					{
						setState(269);
						match(K_DATABASE);
					}
					break;
				}
				setState(272);
				expr(0);
				setState(273);
				match(K_AS);
				setState(274);
				database_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Begin_stmtContext extends ParserRuleContext {
		public TerminalNode K_BEGIN() { return getToken(SQLiteParser.K_BEGIN, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public TerminalNode K_DEFERRED() { return getToken(SQLiteParser.K_DEFERRED, 0); }
		public TerminalNode K_IMMEDIATE() { return getToken(SQLiteParser.K_IMMEDIATE, 0); }
		public TerminalNode K_EXCLUSIVE() { return getToken(SQLiteParser.K_EXCLUSIVE, 0); }
		public Transaction_nameContext transaction_name() {
			return getRuleContext(Transaction_nameContext.class,0);
		}
		public Begin_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_begin_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterBegin_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitBegin_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitBegin_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Begin_stmtContext begin_stmt() throws RecognitionException {
		Begin_stmtContext _localctx = new Begin_stmtContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_begin_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(276);
				match(K_BEGIN);
				setState(278);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 58)) & ~0x3f) == 0 && ((1L << (_la - 58)) & ((1L << (K_DEFERRED - 58)) | (1L << (K_EXCLUSIVE - 58)) | (1L << (K_IMMEDIATE - 58)))) != 0)) {
					{
						setState(277);
						_la = _input.LA(1);
						if ( !(((((_la - 58)) & ~0x3f) == 0 && ((1L << (_la - 58)) & ((1L << (K_DEFERRED - 58)) | (1L << (K_EXCLUSIVE - 58)) | (1L << (K_IMMEDIATE - 58)))) != 0)) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

				setState(284);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_TRANSACTION) {
					{
						setState(280);
						match(K_TRANSACTION);
						setState(282);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
							case 1:
							{
								setState(281);
								transaction_name();
							}
							break;
						}
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Commit_stmtContext extends ParserRuleContext {
		public TerminalNode K_COMMIT() { return getToken(SQLiteParser.K_COMMIT, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public Transaction_nameContext transaction_name() {
			return getRuleContext(Transaction_nameContext.class,0);
		}
		public Commit_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commit_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCommit_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCommit_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCommit_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Commit_stmtContext commit_stmt() throws RecognitionException {
		Commit_stmtContext _localctx = new Commit_stmtContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_commit_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(286);
				_la = _input.LA(1);
				if ( !(_la==K_COMMIT || _la==K_END) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(291);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_TRANSACTION) {
					{
						setState(287);
						match(K_TRANSACTION);
						setState(289);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
							case 1:
							{
								setState(288);
								transaction_name();
							}
							break;
						}
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Compound_select_stmtContext extends ParserRuleContext {
		public List<Select_coreContext> select_core() {
			return getRuleContexts(Select_coreContext.class);
		}
		public Select_coreContext select_core(int i) {
			return getRuleContext(Select_coreContext.class,i);
		}
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public List<Common_table_expressionContext> common_table_expression() {
			return getRuleContexts(Common_table_expressionContext.class);
		}
		public Common_table_expressionContext common_table_expression(int i) {
			return getRuleContext(Common_table_expressionContext.class,i);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> K_UNION() { return getTokens(SQLiteParser.K_UNION); }
		public TerminalNode K_UNION(int i) {
			return getToken(SQLiteParser.K_UNION, i);
		}
		public List<TerminalNode> K_INTERSECT() { return getTokens(SQLiteParser.K_INTERSECT); }
		public TerminalNode K_INTERSECT(int i) {
			return getToken(SQLiteParser.K_INTERSECT, i);
		}
		public List<TerminalNode> K_EXCEPT() { return getTokens(SQLiteParser.K_EXCEPT); }
		public TerminalNode K_EXCEPT(int i) {
			return getToken(SQLiteParser.K_EXCEPT, i);
		}
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public List<TerminalNode> K_ALL() { return getTokens(SQLiteParser.K_ALL); }
		public TerminalNode K_ALL(int i) {
			return getToken(SQLiteParser.K_ALL, i);
		}
		public Compound_select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compound_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCompound_select_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCompound_select_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCompound_select_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Compound_select_stmtContext compound_select_stmt() throws RecognitionException {
		Compound_select_stmtContext _localctx = new Compound_select_stmtContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_compound_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(305);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(293);
						match(K_WITH);
						setState(295);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
							case 1:
							{
								setState(294);
								match(K_RECURSIVE);
							}
							break;
						}
						setState(297);
						common_table_expression();
						setState(302);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(298);
									match(COMMA);
									setState(299);
									common_table_expression();
								}
							}
							setState(304);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(307);
				select_core();
				setState(317);
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
						{
							setState(314);
							_errHandler.sync(this);
							switch (_input.LA(1)) {
								case K_UNION:
								{
									setState(308);
									match(K_UNION);
									setState(310);
									_errHandler.sync(this);
									_la = _input.LA(1);
									if (_la==K_ALL) {
										{
											setState(309);
											match(K_ALL);
										}
									}

								}
								break;
								case K_INTERSECT:
								{
									setState(312);
									match(K_INTERSECT);
								}
								break;
								case K_EXCEPT:
								{
									setState(313);
									match(K_EXCEPT);
								}
								break;
								default:
									throw new NoViableAltException(this);
							}
							setState(316);
							select_core();
						}
					}
					setState(319);
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==K_EXCEPT || _la==K_INTERSECT || _la==K_UNION );
				setState(331);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ORDER) {
					{
						setState(321);
						match(K_ORDER);
						setState(322);
						match(K_BY);
						setState(323);
						ordering_term();
						setState(328);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(324);
									match(COMMA);
									setState(325);
									ordering_term();
								}
							}
							setState(330);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(339);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
						setState(333);
						match(K_LIMIT);
						setState(334);
						expr(0);
						setState(337);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA || _la==K_OFFSET) {
							{
								setState(335);
								_la = _input.LA(1);
								if ( !(_la==COMMA || _la==K_OFFSET) ) {
									_errHandler.recoverInline(this);
								}
								else {
									if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
									_errHandler.reportMatch(this);
									consume();
								}
								setState(336);
								expr(0);
							}
						}

					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_index_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_INDEX() { return getToken(SQLiteParser.K_INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<Indexed_columnContext> indexed_column() {
			return getRuleContexts(Indexed_columnContext.class);
		}
		public Indexed_columnContext indexed_column(int i) {
			return getRuleContext(Indexed_columnContext.class,i);
		}
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Create_index_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_index_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_index_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_index_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCreate_index_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_index_stmtContext create_index_stmt() throws RecognitionException {
		Create_index_stmtContext _localctx = new Create_index_stmtContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_create_index_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(341);
				match(K_CREATE);
				setState(343);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_UNIQUE) {
					{
						setState(342);
						match(K_UNIQUE);
					}
				}

				setState(345);
				match(K_INDEX);
				setState(349);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
					case 1:
					{
						setState(346);
						match(K_IF);
						setState(347);
						match(K_NOT);
						setState(348);
						match(K_EXISTS);
					}
					break;
				}
				setState(354);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
					case 1:
					{
						setState(351);
						database_name();
						setState(352);
						match(DOT);
					}
					break;
				}
				setState(356);
				index_name();
				setState(357);
				match(K_ON);
				setState(358);
				table_name();
				setState(359);
				match(OPEN_PAR);
				setState(360);
				indexed_column();
				setState(365);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
						{
							setState(361);
							match(COMMA);
							setState(362);
							indexed_column();
						}
					}
					setState(367);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(368);
				match(CLOSE_PAR);
				setState(371);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHERE) {
					{
						setState(369);
						match(K_WHERE);
						setState(370);
						expr(0);
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<Column_defContext> column_def() {
			return getRuleContexts(Column_defContext.class);
		}
		public Column_defContext column_def(int i) {
			return getRuleContext(Column_defContext.class,i);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public List<Table_constraintContext> table_constraint() {
			return getRuleContexts(Table_constraintContext.class);
		}
		public Table_constraintContext table_constraint(int i) {
			return getRuleContext(Table_constraintContext.class,i);
		}
		public TerminalNode K_WITHOUT() { return getToken(SQLiteParser.K_WITHOUT, 0); }
		public TerminalNode IDENTIFIER() { return getToken(SQLiteParser.IDENTIFIER, 0); }
		public Create_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_table_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCreate_table_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_table_stmtContext create_table_stmt() throws RecognitionException {
		Create_table_stmtContext _localctx = new Create_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_create_table_stmt);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(373);
				match(K_CREATE);
				setState(375);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_TEMP || _la==K_TEMPORARY) {
					{
						setState(374);
						_la = _input.LA(1);
						if ( !(_la==K_TEMP || _la==K_TEMPORARY) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

				setState(377);
				match(K_TABLE);
				setState(381);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
					case 1:
					{
						setState(378);
						match(K_IF);
						setState(379);
						match(K_NOT);
						setState(380);
						match(K_EXISTS);
					}
					break;
				}
				setState(386);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
					case 1:
					{
						setState(383);
						database_name();
						setState(384);
						match(DOT);
					}
					break;
				}
				setState(388);
				table_name();
				setState(412);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case OPEN_PAR:
					{
						setState(389);
						match(OPEN_PAR);
						setState(390);
						column_def();
						setState(395);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,37,_ctx);
						while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
							if ( _alt==1 ) {
								{
									{
										setState(391);
										match(COMMA);
										setState(392);
										column_def();
									}
								}
							}
							setState(397);
							_errHandler.sync(this);
							_alt = getInterpreter().adaptivePredict(_input,37,_ctx);
						}
						setState(402);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(398);
									match(COMMA);
									setState(399);
									table_constraint();
								}
							}
							setState(404);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(405);
						match(CLOSE_PAR);
						setState(408);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_WITHOUT) {
							{
								setState(406);
								match(K_WITHOUT);
								setState(407);
								match(IDENTIFIER);
							}
						}

					}
					break;
					case K_AS:
					{
						setState(410);
						match(K_AS);
						setState(411);
						select_stmt();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_trigger_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_TRIGGER() { return getToken(SQLiteParser.K_TRIGGER, 0); }
		public Trigger_nameContext trigger_name() {
			return getRuleContext(Trigger_nameContext.class,0);
		}
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_BEGIN() { return getToken(SQLiteParser.K_BEGIN, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_INSERT() { return getToken(SQLiteParser.K_INSERT, 0); }
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public List<Database_nameContext> database_name() {
			return getRuleContexts(Database_nameContext.class);
		}
		public Database_nameContext database_name(int i) {
			return getRuleContext(Database_nameContext.class,i);
		}
		public TerminalNode K_BEFORE() { return getToken(SQLiteParser.K_BEFORE, 0); }
		public TerminalNode K_AFTER() { return getToken(SQLiteParser.K_AFTER, 0); }
		public TerminalNode K_INSTEAD() { return getToken(SQLiteParser.K_INSTEAD, 0); }
		public List<TerminalNode> K_OF() { return getTokens(SQLiteParser.K_OF); }
		public TerminalNode K_OF(int i) {
			return getToken(SQLiteParser.K_OF, i);
		}
		public TerminalNode K_FOR() { return getToken(SQLiteParser.K_FOR, 0); }
		public TerminalNode K_EACH() { return getToken(SQLiteParser.K_EACH, 0); }
		public TerminalNode K_ROW() { return getToken(SQLiteParser.K_ROW, 0); }
		public TerminalNode K_WHEN() { return getToken(SQLiteParser.K_WHEN, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<Update_stmtContext> update_stmt() {
			return getRuleContexts(Update_stmtContext.class);
		}
		public Update_stmtContext update_stmt(int i) {
			return getRuleContext(Update_stmtContext.class,i);
		}
		public List<Insert_stmtContext> insert_stmt() {
			return getRuleContexts(Insert_stmtContext.class);
		}
		public Insert_stmtContext insert_stmt(int i) {
			return getRuleContext(Insert_stmtContext.class,i);
		}
		public List<Delete_stmtContext> delete_stmt() {
			return getRuleContexts(Delete_stmtContext.class);
		}
		public Delete_stmtContext delete_stmt(int i) {
			return getRuleContext(Delete_stmtContext.class,i);
		}
		public List<Select_stmtContext> select_stmt() {
			return getRuleContexts(Select_stmtContext.class);
		}
		public Select_stmtContext select_stmt(int i) {
			return getRuleContext(Select_stmtContext.class,i);
		}
		public Create_trigger_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_trigger_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_trigger_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_trigger_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCreate_trigger_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_trigger_stmtContext create_trigger_stmt() throws RecognitionException {
		Create_trigger_stmtContext _localctx = new Create_trigger_stmtContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_create_trigger_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(414);
				match(K_CREATE);
				setState(416);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_TEMP || _la==K_TEMPORARY) {
					{
						setState(415);
						_la = _input.LA(1);
						if ( !(_la==K_TEMP || _la==K_TEMPORARY) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

				setState(418);
				match(K_TRIGGER);
				setState(422);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
					case 1:
					{
						setState(419);
						match(K_IF);
						setState(420);
						match(K_NOT);
						setState(421);
						match(K_EXISTS);
					}
					break;
				}
				setState(427);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
					case 1:
					{
						setState(424);
						database_name();
						setState(425);
						match(DOT);
					}
					break;
				}
				setState(429);
				trigger_name();
				setState(434);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_BEFORE:
					{
						setState(430);
						match(K_BEFORE);
					}
					break;
					case K_AFTER:
					{
						setState(431);
						match(K_AFTER);
					}
					break;
					case K_INSTEAD:
					{
						setState(432);
						match(K_INSTEAD);
						setState(433);
						match(K_OF);
					}
					break;
					case K_DELETE:
					case K_INSERT:
					case K_UPDATE:
						break;
					default:
						break;
				}
				setState(450);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_DELETE:
					{
						setState(436);
						match(K_DELETE);
					}
					break;
					case K_INSERT:
					{
						setState(437);
						match(K_INSERT);
					}
					break;
					case K_UPDATE:
					{
						setState(438);
						match(K_UPDATE);
						setState(448);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_OF) {
							{
								setState(439);
								match(K_OF);
								setState(440);
								column_name();
								setState(445);
								_errHandler.sync(this);
								_la = _input.LA(1);
								while (_la==COMMA) {
									{
										{
											setState(441);
											match(COMMA);
											setState(442);
											column_name();
										}
									}
									setState(447);
									_errHandler.sync(this);
									_la = _input.LA(1);
								}
							}
						}

					}
					break;
					default:
						throw new NoViableAltException(this);
				}
				setState(452);
				match(K_ON);
				setState(456);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
					case 1:
					{
						setState(453);
						database_name();
						setState(454);
						match(DOT);
					}
					break;
				}
				setState(458);
				table_name();
				setState(462);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_FOR) {
					{
						setState(459);
						match(K_FOR);
						setState(460);
						match(K_EACH);
						setState(461);
						match(K_ROW);
					}
				}

				setState(466);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHEN) {
					{
						setState(464);
						match(K_WHEN);
						setState(465);
						expr(0);
					}
				}

				setState(468);
				match(K_BEGIN);
				setState(477);
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
						{
							setState(473);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
								case 1:
								{
									setState(469);
									update_stmt();
								}
								break;
								case 2:
								{
									setState(470);
									insert_stmt();
								}
								break;
								case 3:
								{
									setState(471);
									delete_stmt();
								}
								break;
								case 4:
								{
									setState(472);
									select_stmt();
								}
								break;
							}
							setState(475);
							match(SCOL);
						}
					}
					setState(479);
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==K_DELETE || ((((_la - 88)) & ~0x3f) == 0 && ((1L << (_la - 88)) & ((1L << (K_INSERT - 88)) | (1L << (K_REPLACE - 88)) | (1L << (K_SELECT - 88)) | (1L << (K_UPDATE - 88)) | (1L << (K_VALUES - 88)) | (1L << (K_WITH - 88)))) != 0) );
				setState(481);
				match(K_END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_view_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLiteParser.K_VIEW, 0); }
		public View_nameContext view_name() {
			return getRuleContext(View_nameContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public Create_view_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_view_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_view_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_view_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCreate_view_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_view_stmtContext create_view_stmt() throws RecognitionException {
		Create_view_stmtContext _localctx = new Create_view_stmtContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_create_view_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(483);
				match(K_CREATE);
				setState(485);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_TEMP || _la==K_TEMPORARY) {
					{
						setState(484);
						_la = _input.LA(1);
						if ( !(_la==K_TEMP || _la==K_TEMPORARY) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

				setState(487);
				match(K_VIEW);
				setState(491);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
					case 1:
					{
						setState(488);
						match(K_IF);
						setState(489);
						match(K_NOT);
						setState(490);
						match(K_EXISTS);
					}
					break;
				}
				setState(496);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
					case 1:
					{
						setState(493);
						database_name();
						setState(494);
						match(DOT);
					}
					break;
				}
				setState(498);
				view_name();
				setState(499);
				match(K_AS);
				setState(500);
				select_stmt();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Create_virtual_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_VIRTUAL() { return getToken(SQLiteParser.K_VIRTUAL, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_USING() { return getToken(SQLiteParser.K_USING, 0); }
		public Module_nameContext module_name() {
			return getRuleContext(Module_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public List<Module_argumentContext> module_argument() {
			return getRuleContexts(Module_argumentContext.class);
		}
		public Module_argumentContext module_argument(int i) {
			return getRuleContext(Module_argumentContext.class,i);
		}
		public Create_virtual_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_virtual_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCreate_virtual_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCreate_virtual_table_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCreate_virtual_table_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_virtual_table_stmtContext create_virtual_table_stmt() throws RecognitionException {
		Create_virtual_table_stmtContext _localctx = new Create_virtual_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_create_virtual_table_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(502);
				match(K_CREATE);
				setState(503);
				match(K_VIRTUAL);
				setState(504);
				match(K_TABLE);
				setState(508);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
					case 1:
					{
						setState(505);
						match(K_IF);
						setState(506);
						match(K_NOT);
						setState(507);
						match(K_EXISTS);
					}
					break;
				}
				setState(513);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
					case 1:
					{
						setState(510);
						database_name();
						setState(511);
						match(DOT);
					}
					break;
				}
				setState(515);
				table_name();
				setState(516);
				match(K_USING);
				setState(517);
				module_name();
				setState(529);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR) {
					{
						setState(518);
						match(OPEN_PAR);
						setState(519);
						module_argument();
						setState(524);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(520);
									match(COMMA);
									setState(521);
									module_argument();
								}
							}
							setState(526);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(527);
						match(CLOSE_PAR);
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Delete_stmtContext extends ParserRuleContext {
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Delete_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDelete_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDelete_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDelete_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Delete_stmtContext delete_stmt() throws RecognitionException {
		Delete_stmtContext _localctx = new Delete_stmtContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_delete_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(532);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(531);
						with_clause();
					}
				}

				setState(534);
				match(K_DELETE);
				setState(535);
				match(K_FROM);
				setState(536);
				qualified_table_name();
				setState(539);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHERE) {
					{
						setState(537);
						match(K_WHERE);
						setState(538);
						expr(0);
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Delete_stmt_limitedContext extends ParserRuleContext {
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Delete_stmt_limitedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_stmt_limited; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDelete_stmt_limited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDelete_stmt_limited(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDelete_stmt_limited(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Delete_stmt_limitedContext delete_stmt_limited() throws RecognitionException {
		Delete_stmt_limitedContext _localctx = new Delete_stmt_limitedContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_delete_stmt_limited);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(542);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(541);
						with_clause();
					}
				}

				setState(544);
				match(K_DELETE);
				setState(545);
				match(K_FROM);
				setState(546);
				qualified_table_name();
				setState(549);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHERE) {
					{
						setState(547);
						match(K_WHERE);
						setState(548);
						expr(0);
					}
				}

				setState(569);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT || _la==K_ORDER) {
					{
						setState(561);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_ORDER) {
							{
								setState(551);
								match(K_ORDER);
								setState(552);
								match(K_BY);
								setState(553);
								ordering_term();
								setState(558);
								_errHandler.sync(this);
								_la = _input.LA(1);
								while (_la==COMMA) {
									{
										{
											setState(554);
											match(COMMA);
											setState(555);
											ordering_term();
										}
									}
									setState(560);
									_errHandler.sync(this);
									_la = _input.LA(1);
								}
							}
						}

						setState(563);
						match(K_LIMIT);
						setState(564);
						expr(0);
						setState(567);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA || _la==K_OFFSET) {
							{
								setState(565);
								_la = _input.LA(1);
								if ( !(_la==COMMA || _la==K_OFFSET) ) {
									_errHandler.recoverInline(this);
								}
								else {
									if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
									_errHandler.reportMatch(this);
									consume();
								}
								setState(566);
								expr(0);
							}
						}

					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Detach_stmtContext extends ParserRuleContext {
		public TerminalNode K_DETACH() { return getToken(SQLiteParser.K_DETACH, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_DATABASE() { return getToken(SQLiteParser.K_DATABASE, 0); }
		public Detach_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_detach_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDetach_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDetach_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDetach_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Detach_stmtContext detach_stmt() throws RecognitionException {
		Detach_stmtContext _localctx = new Detach_stmtContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_detach_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(571);
				match(K_DETACH);
				setState(573);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
					case 1:
					{
						setState(572);
						match(K_DATABASE);
					}
					break;
				}
				setState(575);
				database_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_index_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_INDEX() { return getToken(SQLiteParser.K_INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Drop_index_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_index_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_index_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_index_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDrop_index_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Drop_index_stmtContext drop_index_stmt() throws RecognitionException {
		Drop_index_stmtContext _localctx = new Drop_index_stmtContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_drop_index_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(577);
				match(K_DROP);
				setState(578);
				match(K_INDEX);
				setState(581);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
					case 1:
					{
						setState(579);
						match(K_IF);
						setState(580);
						match(K_EXISTS);
					}
					break;
				}
				setState(586);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
					case 1:
					{
						setState(583);
						database_name();
						setState(584);
						match(DOT);
					}
					break;
				}
				setState(588);
				index_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_table_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Drop_table_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_table_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_table_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_table_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDrop_table_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Drop_table_stmtContext drop_table_stmt() throws RecognitionException {
		Drop_table_stmtContext _localctx = new Drop_table_stmtContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_drop_table_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(590);
				match(K_DROP);
				setState(591);
				match(K_TABLE);
				setState(594);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
					case 1:
					{
						setState(592);
						match(K_IF);
						setState(593);
						match(K_EXISTS);
					}
					break;
				}
				setState(599);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
					case 1:
					{
						setState(596);
						database_name();
						setState(597);
						match(DOT);
					}
					break;
				}
				setState(601);
				table_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_trigger_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_TRIGGER() { return getToken(SQLiteParser.K_TRIGGER, 0); }
		public Trigger_nameContext trigger_name() {
			return getRuleContext(Trigger_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Drop_trigger_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_trigger_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_trigger_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_trigger_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDrop_trigger_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Drop_trigger_stmtContext drop_trigger_stmt() throws RecognitionException {
		Drop_trigger_stmtContext _localctx = new Drop_trigger_stmtContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_drop_trigger_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(603);
				match(K_DROP);
				setState(604);
				match(K_TRIGGER);
				setState(607);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
					case 1:
					{
						setState(605);
						match(K_IF);
						setState(606);
						match(K_EXISTS);
					}
					break;
				}
				setState(612);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
					case 1:
					{
						setState(609);
						database_name();
						setState(610);
						match(DOT);
					}
					break;
				}
				setState(614);
				trigger_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Drop_view_stmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLiteParser.K_VIEW, 0); }
		public View_nameContext view_name() {
			return getRuleContext(View_nameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Drop_view_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_view_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDrop_view_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDrop_view_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDrop_view_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Drop_view_stmtContext drop_view_stmt() throws RecognitionException {
		Drop_view_stmtContext _localctx = new Drop_view_stmtContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_drop_view_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(616);
				match(K_DROP);
				setState(617);
				match(K_VIEW);
				setState(620);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
					case 1:
					{
						setState(618);
						match(K_IF);
						setState(619);
						match(K_EXISTS);
					}
					break;
				}
				setState(625);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
					case 1:
					{
						setState(622);
						database_name();
						setState(623);
						match(DOT);
					}
					break;
				}
				setState(627);
				view_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Factored_select_stmtContext extends ParserRuleContext {
		public List<Select_coreContext> select_core() {
			return getRuleContexts(Select_coreContext.class);
		}
		public Select_coreContext select_core(int i) {
			return getRuleContext(Select_coreContext.class,i);
		}
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public List<Common_table_expressionContext> common_table_expression() {
			return getRuleContexts(Common_table_expressionContext.class);
		}
		public Common_table_expressionContext common_table_expression(int i) {
			return getRuleContext(Common_table_expressionContext.class,i);
		}
		public List<Compound_operatorContext> compound_operator() {
			return getRuleContexts(Compound_operatorContext.class);
		}
		public Compound_operatorContext compound_operator(int i) {
			return getRuleContext(Compound_operatorContext.class,i);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Factored_select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_factored_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterFactored_select_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitFactored_select_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitFactored_select_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Factored_select_stmtContext factored_select_stmt() throws RecognitionException {
		Factored_select_stmtContext _localctx = new Factored_select_stmtContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_factored_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(641);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(629);
						match(K_WITH);
						setState(631);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
							case 1:
							{
								setState(630);
								match(K_RECURSIVE);
							}
							break;
						}
						setState(633);
						common_table_expression();
						setState(638);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(634);
									match(COMMA);
									setState(635);
									common_table_expression();
								}
							}
							setState(640);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(643);
				select_core();
				setState(649);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==K_EXCEPT || _la==K_INTERSECT || _la==K_UNION) {
					{
						{
							setState(644);
							compound_operator();
							setState(645);
							select_core();
						}
					}
					setState(651);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(662);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ORDER) {
					{
						setState(652);
						match(K_ORDER);
						setState(653);
						match(K_BY);
						setState(654);
						ordering_term();
						setState(659);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(655);
									match(COMMA);
									setState(656);
									ordering_term();
								}
							}
							setState(661);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(670);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
						setState(664);
						match(K_LIMIT);
						setState(665);
						expr(0);
						setState(668);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA || _la==K_OFFSET) {
							{
								setState(666);
								_la = _input.LA(1);
								if ( !(_la==COMMA || _la==K_OFFSET) ) {
									_errHandler.recoverInline(this);
								}
								else {
									if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
									_errHandler.reportMatch(this);
									consume();
								}
								setState(667);
								expr(0);
							}
						}

					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Insert_stmtContext extends ParserRuleContext {
		public TerminalNode K_INTO() { return getToken(SQLiteParser.K_INTO, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_INSERT() { return getToken(SQLiteParser.K_INSERT, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_DEFAULT() { return getToken(SQLiteParser.K_DEFAULT, 0); }
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Insert_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterInsert_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitInsert_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitInsert_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Insert_stmtContext insert_stmt() throws RecognitionException {
		Insert_stmtContext _localctx = new Insert_stmtContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_insert_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(673);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(672);
						with_clause();
					}
				}

				setState(692);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
					case 1:
					{
						setState(675);
						match(K_INSERT);
					}
					break;
					case 2:
					{
						setState(676);
						match(K_REPLACE);
					}
					break;
					case 3:
					{
						setState(677);
						match(K_INSERT);
						setState(678);
						match(K_OR);
						setState(679);
						match(K_REPLACE);
					}
					break;
					case 4:
					{
						setState(680);
						match(K_INSERT);
						setState(681);
						match(K_OR);
						setState(682);
						match(K_ROLLBACK);
					}
					break;
					case 5:
					{
						setState(683);
						match(K_INSERT);
						setState(684);
						match(K_OR);
						setState(685);
						match(K_ABORT);
					}
					break;
					case 6:
					{
						setState(686);
						match(K_INSERT);
						setState(687);
						match(K_OR);
						setState(688);
						match(K_FAIL);
					}
					break;
					case 7:
					{
						setState(689);
						match(K_INSERT);
						setState(690);
						match(K_OR);
						setState(691);
						match(K_IGNORE);
					}
					break;
				}
				setState(694);
				match(K_INTO);
				setState(698);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
					case 1:
					{
						setState(695);
						database_name();
						setState(696);
						match(DOT);
					}
					break;
				}
				setState(700);
				table_name();
				setState(712);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR) {
					{
						setState(701);
						match(OPEN_PAR);
						setState(702);
						column_name();
						setState(707);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(703);
									match(COMMA);
									setState(704);
									column_name();
								}
							}
							setState(709);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(710);
						match(CLOSE_PAR);
					}
				}

				setState(745);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
					case 1:
					{
						setState(714);
						match(K_VALUES);
						setState(715);
						match(OPEN_PAR);
						setState(716);
						expr(0);
						setState(721);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(717);
									match(COMMA);
									setState(718);
									expr(0);
								}
							}
							setState(723);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(724);
						match(CLOSE_PAR);
						setState(739);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(725);
									match(COMMA);
									setState(726);
									match(OPEN_PAR);
									setState(727);
									expr(0);
									setState(732);
									_errHandler.sync(this);
									_la = _input.LA(1);
									while (_la==COMMA) {
										{
											{
												setState(728);
												match(COMMA);
												setState(729);
												expr(0);
											}
										}
										setState(734);
										_errHandler.sync(this);
										_la = _input.LA(1);
									}
									setState(735);
									match(CLOSE_PAR);
								}
							}
							setState(741);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
					break;
					case 2:
					{
						setState(742);
						select_stmt();
					}
					break;
					case 3:
					{
						setState(743);
						match(K_DEFAULT);
						setState(744);
						match(K_VALUES);
					}
					break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Pragma_stmtContext extends ParserRuleContext {
		public TerminalNode K_PRAGMA() { return getToken(SQLiteParser.K_PRAGMA, 0); }
		public Pragma_nameContext pragma_name() {
			return getRuleContext(Pragma_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Pragma_valueContext pragma_value() {
			return getRuleContext(Pragma_valueContext.class,0);
		}
		public Pragma_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pragma_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterPragma_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitPragma_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitPragma_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Pragma_stmtContext pragma_stmt() throws RecognitionException {
		Pragma_stmtContext _localctx = new Pragma_stmtContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_pragma_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(747);
				match(K_PRAGMA);
				setState(751);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
					case 1:
					{
						setState(748);
						database_name();
						setState(749);
						match(DOT);
					}
					break;
				}
				setState(753);
				pragma_name();
				setState(760);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case ASSIGN:
					{
						setState(754);
						match(ASSIGN);
						setState(755);
						pragma_value();
					}
					break;
					case OPEN_PAR:
					{
						setState(756);
						match(OPEN_PAR);
						setState(757);
						pragma_value();
						setState(758);
						match(CLOSE_PAR);
					}
					break;
					case EOF:
					case SCOL:
					case K_ALTER:
					case K_ANALYZE:
					case K_ATTACH:
					case K_BEGIN:
					case K_COMMIT:
					case K_CREATE:
					case K_DELETE:
					case K_DETACH:
					case K_DROP:
					case K_END:
					case K_EXPLAIN:
					case K_INSERT:
					case K_PRAGMA:
					case K_REINDEX:
					case K_RELEASE:
					case K_REPLACE:
					case K_ROLLBACK:
					case K_SAVEPOINT:
					case K_SELECT:
					case K_UPDATE:
					case K_VACUUM:
					case K_VALUES:
					case K_WITH:
					case UNEXPECTED_CHAR:
						break;
					default:
						break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Reindex_stmtContext extends ParserRuleContext {
		public TerminalNode K_REINDEX() { return getToken(SQLiteParser.K_REINDEX, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Reindex_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_reindex_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterReindex_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitReindex_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitReindex_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Reindex_stmtContext reindex_stmt() throws RecognitionException {
		Reindex_stmtContext _localctx = new Reindex_stmtContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_reindex_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(762);
				match(K_REINDEX);
				setState(773);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
					case 1:
					{
						setState(763);
						collation_name();
					}
					break;
					case 2:
					{
						setState(767);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
							case 1:
							{
								setState(764);
								database_name();
								setState(765);
								match(DOT);
							}
							break;
						}
						setState(771);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
							case 1:
							{
								setState(769);
								table_name();
							}
							break;
							case 2:
							{
								setState(770);
								index_name();
							}
							break;
						}
					}
					break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Release_stmtContext extends ParserRuleContext {
		public TerminalNode K_RELEASE() { return getToken(SQLiteParser.K_RELEASE, 0); }
		public Savepoint_nameContext savepoint_name() {
			return getRuleContext(Savepoint_nameContext.class,0);
		}
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public Release_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_release_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterRelease_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitRelease_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitRelease_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Release_stmtContext release_stmt() throws RecognitionException {
		Release_stmtContext _localctx = new Release_stmtContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_release_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(775);
				match(K_RELEASE);
				setState(777);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
					case 1:
					{
						setState(776);
						match(K_SAVEPOINT);
					}
					break;
				}
				setState(779);
				savepoint_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Rollback_stmtContext extends ParserRuleContext {
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public TerminalNode K_TO() { return getToken(SQLiteParser.K_TO, 0); }
		public Savepoint_nameContext savepoint_name() {
			return getRuleContext(Savepoint_nameContext.class,0);
		}
		public Transaction_nameContext transaction_name() {
			return getRuleContext(Transaction_nameContext.class,0);
		}
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public Rollback_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rollback_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterRollback_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitRollback_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitRollback_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Rollback_stmtContext rollback_stmt() throws RecognitionException {
		Rollback_stmtContext _localctx = new Rollback_stmtContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_rollback_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(781);
				match(K_ROLLBACK);
				setState(786);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_TRANSACTION) {
					{
						setState(782);
						match(K_TRANSACTION);
						setState(784);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
							case 1:
							{
								setState(783);
								transaction_name();
							}
							break;
						}
					}
				}

				setState(793);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_TO) {
					{
						setState(788);
						match(K_TO);
						setState(790);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,102,_ctx) ) {
							case 1:
							{
								setState(789);
								match(K_SAVEPOINT);
							}
							break;
						}
						setState(792);
						savepoint_name();
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Savepoint_stmtContext extends ParserRuleContext {
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public Savepoint_nameContext savepoint_name() {
			return getRuleContext(Savepoint_nameContext.class,0);
		}
		public Savepoint_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_savepoint_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSavepoint_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSavepoint_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSavepoint_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Savepoint_stmtContext savepoint_stmt() throws RecognitionException {
		Savepoint_stmtContext _localctx = new Savepoint_stmtContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_savepoint_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(795);
				match(K_SAVEPOINT);
				setState(796);
				savepoint_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Simple_select_stmtContext extends ParserRuleContext {
		public Select_coreContext select_core() {
			return getRuleContext(Select_coreContext.class,0);
		}
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public List<Common_table_expressionContext> common_table_expression() {
			return getRuleContexts(Common_table_expressionContext.class);
		}
		public Common_table_expressionContext common_table_expression(int i) {
			return getRuleContext(Common_table_expressionContext.class,i);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Simple_select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simple_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSimple_select_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSimple_select_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSimple_select_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Simple_select_stmtContext simple_select_stmt() throws RecognitionException {
		Simple_select_stmtContext _localctx = new Simple_select_stmtContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_simple_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(810);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(798);
						match(K_WITH);
						setState(800);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,104,_ctx) ) {
							case 1:
							{
								setState(799);
								match(K_RECURSIVE);
							}
							break;
						}
						setState(802);
						common_table_expression();
						setState(807);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(803);
									match(COMMA);
									setState(804);
									common_table_expression();
								}
							}
							setState(809);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(812);
				select_core();
				setState(823);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ORDER) {
					{
						setState(813);
						match(K_ORDER);
						setState(814);
						match(K_BY);
						setState(815);
						ordering_term();
						setState(820);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(816);
									match(COMMA);
									setState(817);
									ordering_term();
								}
							}
							setState(822);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(831);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
						setState(825);
						match(K_LIMIT);
						setState(826);
						expr(0);
						setState(829);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA || _la==K_OFFSET) {
							{
								setState(827);
								_la = _input.LA(1);
								if ( !(_la==COMMA || _la==K_OFFSET) ) {
									_errHandler.recoverInline(this);
								}
								else {
									if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
									_errHandler.reportMatch(this);
									consume();
								}
								setState(828);
								expr(0);
							}
						}

					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_stmtContext extends ParserRuleContext {
		public List<Select_or_valuesContext> select_or_values() {
			return getRuleContexts(Select_or_valuesContext.class);
		}
		public Select_or_valuesContext select_or_values(int i) {
			return getRuleContext(Select_or_valuesContext.class,i);
		}
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public List<Common_table_expressionContext> common_table_expression() {
			return getRuleContexts(Common_table_expressionContext.class);
		}
		public Common_table_expressionContext common_table_expression(int i) {
			return getRuleContext(Common_table_expressionContext.class,i);
		}
		public List<Compound_operatorContext> compound_operator() {
			return getRuleContexts(Compound_operatorContext.class);
		}
		public Compound_operatorContext compound_operator(int i) {
			return getRuleContext(Compound_operatorContext.class,i);
		}
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Select_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSelect_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSelect_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSelect_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_stmtContext select_stmt() throws RecognitionException {
		Select_stmtContext _localctx = new Select_stmtContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_select_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(845);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(833);
						match(K_WITH);
						setState(835);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
							case 1:
							{
								setState(834);
								match(K_RECURSIVE);
							}
							break;
						}
						setState(837);
						common_table_expression();
						setState(842);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(838);
									match(COMMA);
									setState(839);
									common_table_expression();
								}
							}
							setState(844);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(847);
				select_or_values();
				setState(853);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==K_EXCEPT || _la==K_INTERSECT || _la==K_UNION) {
					{
						{
							setState(848);
							compound_operator();
							setState(849);
							select_or_values();
						}
					}
					setState(855);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(866);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ORDER) {
					{
						setState(856);
						match(K_ORDER);
						setState(857);
						match(K_BY);
						setState(858);
						ordering_term();
						setState(863);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(859);
									match(COMMA);
									setState(860);
									ordering_term();
								}
							}
							setState(865);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
					}
				}

				setState(874);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT) {
					{
						setState(868);
						match(K_LIMIT);
						setState(869);
						expr(0);
						setState(872);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA || _la==K_OFFSET) {
							{
								setState(870);
								_la = _input.LA(1);
								if ( !(_la==COMMA || _la==K_OFFSET) ) {
									_errHandler.recoverInline(this);
								}
								else {
									if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
									_errHandler.reportMatch(this);
									consume();
								}
								setState(871);
								expr(0);
							}
						}

					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_or_valuesContext extends ParserRuleContext {
		public TerminalNode K_SELECT() { return getToken(SQLiteParser.K_SELECT, 0); }
		public List<Result_columnContext> result_column() {
			return getRuleContexts(Result_columnContext.class);
		}
		public Result_columnContext result_column(int i) {
			return getRuleContext(Result_columnContext.class,i);
		}
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_GROUP() { return getToken(SQLiteParser.K_GROUP, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public Group_byContext group_by() {
			return getRuleContext(Group_byContext.class,0);
		}
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public Join_clauseContext join_clause() {
			return getRuleContext(Join_clauseContext.class,0);
		}
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public Select_or_valuesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_or_values; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSelect_or_values(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSelect_or_values(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSelect_or_values(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_or_valuesContext select_or_values() throws RecognitionException {
		Select_or_valuesContext _localctx = new Select_or_valuesContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_select_or_values);
		int _la;
		try {
			setState(939);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case K_SELECT:
					enterOuterAlt(_localctx, 1);
				{
					setState(876);
					match(K_SELECT);
					setState(878);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,119,_ctx) ) {
						case 1:
						{
							setState(877);
							_la = _input.LA(1);
							if ( !(_la==K_ALL || _la==K_DISTINCT) ) {
								_errHandler.recoverInline(this);
							}
							else {
								if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
								_errHandler.reportMatch(this);
								consume();
							}
						}
						break;
					}
					setState(880);
					result_column();
					setState(885);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
							{
								setState(881);
								match(COMMA);
								setState(882);
								result_column();
							}
						}
						setState(887);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(900);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_FROM) {
						{
							setState(888);
							match(K_FROM);
							setState(898);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,122,_ctx) ) {
								case 1:
								{
									setState(889);
									table_or_subquery();
									setState(894);
									_errHandler.sync(this);
									_la = _input.LA(1);
									while (_la==COMMA) {
										{
											{
												setState(890);
												match(COMMA);
												setState(891);
												table_or_subquery();
											}
										}
										setState(896);
										_errHandler.sync(this);
										_la = _input.LA(1);
									}
								}
								break;
								case 2:
								{
									setState(897);
									join_clause();
								}
								break;
							}
						}
					}

					setState(904);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_WHERE) {
						{
							setState(902);
							match(K_WHERE);
							setState(903);
							expr(0);
						}
					}

					setState(909);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_GROUP) {
						{
							setState(906);
							match(K_GROUP);
							setState(907);
							match(K_BY);
							setState(908);
							group_by();
						}
					}

				}
				break;
				case K_VALUES:
					enterOuterAlt(_localctx, 2);
				{
					setState(911);
					match(K_VALUES);
					setState(912);
					match(OPEN_PAR);
					setState(913);
					expr(0);
					setState(918);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
							{
								setState(914);
								match(COMMA);
								setState(915);
								expr(0);
							}
						}
						setState(920);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(921);
					match(CLOSE_PAR);
					setState(936);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
							{
								setState(922);
								match(COMMA);
								setState(923);
								match(OPEN_PAR);
								setState(924);
								expr(0);
								setState(929);
								_errHandler.sync(this);
								_la = _input.LA(1);
								while (_la==COMMA) {
									{
										{
											setState(925);
											match(COMMA);
											setState(926);
											expr(0);
										}
									}
									setState(931);
									_errHandler.sync(this);
									_la = _input.LA(1);
								}
								setState(932);
								match(CLOSE_PAR);
							}
						}
						setState(938);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Update_stmtContext extends ParserRuleContext {
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public TerminalNode K_SET() { return getToken(SQLiteParser.K_SET, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public Update_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterUpdate_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitUpdate_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitUpdate_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Update_stmtContext update_stmt() throws RecognitionException {
		Update_stmtContext _localctx = new Update_stmtContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_update_stmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(942);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(941);
						with_clause();
					}
				}

				setState(944);
				match(K_UPDATE);
				setState(955);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,131,_ctx) ) {
					case 1:
					{
						setState(945);
						match(K_OR);
						setState(946);
						match(K_ROLLBACK);
					}
					break;
					case 2:
					{
						setState(947);
						match(K_OR);
						setState(948);
						match(K_ABORT);
					}
					break;
					case 3:
					{
						setState(949);
						match(K_OR);
						setState(950);
						match(K_REPLACE);
					}
					break;
					case 4:
					{
						setState(951);
						match(K_OR);
						setState(952);
						match(K_FAIL);
					}
					break;
					case 5:
					{
						setState(953);
						match(K_OR);
						setState(954);
						match(K_IGNORE);
					}
					break;
				}
				setState(957);
				qualified_table_name();
				setState(958);
				match(K_SET);
				setState(959);
				column_name();
				setState(960);
				match(ASSIGN);
				setState(961);
				expr(0);
				setState(969);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
						{
							setState(962);
							match(COMMA);
							setState(963);
							column_name();
							setState(964);
							match(ASSIGN);
							setState(965);
							expr(0);
						}
					}
					setState(971);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(974);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHERE) {
					{
						setState(972);
						match(K_WHERE);
						setState(973);
						expr(0);
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Update_stmt_limitedContext extends ParserRuleContext {
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public Qualified_table_nameContext qualified_table_name() {
			return getRuleContext(Qualified_table_nameContext.class,0);
		}
		public TerminalNode K_SET() { return getToken(SQLiteParser.K_SET, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public With_clauseContext with_clause() {
			return getRuleContext(With_clauseContext.class,0);
		}
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public List<Ordering_termContext> ordering_term() {
			return getRuleContexts(Ordering_termContext.class);
		}
		public Ordering_termContext ordering_term(int i) {
			return getRuleContext(Ordering_termContext.class,i);
		}
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public Update_stmt_limitedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_stmt_limited; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterUpdate_stmt_limited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitUpdate_stmt_limited(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitUpdate_stmt_limited(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Update_stmt_limitedContext update_stmt_limited() throws RecognitionException {
		Update_stmt_limitedContext _localctx = new Update_stmt_limitedContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_update_stmt_limited);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(977);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WITH) {
					{
						setState(976);
						with_clause();
					}
				}

				setState(979);
				match(K_UPDATE);
				setState(990);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,135,_ctx) ) {
					case 1:
					{
						setState(980);
						match(K_OR);
						setState(981);
						match(K_ROLLBACK);
					}
					break;
					case 2:
					{
						setState(982);
						match(K_OR);
						setState(983);
						match(K_ABORT);
					}
					break;
					case 3:
					{
						setState(984);
						match(K_OR);
						setState(985);
						match(K_REPLACE);
					}
					break;
					case 4:
					{
						setState(986);
						match(K_OR);
						setState(987);
						match(K_FAIL);
					}
					break;
					case 5:
					{
						setState(988);
						match(K_OR);
						setState(989);
						match(K_IGNORE);
					}
					break;
				}
				setState(992);
				qualified_table_name();
				setState(993);
				match(K_SET);
				setState(994);
				column_name();
				setState(995);
				match(ASSIGN);
				setState(996);
				expr(0);
				setState(1004);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
						{
							setState(997);
							match(COMMA);
							setState(998);
							column_name();
							setState(999);
							match(ASSIGN);
							setState(1000);
							expr(0);
						}
					}
					setState(1006);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1009);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_WHERE) {
					{
						setState(1007);
						match(K_WHERE);
						setState(1008);
						expr(0);
					}
				}

				setState(1029);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_LIMIT || _la==K_ORDER) {
					{
						setState(1021);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_ORDER) {
							{
								setState(1011);
								match(K_ORDER);
								setState(1012);
								match(K_BY);
								setState(1013);
								ordering_term();
								setState(1018);
								_errHandler.sync(this);
								_la = _input.LA(1);
								while (_la==COMMA) {
									{
										{
											setState(1014);
											match(COMMA);
											setState(1015);
											ordering_term();
										}
									}
									setState(1020);
									_errHandler.sync(this);
									_la = _input.LA(1);
								}
							}
						}

						setState(1023);
						match(K_LIMIT);
						setState(1024);
						expr(0);
						setState(1027);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==COMMA || _la==K_OFFSET) {
							{
								setState(1025);
								_la = _input.LA(1);
								if ( !(_la==COMMA || _la==K_OFFSET) ) {
									_errHandler.recoverInline(this);
								}
								else {
									if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
									_errHandler.reportMatch(this);
									consume();
								}
								setState(1026);
								expr(0);
							}
						}

					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Vacuum_stmtContext extends ParserRuleContext {
		public TerminalNode K_VACUUM() { return getToken(SQLiteParser.K_VACUUM, 0); }
		public Vacuum_stmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_vacuum_stmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterVacuum_stmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitVacuum_stmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitVacuum_stmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Vacuum_stmtContext vacuum_stmt() throws RecognitionException {
		Vacuum_stmtContext _localctx = new Vacuum_stmtContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_vacuum_stmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1031);
				match(K_VACUUM);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_defContext extends ParserRuleContext {
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public Type_nameContext type_name() {
			return getRuleContext(Type_nameContext.class,0);
		}
		public List<Column_constraintContext> column_constraint() {
			return getRuleContexts(Column_constraintContext.class);
		}
		public Column_constraintContext column_constraint(int i) {
			return getRuleContext(Column_constraintContext.class,i);
		}
		public Column_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_def(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitColumn_def(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_defContext column_def() throws RecognitionException {
		Column_defContext _localctx = new Column_defContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_column_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1033);
				column_name();
				setState(1035);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
					case 1:
					{
						setState(1034);
						type_name();
					}
					break;
				}
				setState(1040);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << K_CHECK) | (1L << K_COLLATE) | (1L << K_CONSTRAINT) | (1L << K_DEFAULT))) != 0) || ((((_la - 102)) & ~0x3f) == 0 && ((1L << (_la - 102)) & ((1L << (K_NOT - 102)) | (1L << (K_NULL - 102)) | (1L << (K_PRIMARY - 102)) | (1L << (K_REFERENCES - 102)) | (1L << (K_UNIQUE - 102)))) != 0)) {
					{
						{
							setState(1037);
							column_constraint();
						}
					}
					setState(1042);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Type_nameContext extends ParserRuleContext {
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public List<Signed_numberContext> signed_number() {
			return getRuleContexts(Signed_numberContext.class);
		}
		public Signed_numberContext signed_number(int i) {
			return getRuleContext(Signed_numberContext.class,i);
		}
		public Type_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterType_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitType_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitType_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Type_nameContext type_name() throws RecognitionException {
		Type_nameContext _localctx = new Type_nameContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_type_name);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(1044);
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
						case 1:
						{
							{
								setState(1043);
								name();
							}
						}
						break;
						default:
							throw new NoViableAltException(this);
					}
					setState(1046);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,144,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				setState(1058);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,145,_ctx) ) {
					case 1:
					{
						setState(1048);
						match(OPEN_PAR);
						setState(1049);
						signed_number();
						setState(1050);
						match(CLOSE_PAR);
					}
					break;
					case 2:
					{
						setState(1052);
						match(OPEN_PAR);
						setState(1053);
						signed_number();
						setState(1054);
						match(COMMA);
						setState(1055);
						signed_number();
						setState(1056);
						match(CLOSE_PAR);
					}
					break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_constraintContext extends ParserRuleContext {
		public TerminalNode K_PRIMARY() { return getToken(SQLiteParser.K_PRIMARY, 0); }
		public TerminalNode K_KEY() { return getToken(SQLiteParser.K_KEY, 0); }
		public Conflict_clauseContext conflict_clause() {
			return getRuleContext(Conflict_clauseContext.class,0);
		}
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public TerminalNode K_CHECK() { return getToken(SQLiteParser.K_CHECK, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_DEFAULT() { return getToken(SQLiteParser.K_DEFAULT, 0); }
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public Foreign_key_clauseContext foreign_key_clause() {
			return getRuleContext(Foreign_key_clauseContext.class,0);
		}
		public TerminalNode K_CONSTRAINT() { return getToken(SQLiteParser.K_CONSTRAINT, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public Signed_numberContext signed_number() {
			return getRuleContext(Signed_numberContext.class,0);
		}
		public Literal_valueContext literal_value() {
			return getRuleContext(Literal_valueContext.class,0);
		}
		public TerminalNode K_AUTOINCREMENT() { return getToken(SQLiteParser.K_AUTOINCREMENT, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public Column_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_constraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitColumn_constraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_constraintContext column_constraint() throws RecognitionException {
		Column_constraintContext _localctx = new Column_constraintContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_column_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1062);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_CONSTRAINT) {
					{
						setState(1060);
						match(K_CONSTRAINT);
						setState(1061);
						name();
					}
				}

				setState(1097);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_PRIMARY:
					{
						setState(1064);
						match(K_PRIMARY);
						setState(1065);
						match(K_KEY);
						setState(1067);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_ASC || _la==K_DESC) {
							{
								setState(1066);
								_la = _input.LA(1);
								if ( !(_la==K_ASC || _la==K_DESC) ) {
									_errHandler.recoverInline(this);
								}
								else {
									if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
									_errHandler.reportMatch(this);
									consume();
								}
							}
						}

						setState(1069);
						conflict_clause();
						setState(1071);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_AUTOINCREMENT) {
							{
								setState(1070);
								match(K_AUTOINCREMENT);
							}
						}

					}
					break;
					case K_NOT:
					case K_NULL:
					{
						setState(1074);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_NOT) {
							{
								setState(1073);
								match(K_NOT);
							}
						}

						setState(1076);
						match(K_NULL);
						setState(1077);
						conflict_clause();
					}
					break;
					case K_UNIQUE:
					{
						setState(1078);
						match(K_UNIQUE);
						setState(1079);
						conflict_clause();
					}
					break;
					case K_CHECK:
					{
						setState(1080);
						match(K_CHECK);
						setState(1081);
						match(OPEN_PAR);
						setState(1082);
						expr(0);
						setState(1083);
						match(CLOSE_PAR);
					}
					break;
					case K_DEFAULT:
					{
						setState(1085);
						match(K_DEFAULT);
						setState(1092);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,150,_ctx) ) {
							case 1:
							{
								setState(1086);
								signed_number();
							}
							break;
							case 2:
							{
								setState(1087);
								literal_value();
							}
							break;
							case 3:
							{
								setState(1088);
								match(OPEN_PAR);
								setState(1089);
								expr(0);
								setState(1090);
								match(CLOSE_PAR);
							}
							break;
						}
					}
					break;
					case K_COLLATE:
					{
						setState(1094);
						match(K_COLLATE);
						setState(1095);
						collation_name();
					}
					break;
					case K_REFERENCES:
					{
						setState(1096);
						foreign_key_clause();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Conflict_clauseContext extends ParserRuleContext {
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public TerminalNode K_CONFLICT() { return getToken(SQLiteParser.K_CONFLICT, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public Conflict_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conflict_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterConflict_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitConflict_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitConflict_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Conflict_clauseContext conflict_clause() throws RecognitionException {
		Conflict_clauseContext _localctx = new Conflict_clauseContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_conflict_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1102);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ON) {
					{
						setState(1099);
						match(K_ON);
						setState(1100);
						match(K_CONFLICT);
						setState(1101);
						_la = _input.LA(1);
						if ( !(_la==K_ABORT || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (K_FAIL - 72)) | (1L << (K_IGNORE - 72)) | (1L << (K_REPLACE - 72)) | (1L << (K_ROLLBACK - 72)))) != 0)) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExprContext extends ParserRuleContext {
		public Literal_valueContext literal_value() {
			return getRuleContext(Literal_valueContext.class,0);
		}
		public TerminalNode BIND_PARAMETER() { return getToken(SQLiteParser.BIND_PARAMETER, 0); }
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Unary_operatorContext unary_operator() {
			return getRuleContext(Unary_operatorContext.class,0);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public Function_nameContext function_name() {
			return getRuleContext(Function_nameContext.class,0);
		}
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public TerminalNode K_CAST() { return getToken(SQLiteParser.K_CAST, 0); }
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Type_nameContext type_name() {
			return getRuleContext(Type_nameContext.class,0);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_CASE() { return getToken(SQLiteParser.K_CASE, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public List<TerminalNode> K_WHEN() { return getTokens(SQLiteParser.K_WHEN); }
		public TerminalNode K_WHEN(int i) {
			return getToken(SQLiteParser.K_WHEN, i);
		}
		public List<TerminalNode> K_THEN() { return getTokens(SQLiteParser.K_THEN); }
		public TerminalNode K_THEN(int i) {
			return getToken(SQLiteParser.K_THEN, i);
		}
		public TerminalNode K_ELSE() { return getToken(SQLiteParser.K_ELSE, 0); }
		public Raise_functionContext raise_function() {
			return getRuleContext(Raise_functionContext.class,0);
		}
		public TerminalNode K_IS() { return getToken(SQLiteParser.K_IS, 0); }
		public TerminalNode K_IN() { return getToken(SQLiteParser.K_IN, 0); }
		public TerminalNode K_LIKE() { return getToken(SQLiteParser.K_LIKE, 0); }
		public TerminalNode K_GLOB() { return getToken(SQLiteParser.K_GLOB, 0); }
		public TerminalNode K_MATCH() { return getToken(SQLiteParser.K_MATCH, 0); }
		public TerminalNode K_REGEXP() { return getToken(SQLiteParser.K_REGEXP, 0); }
		public TerminalNode K_AND() { return getToken(SQLiteParser.K_AND, 0); }
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_BETWEEN() { return getToken(SQLiteParser.K_BETWEEN, 0); }
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public TerminalNode K_ESCAPE() { return getToken(SQLiteParser.K_ESCAPE, 0); }
		public TerminalNode K_ISNULL() { return getToken(SQLiteParser.K_ISNULL, 0); }
		public TerminalNode K_NOTNULL() { return getToken(SQLiteParser.K_NOTNULL, 0); }
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 78;
		enterRecursionRule(_localctx, 78, RULE_expr, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(1180);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
					case 1:
					{
						setState(1105);
						literal_value();
					}
					break;
					case 2:
					{
						setState(1106);
						match(BIND_PARAMETER);
					}
					break;
					case 3:
					{
						setState(1115);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,154,_ctx) ) {
							case 1:
							{
								setState(1110);
								_errHandler.sync(this);
								switch ( getInterpreter().adaptivePredict(_input,153,_ctx) ) {
									case 1:
									{
										setState(1107);
										database_name();
										setState(1108);
										match(DOT);
									}
									break;
								}
								setState(1112);
								table_name();
								setState(1113);
								match(DOT);
							}
							break;
						}
						setState(1117);
						column_name();
					}
					break;
					case 4:
					{
						setState(1118);
						unary_operator();
						setState(1119);
						expr(21);
					}
					break;
					case 5:
					{
						setState(1121);
						function_name();
						setState(1122);
						match(OPEN_PAR);
						setState(1135);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
							case OPEN_PAR:
							case PLUS:
							case MINUS:
							case TILDE:
							case K_ABORT:
							case K_ACTION:
							case K_ADD:
							case K_AFTER:
							case K_ALL:
							case K_ALTER:
							case K_ANALYZE:
							case K_AND:
							case K_AS:
							case K_ASC:
							case K_ATTACH:
							case K_AUTOINCREMENT:
							case K_BEFORE:
							case K_BEGIN:
							case K_BETWEEN:
							case K_BY:
							case K_CASCADE:
							case K_CASE:
							case K_CAST:
							case K_CHECK:
							case K_COLLATE:
							case K_COLUMN:
							case K_COMMIT:
							case K_CONFLICT:
							case K_CONSTRAINT:
							case K_CREATE:
							case K_CROSS:
							case K_CURRENT_DATE:
							case K_CURRENT_TIME:
							case K_CURRENT_TIMESTAMP:
							case K_DATABASE:
							case K_DEFAULT:
							case K_DEFERRABLE:
							case K_DEFERRED:
							case K_DELETE:
							case K_DESC:
							case K_DETACH:
							case K_DISTINCT:
							case K_DROP:
							case K_EACH:
							case K_ELSE:
							case K_END:
							case K_ESCAPE:
							case K_EXCEPT:
							case K_EXCLUSIVE:
							case K_EXISTS:
							case K_EXPLAIN:
							case K_FAIL:
							case K_FOR:
							case K_FOREIGN:
							case K_FROM:
							case K_FULL:
							case K_GLOB:
							case K_GROUP:
							case K_HAVING:
							case K_IF:
							case K_IGNORE:
							case K_IMMEDIATE:
							case K_IN:
							case K_INDEX:
							case K_INDEXED:
							case K_INITIALLY:
							case K_INNER:
							case K_INSERT:
							case K_INSTEAD:
							case K_INTERSECT:
							case K_INTO:
							case K_IS:
							case K_ISNULL:
							case K_JOIN:
							case K_KEY:
							case K_LEFT:
							case K_LIKE:
							case K_LIMIT:
							case K_MATCH:
							case K_NATURAL:
							case K_NO:
							case K_NOT:
							case K_NOTNULL:
							case K_NULL:
							case K_OF:
							case K_OFFSET:
							case K_ON:
							case K_OR:
							case K_ORDER:
							case K_OUTER:
							case K_PLAN:
							case K_PRAGMA:
							case K_PRIMARY:
							case K_QUERY:
							case K_RAISE:
							case K_RECURSIVE:
							case K_REFERENCES:
							case K_REGEXP:
							case K_REINDEX:
							case K_RELEASE:
							case K_RENAME:
							case K_REPLACE:
							case K_RESTRICT:
							case K_RIGHT:
							case K_ROLLBACK:
							case K_ROW:
							case K_SAVEPOINT:
							case K_SELECT:
							case K_SET:
							case K_TABLE:
							case K_TEMP:
							case K_TEMPORARY:
							case K_THEN:
							case K_TO:
							case K_TRANSACTION:
							case K_TRIGGER:
							case K_UNION:
							case K_UNIQUE:
							case K_UPDATE:
							case K_USING:
							case K_VACUUM:
							case K_VALUES:
							case K_VIEW:
							case K_VIRTUAL:
							case K_WHEN:
							case K_WHERE:
							case K_WITH:
							case K_WITHOUT:
							case IDENTIFIER:
							case NUMERIC_LITERAL:
							case BIND_PARAMETER:
							case STRING_LITERAL:
							case BLOB_LITERAL:
							{
								setState(1124);
								_errHandler.sync(this);
								switch ( getInterpreter().adaptivePredict(_input,155,_ctx) ) {
									case 1:
									{
										setState(1123);
										match(K_DISTINCT);
									}
									break;
								}
								setState(1126);
								expr(0);
								setState(1131);
								_errHandler.sync(this);
								_la = _input.LA(1);
								while (_la==COMMA) {
									{
										{
											setState(1127);
											match(COMMA);
											setState(1128);
											expr(0);
										}
									}
									setState(1133);
									_errHandler.sync(this);
									_la = _input.LA(1);
								}
							}
							break;
							case STAR:
							{
								setState(1134);
								match(STAR);
							}
							break;
							case CLOSE_PAR:
								break;
							default:
								break;
						}
						setState(1137);
						match(CLOSE_PAR);
					}
					break;
					case 6:
					{
						setState(1139);
						match(OPEN_PAR);
						setState(1140);
						expr(0);
						setState(1141);
						match(CLOSE_PAR);
					}
					break;
					case 7:
					{
						setState(1143);
						match(K_CAST);
						setState(1144);
						match(OPEN_PAR);
						setState(1145);
						expr(0);
						setState(1146);
						match(K_AS);
						setState(1147);
						type_name();
						setState(1148);
						match(CLOSE_PAR);
					}
					break;
					case 8:
					{
						setState(1154);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_EXISTS || _la==K_NOT) {
							{
								setState(1151);
								_errHandler.sync(this);
								_la = _input.LA(1);
								if (_la==K_NOT) {
									{
										setState(1150);
										match(K_NOT);
									}
								}

								setState(1153);
								match(K_EXISTS);
							}
						}

						setState(1156);
						match(OPEN_PAR);
						setState(1157);
						select_stmt();
						setState(1158);
						match(CLOSE_PAR);
					}
					break;
					case 9:
					{
						setState(1160);
						match(K_CASE);
						setState(1162);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,160,_ctx) ) {
							case 1:
							{
								setState(1161);
								expr(0);
							}
							break;
						}
						setState(1169);
						_errHandler.sync(this);
						_la = _input.LA(1);
						do {
							{
								{
									setState(1164);
									match(K_WHEN);
									setState(1165);
									expr(0);
									setState(1166);
									match(K_THEN);
									setState(1167);
									expr(0);
								}
							}
							setState(1171);
							_errHandler.sync(this);
							_la = _input.LA(1);
						} while ( _la==K_WHEN );
						setState(1175);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_ELSE) {
							{
								setState(1173);
								match(K_ELSE);
								setState(1174);
								expr(0);
							}
						}

						setState(1177);
						match(K_END);
					}
					break;
					case 10:
					{
						setState(1179);
						raise_function();
					}
					break;
				}
				_ctx.stop = _input.LT(-1);
				setState(1282);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,176,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						if ( _parseListeners!=null ) triggerExitRuleEvent();
						_prevctx = _localctx;
						{
							setState(1280);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
								case 1:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1182);
									if (!(precpred(_ctx, 20))) throw new FailedPredicateException(this, "precpred(_ctx, 20)");
									setState(1183);
									match(PIPE2);
									setState(1184);
									expr(21);
								}
								break;
								case 2:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1185);
									if (!(precpred(_ctx, 19))) throw new FailedPredicateException(this, "precpred(_ctx, 19)");
									setState(1186);
									_la = _input.LA(1);
									if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << STAR) | (1L << DIV) | (1L << MOD))) != 0)) ) {
										_errHandler.recoverInline(this);
									}
									else {
										if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
										_errHandler.reportMatch(this);
										consume();
									}
									setState(1187);
									expr(20);
								}
								break;
								case 3:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1188);
									if (!(precpred(_ctx, 18))) throw new FailedPredicateException(this, "precpred(_ctx, 18)");
									setState(1189);
									_la = _input.LA(1);
									if ( !(_la==PLUS || _la==MINUS) ) {
										_errHandler.recoverInline(this);
									}
									else {
										if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
										_errHandler.reportMatch(this);
										consume();
									}
									setState(1190);
									expr(19);
								}
								break;
								case 4:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1191);
									if (!(precpred(_ctx, 17))) throw new FailedPredicateException(this, "precpred(_ctx, 17)");
									setState(1192);
									_la = _input.LA(1);
									if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT2) | (1L << GT2) | (1L << AMP) | (1L << PIPE))) != 0)) ) {
										_errHandler.recoverInline(this);
									}
									else {
										if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
										_errHandler.reportMatch(this);
										consume();
									}
									setState(1193);
									expr(18);
								}
								break;
								case 5:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1194);
									if (!(precpred(_ctx, 16))) throw new FailedPredicateException(this, "precpred(_ctx, 16)");
									setState(1195);
									_la = _input.LA(1);
									if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LT_EQ) | (1L << GT) | (1L << GT_EQ))) != 0)) ) {
										_errHandler.recoverInline(this);
									}
									else {
										if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
										_errHandler.reportMatch(this);
										consume();
									}
									setState(1196);
									expr(17);
								}
								break;
								case 6:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1197);
									if (!(precpred(_ctx, 15))) throw new FailedPredicateException(this, "precpred(_ctx, 15)");
									setState(1210);
									_errHandler.sync(this);
									switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
										case 1:
										{
											setState(1198);
											match(ASSIGN);
										}
										break;
										case 2:
										{
											setState(1199);
											match(EQ);
										}
										break;
										case 3:
										{
											setState(1200);
											match(NOT_EQ1);
										}
										break;
										case 4:
										{
											setState(1201);
											match(NOT_EQ2);
										}
										break;
										case 5:
										{
											setState(1202);
											match(K_IS);
										}
										break;
										case 6:
										{
											setState(1203);
											match(K_IS);
											setState(1204);
											match(K_NOT);
										}
										break;
										case 7:
										{
											setState(1205);
											match(K_IN);
										}
										break;
										case 8:
										{
											setState(1206);
											match(K_LIKE);
										}
										break;
										case 9:
										{
											setState(1207);
											match(K_GLOB);
										}
										break;
										case 10:
										{
											setState(1208);
											match(K_MATCH);
										}
										break;
										case 11:
										{
											setState(1209);
											match(K_REGEXP);
										}
										break;
									}
									setState(1212);
									expr(16);
								}
								break;
								case 7:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1213);
									if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
									setState(1214);
									match(K_AND);
									setState(1215);
									expr(15);
								}
								break;
								case 8:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1216);
									if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
									setState(1217);
									match(K_OR);
									setState(1218);
									expr(14);
								}
								break;
								case 9:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1219);
									if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
									setState(1220);
									match(K_IS);
									setState(1222);
									_errHandler.sync(this);
									switch ( getInterpreter().adaptivePredict(_input,165,_ctx) ) {
										case 1:
										{
											setState(1221);
											match(K_NOT);
										}
										break;
									}
									setState(1224);
									expr(7);
								}
								break;
								case 10:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1225);
									if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
									setState(1227);
									_errHandler.sync(this);
									_la = _input.LA(1);
									if (_la==K_NOT) {
										{
											setState(1226);
											match(K_NOT);
										}
									}

									setState(1229);
									match(K_BETWEEN);
									setState(1230);
									expr(0);
									setState(1231);
									match(K_AND);
									setState(1232);
									expr(6);
								}
								break;
								case 11:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1234);
									if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
									setState(1235);
									match(K_COLLATE);
									setState(1236);
									collation_name();
								}
								break;
								case 12:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1237);
									if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
									setState(1239);
									_errHandler.sync(this);
									_la = _input.LA(1);
									if (_la==K_NOT) {
										{
											setState(1238);
											match(K_NOT);
										}
									}

									setState(1241);
									_la = _input.LA(1);
									if ( !(((((_la - 77)) & ~0x3f) == 0 && ((1L << (_la - 77)) & ((1L << (K_GLOB - 77)) | (1L << (K_LIKE - 77)) | (1L << (K_MATCH - 77)) | (1L << (K_REGEXP - 77)))) != 0)) ) {
										_errHandler.recoverInline(this);
									}
									else {
										if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
										_errHandler.reportMatch(this);
										consume();
									}
									setState(1242);
									expr(0);
									setState(1245);
									_errHandler.sync(this);
									switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
										case 1:
										{
											setState(1243);
											match(K_ESCAPE);
											setState(1244);
											expr(0);
										}
										break;
									}
								}
								break;
								case 13:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1247);
									if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
									setState(1252);
									_errHandler.sync(this);
									switch (_input.LA(1)) {
										case K_ISNULL:
										{
											setState(1248);
											match(K_ISNULL);
										}
										break;
										case K_NOTNULL:
										{
											setState(1249);
											match(K_NOTNULL);
										}
										break;
										case K_NOT:
										{
											setState(1250);
											match(K_NOT);
											setState(1251);
											match(K_NULL);
										}
										break;
										default:
											throw new NoViableAltException(this);
									}
								}
								break;
								case 14:
								{
									_localctx = new ExprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_expr);
									setState(1254);
									if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
									setState(1256);
									_errHandler.sync(this);
									_la = _input.LA(1);
									if (_la==K_NOT) {
										{
											setState(1255);
											match(K_NOT);
										}
									}

									setState(1258);
									match(K_IN);
									setState(1278);
									_errHandler.sync(this);
									switch ( getInterpreter().adaptivePredict(_input,174,_ctx) ) {
										case 1:
										{
											setState(1259);
											match(OPEN_PAR);
											setState(1269);
											_errHandler.sync(this);
											switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
												case 1:
												{
													setState(1260);
													select_stmt();
												}
												break;
												case 2:
												{
													setState(1261);
													expr(0);
													setState(1266);
													_errHandler.sync(this);
													_la = _input.LA(1);
													while (_la==COMMA) {
														{
															{
																setState(1262);
																match(COMMA);
																setState(1263);
																expr(0);
															}
														}
														setState(1268);
														_errHandler.sync(this);
														_la = _input.LA(1);
													}
												}
												break;
											}
											setState(1271);
											match(CLOSE_PAR);
										}
										break;
										case 2:
										{
											setState(1275);
											_errHandler.sync(this);
											switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
												case 1:
												{
													setState(1272);
													database_name();
													setState(1273);
													match(DOT);
												}
												break;
											}
											setState(1277);
											table_name();
										}
										break;
									}
								}
								break;
							}
						}
					}
					setState(1284);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,176,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class Foreign_key_clauseContext extends ParserRuleContext {
		public TerminalNode K_REFERENCES() { return getToken(SQLiteParser.K_REFERENCES, 0); }
		public Foreign_tableContext foreign_table() {
			return getRuleContext(Foreign_tableContext.class,0);
		}
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public TerminalNode K_DEFERRABLE() { return getToken(SQLiteParser.K_DEFERRABLE, 0); }
		public List<TerminalNode> K_ON() { return getTokens(SQLiteParser.K_ON); }
		public TerminalNode K_ON(int i) {
			return getToken(SQLiteParser.K_ON, i);
		}
		public List<TerminalNode> K_MATCH() { return getTokens(SQLiteParser.K_MATCH); }
		public TerminalNode K_MATCH(int i) {
			return getToken(SQLiteParser.K_MATCH, i);
		}
		public List<NameContext> name() {
			return getRuleContexts(NameContext.class);
		}
		public NameContext name(int i) {
			return getRuleContext(NameContext.class,i);
		}
		public List<TerminalNode> K_DELETE() { return getTokens(SQLiteParser.K_DELETE); }
		public TerminalNode K_DELETE(int i) {
			return getToken(SQLiteParser.K_DELETE, i);
		}
		public List<TerminalNode> K_UPDATE() { return getTokens(SQLiteParser.K_UPDATE); }
		public TerminalNode K_UPDATE(int i) {
			return getToken(SQLiteParser.K_UPDATE, i);
		}
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_INITIALLY() { return getToken(SQLiteParser.K_INITIALLY, 0); }
		public TerminalNode K_DEFERRED() { return getToken(SQLiteParser.K_DEFERRED, 0); }
		public TerminalNode K_IMMEDIATE() { return getToken(SQLiteParser.K_IMMEDIATE, 0); }
		public List<TerminalNode> K_SET() { return getTokens(SQLiteParser.K_SET); }
		public TerminalNode K_SET(int i) {
			return getToken(SQLiteParser.K_SET, i);
		}
		public List<TerminalNode> K_NULL() { return getTokens(SQLiteParser.K_NULL); }
		public TerminalNode K_NULL(int i) {
			return getToken(SQLiteParser.K_NULL, i);
		}
		public List<TerminalNode> K_DEFAULT() { return getTokens(SQLiteParser.K_DEFAULT); }
		public TerminalNode K_DEFAULT(int i) {
			return getToken(SQLiteParser.K_DEFAULT, i);
		}
		public List<TerminalNode> K_CASCADE() { return getTokens(SQLiteParser.K_CASCADE); }
		public TerminalNode K_CASCADE(int i) {
			return getToken(SQLiteParser.K_CASCADE, i);
		}
		public List<TerminalNode> K_RESTRICT() { return getTokens(SQLiteParser.K_RESTRICT); }
		public TerminalNode K_RESTRICT(int i) {
			return getToken(SQLiteParser.K_RESTRICT, i);
		}
		public List<TerminalNode> K_NO() { return getTokens(SQLiteParser.K_NO); }
		public TerminalNode K_NO(int i) {
			return getToken(SQLiteParser.K_NO, i);
		}
		public List<TerminalNode> K_ACTION() { return getTokens(SQLiteParser.K_ACTION); }
		public TerminalNode K_ACTION(int i) {
			return getToken(SQLiteParser.K_ACTION, i);
		}
		public Foreign_key_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_foreign_key_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterForeign_key_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitForeign_key_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitForeign_key_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Foreign_key_clauseContext foreign_key_clause() throws RecognitionException {
		Foreign_key_clauseContext _localctx = new Foreign_key_clauseContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_foreign_key_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1285);
				match(K_REFERENCES);
				setState(1286);
				foreign_table();
				setState(1298);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR) {
					{
						setState(1287);
						match(OPEN_PAR);
						setState(1288);
						column_name();
						setState(1293);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(1289);
									match(COMMA);
									setState(1290);
									column_name();
								}
							}
							setState(1295);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(1296);
						match(CLOSE_PAR);
					}
				}

				setState(1318);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==K_MATCH || _la==K_ON) {
					{
						{
							setState(1314);
							_errHandler.sync(this);
							switch (_input.LA(1)) {
								case K_ON:
								{
									setState(1300);
									match(K_ON);
									setState(1301);
									_la = _input.LA(1);
									if ( !(_la==K_DELETE || _la==K_UPDATE) ) {
										_errHandler.recoverInline(this);
									}
									else {
										if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
										_errHandler.reportMatch(this);
										consume();
									}
									setState(1310);
									_errHandler.sync(this);
									switch ( getInterpreter().adaptivePredict(_input,179,_ctx) ) {
										case 1:
										{
											setState(1302);
											match(K_SET);
											setState(1303);
											match(K_NULL);
										}
										break;
										case 2:
										{
											setState(1304);
											match(K_SET);
											setState(1305);
											match(K_DEFAULT);
										}
										break;
										case 3:
										{
											setState(1306);
											match(K_CASCADE);
										}
										break;
										case 4:
										{
											setState(1307);
											match(K_RESTRICT);
										}
										break;
										case 5:
										{
											setState(1308);
											match(K_NO);
											setState(1309);
											match(K_ACTION);
										}
										break;
									}
								}
								break;
								case K_MATCH:
								{
									setState(1312);
									match(K_MATCH);
									setState(1313);
									name();
								}
								break;
								default:
									throw new NoViableAltException(this);
							}
						}
					}
					setState(1320);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1331);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
					case 1:
					{
						setState(1322);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==K_NOT) {
							{
								setState(1321);
								match(K_NOT);
							}
						}

						setState(1324);
						match(K_DEFERRABLE);
						setState(1329);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,183,_ctx) ) {
							case 1:
							{
								setState(1325);
								match(K_INITIALLY);
								setState(1326);
								match(K_DEFERRED);
							}
							break;
							case 2:
							{
								setState(1327);
								match(K_INITIALLY);
								setState(1328);
								match(K_IMMEDIATE);
							}
							break;
						}
					}
					break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Raise_functionContext extends ParserRuleContext {
		public TerminalNode K_RAISE() { return getToken(SQLiteParser.K_RAISE, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public Error_messageContext error_message() {
			return getRuleContext(Error_messageContext.class,0);
		}
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public Raise_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_raise_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterRaise_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitRaise_function(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitRaise_function(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Raise_functionContext raise_function() throws RecognitionException {
		Raise_functionContext _localctx = new Raise_functionContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_raise_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1333);
				match(K_RAISE);
				setState(1334);
				match(OPEN_PAR);
				setState(1339);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_IGNORE:
					{
						setState(1335);
						match(K_IGNORE);
					}
					break;
					case K_ABORT:
					case K_FAIL:
					case K_ROLLBACK:
					{
						setState(1336);
						_la = _input.LA(1);
						if ( !(_la==K_ABORT || _la==K_FAIL || _la==K_ROLLBACK) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1337);
						match(COMMA);
						setState(1338);
						error_message();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
				setState(1341);
				match(CLOSE_PAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Indexed_columnContext extends ParserRuleContext {
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public Indexed_columnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_indexed_column; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterIndexed_column(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitIndexed_column(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitIndexed_column(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Indexed_columnContext indexed_column() throws RecognitionException {
		Indexed_columnContext _localctx = new Indexed_columnContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_indexed_column);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1343);
				column_name();
				setState(1346);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_COLLATE) {
					{
						setState(1344);
						match(K_COLLATE);
						setState(1345);
						collation_name();
					}
				}

				setState(1349);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ASC || _la==K_DESC) {
					{
						setState(1348);
						_la = _input.LA(1);
						if ( !(_la==K_ASC || _la==K_DESC) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_constraintContext extends ParserRuleContext {
		public List<Indexed_columnContext> indexed_column() {
			return getRuleContexts(Indexed_columnContext.class);
		}
		public Indexed_columnContext indexed_column(int i) {
			return getRuleContext(Indexed_columnContext.class,i);
		}
		public Conflict_clauseContext conflict_clause() {
			return getRuleContext(Conflict_clauseContext.class,0);
		}
		public TerminalNode K_CHECK() { return getToken(SQLiteParser.K_CHECK, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_FOREIGN() { return getToken(SQLiteParser.K_FOREIGN, 0); }
		public TerminalNode K_KEY() { return getToken(SQLiteParser.K_KEY, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Foreign_key_clauseContext foreign_key_clause() {
			return getRuleContext(Foreign_key_clauseContext.class,0);
		}
		public TerminalNode K_CONSTRAINT() { return getToken(SQLiteParser.K_CONSTRAINT, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode K_PRIMARY() { return getToken(SQLiteParser.K_PRIMARY, 0); }
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public Table_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_constraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitTable_constraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_constraintContext table_constraint() throws RecognitionException {
		Table_constraintContext _localctx = new Table_constraintContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_table_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1353);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_CONSTRAINT) {
					{
						setState(1351);
						match(K_CONSTRAINT);
						setState(1352);
						name();
					}
				}

				setState(1391);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_PRIMARY:
					case K_UNIQUE:
					{
						setState(1358);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
							case K_PRIMARY:
							{
								setState(1355);
								match(K_PRIMARY);
								setState(1356);
								match(K_KEY);
							}
							break;
							case K_UNIQUE:
							{
								setState(1357);
								match(K_UNIQUE);
							}
							break;
							default:
								throw new NoViableAltException(this);
						}
						setState(1360);
						match(OPEN_PAR);
						setState(1361);
						indexed_column();
						setState(1366);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(1362);
									match(COMMA);
									setState(1363);
									indexed_column();
								}
							}
							setState(1368);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(1369);
						match(CLOSE_PAR);
						setState(1370);
						conflict_clause();
					}
					break;
					case K_CHECK:
					{
						setState(1372);
						match(K_CHECK);
						setState(1373);
						match(OPEN_PAR);
						setState(1374);
						expr(0);
						setState(1375);
						match(CLOSE_PAR);
					}
					break;
					case K_FOREIGN:
					{
						setState(1377);
						match(K_FOREIGN);
						setState(1378);
						match(K_KEY);
						setState(1379);
						match(OPEN_PAR);
						setState(1380);
						column_name();
						setState(1385);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(1381);
									match(COMMA);
									setState(1382);
									column_name();
								}
							}
							setState(1387);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(1388);
						match(CLOSE_PAR);
						setState(1389);
						foreign_key_clause();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class With_clauseContext extends ParserRuleContext {
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public List<Cte_table_nameContext> cte_table_name() {
			return getRuleContexts(Cte_table_nameContext.class);
		}
		public Cte_table_nameContext cte_table_name(int i) {
			return getRuleContext(Cte_table_nameContext.class,i);
		}
		public List<TerminalNode> K_AS() { return getTokens(SQLiteParser.K_AS); }
		public TerminalNode K_AS(int i) {
			return getToken(SQLiteParser.K_AS, i);
		}
		public List<Select_stmtContext> select_stmt() {
			return getRuleContexts(Select_stmtContext.class);
		}
		public Select_stmtContext select_stmt(int i) {
			return getRuleContext(Select_stmtContext.class,i);
		}
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public With_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterWith_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitWith_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitWith_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final With_clauseContext with_clause() throws RecognitionException {
		With_clauseContext _localctx = new With_clauseContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_with_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1393);
				match(K_WITH);
				setState(1395);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,193,_ctx) ) {
					case 1:
					{
						setState(1394);
						match(K_RECURSIVE);
					}
					break;
				}
				setState(1397);
				cte_table_name();
				setState(1398);
				match(K_AS);
				setState(1399);
				match(OPEN_PAR);
				setState(1400);
				select_stmt();
				setState(1401);
				match(CLOSE_PAR);
				setState(1411);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
						{
							setState(1402);
							match(COMMA);
							setState(1403);
							cte_table_name();
							setState(1404);
							match(K_AS);
							setState(1405);
							match(OPEN_PAR);
							setState(1406);
							select_stmt();
							setState(1407);
							match(CLOSE_PAR);
						}
					}
					setState(1413);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Qualified_table_nameContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public TerminalNode K_INDEXED() { return getToken(SQLiteParser.K_INDEXED, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public Qualified_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualified_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterQualified_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitQualified_table_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitQualified_table_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Qualified_table_nameContext qualified_table_name() throws RecognitionException {
		Qualified_table_nameContext _localctx = new Qualified_table_nameContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_qualified_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1417);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,195,_ctx) ) {
					case 1:
					{
						setState(1414);
						database_name();
						setState(1415);
						match(DOT);
					}
					break;
				}
				setState(1419);
				table_name();
				setState(1425);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_INDEXED:
					{
						setState(1420);
						match(K_INDEXED);
						setState(1421);
						match(K_BY);
						setState(1422);
						index_name();
					}
					break;
					case K_NOT:
					{
						setState(1423);
						match(K_NOT);
						setState(1424);
						match(K_INDEXED);
					}
					break;
					case EOF:
					case SCOL:
					case K_ALTER:
					case K_ANALYZE:
					case K_ATTACH:
					case K_BEGIN:
					case K_COMMIT:
					case K_CREATE:
					case K_DELETE:
					case K_DETACH:
					case K_DROP:
					case K_END:
					case K_EXPLAIN:
					case K_INSERT:
					case K_LIMIT:
					case K_ORDER:
					case K_PRAGMA:
					case K_REINDEX:
					case K_RELEASE:
					case K_REPLACE:
					case K_ROLLBACK:
					case K_SAVEPOINT:
					case K_SELECT:
					case K_SET:
					case K_UPDATE:
					case K_VACUUM:
					case K_VALUES:
					case K_WHERE:
					case K_WITH:
					case UNEXPECTED_CHAR:
						break;
					default:
						break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ordering_termContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public Collation_nameContext collation_name() {
			return getRuleContext(Collation_nameContext.class,0);
		}
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public Ordering_termContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ordering_term; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterOrdering_term(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitOrdering_term(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitOrdering_term(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Ordering_termContext ordering_term() throws RecognitionException {
		Ordering_termContext _localctx = new Ordering_termContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_ordering_term);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1427);
				expr(0);
				setState(1430);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_COLLATE) {
					{
						setState(1428);
						match(K_COLLATE);
						setState(1429);
						collation_name();
					}
				}

				setState(1433);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==K_ASC || _la==K_DESC) {
					{
						setState(1432);
						_la = _input.LA(1);
						if ( !(_la==K_ASC || _la==K_DESC) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Pragma_valueContext extends ParserRuleContext {
		public Signed_numberContext signed_number() {
			return getRuleContext(Signed_numberContext.class,0);
		}
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public Pragma_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pragma_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterPragma_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitPragma_value(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitPragma_value(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Pragma_valueContext pragma_value() throws RecognitionException {
		Pragma_valueContext _localctx = new Pragma_valueContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_pragma_value);
		try {
			setState(1438);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,199,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(1435);
					signed_number();
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(1436);
					name();
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(1437);
					match(STRING_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Common_table_expressionContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Common_table_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_common_table_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCommon_table_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCommon_table_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCommon_table_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Common_table_expressionContext common_table_expression() throws RecognitionException {
		Common_table_expressionContext _localctx = new Common_table_expressionContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_common_table_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1440);
				table_name();
				setState(1452);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR) {
					{
						setState(1441);
						match(OPEN_PAR);
						setState(1442);
						column_name();
						setState(1447);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(1443);
									match(COMMA);
									setState(1444);
									column_name();
								}
							}
							setState(1449);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(1450);
						match(CLOSE_PAR);
					}
				}

				setState(1454);
				match(K_AS);
				setState(1455);
				match(OPEN_PAR);
				setState(1456);
				select_stmt();
				setState(1457);
				match(CLOSE_PAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Result_columnContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Column_aliasContext column_alias() {
			return getRuleContext(Column_aliasContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public Result_columnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_result_column; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterResult_column(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitResult_column(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitResult_column(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Result_columnContext result_column() throws RecognitionException {
		Result_columnContext _localctx = new Result_columnContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_result_column);
		int _la;
		try {
			setState(1471);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,204,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(1459);
					match(STAR);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(1460);
					table_name();
					setState(1461);
					match(DOT);
					setState(1462);
					match(STAR);
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(1464);
					expr(0);
					setState(1469);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_AS || _la==IDENTIFIER || _la==STRING_LITERAL) {
						{
							setState(1466);
							_errHandler.sync(this);
							_la = _input.LA(1);
							if (_la==K_AS) {
								{
									setState(1465);
									match(K_AS);
								}
							}

							setState(1468);
							column_alias();
						}
					}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_or_subqueryContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Database_nameContext database_name() {
			return getRuleContext(Database_nameContext.class,0);
		}
		public Table_aliasContext table_alias() {
			return getRuleContext(Table_aliasContext.class,0);
		}
		public TerminalNode K_INDEXED() { return getToken(SQLiteParser.K_INDEXED, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public Join_clauseContext join_clause() {
			return getRuleContext(Join_clauseContext.class,0);
		}
		public Select_stmtContext select_stmt() {
			return getRuleContext(Select_stmtContext.class,0);
		}
		public Table_or_subqueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_or_subquery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_or_subquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_or_subquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitTable_or_subquery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_or_subqueryContext table_or_subquery() throws RecognitionException {
		Table_or_subqueryContext _localctx = new Table_or_subqueryContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_table_or_subquery);
		int _la;
		try {
			setState(1520);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,215,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(1476);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,205,_ctx) ) {
						case 1:
						{
							setState(1473);
							database_name();
							setState(1474);
							match(DOT);
						}
						break;
					}
					setState(1478);
					table_name();
					setState(1483);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,207,_ctx) ) {
						case 1:
						{
							setState(1480);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,206,_ctx) ) {
								case 1:
								{
									setState(1479);
									match(K_AS);
								}
								break;
							}
							setState(1482);
							table_alias();
						}
						break;
					}
					setState(1490);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
						case K_INDEXED:
						{
							setState(1485);
							match(K_INDEXED);
							setState(1486);
							match(K_BY);
							setState(1487);
							index_name();
						}
						break;
						case K_NOT:
						{
							setState(1488);
							match(K_NOT);
							setState(1489);
							match(K_INDEXED);
						}
						break;
						case EOF:
						case SCOL:
						case CLOSE_PAR:
						case COMMA:
						case K_ALTER:
						case K_ANALYZE:
						case K_ATTACH:
						case K_BEGIN:
						case K_COMMIT:
						case K_CREATE:
						case K_CROSS:
						case K_DELETE:
						case K_DETACH:
						case K_DROP:
						case K_END:
						case K_EXCEPT:
						case K_EXPLAIN:
						case K_GROUP:
						case K_INNER:
						case K_INSERT:
						case K_INTERSECT:
						case K_JOIN:
						case K_LEFT:
						case K_LIMIT:
						case K_NATURAL:
						case K_ON:
						case K_ORDER:
						case K_PRAGMA:
						case K_REINDEX:
						case K_RELEASE:
						case K_REPLACE:
						case K_ROLLBACK:
						case K_SAVEPOINT:
						case K_SELECT:
						case K_UNION:
						case K_UPDATE:
						case K_USING:
						case K_VACUUM:
						case K_VALUES:
						case K_WHERE:
						case K_WITH:
						case UNEXPECTED_CHAR:
							break;
						default:
							break;
					}
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(1492);
					match(OPEN_PAR);
					setState(1502);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
						case 1:
						{
							setState(1493);
							table_or_subquery();
							setState(1498);
							_errHandler.sync(this);
							_la = _input.LA(1);
							while (_la==COMMA) {
								{
									{
										setState(1494);
										match(COMMA);
										setState(1495);
										table_or_subquery();
									}
								}
								setState(1500);
								_errHandler.sync(this);
								_la = _input.LA(1);
							}
						}
						break;
						case 2:
						{
							setState(1501);
							join_clause();
						}
						break;
					}
					setState(1504);
					match(CLOSE_PAR);
					setState(1509);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,212,_ctx) ) {
						case 1:
						{
							setState(1506);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
								case 1:
								{
									setState(1505);
									match(K_AS);
								}
								break;
							}
							setState(1508);
							table_alias();
						}
						break;
					}
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(1511);
					match(OPEN_PAR);
					setState(1512);
					select_stmt();
					setState(1513);
					match(CLOSE_PAR);
					setState(1518);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
						case 1:
						{
							setState(1515);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,213,_ctx) ) {
								case 1:
								{
									setState(1514);
									match(K_AS);
								}
								break;
							}
							setState(1517);
							table_alias();
						}
						break;
					}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_clauseContext extends ParserRuleContext {
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public List<Join_operatorContext> join_operator() {
			return getRuleContexts(Join_operatorContext.class);
		}
		public Join_operatorContext join_operator(int i) {
			return getRuleContext(Join_operatorContext.class,i);
		}
		public List<Join_constraintContext> join_constraint() {
			return getRuleContexts(Join_constraintContext.class);
		}
		public Join_constraintContext join_constraint(int i) {
			return getRuleContext(Join_constraintContext.class,i);
		}
		public Join_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterJoin_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitJoin_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitJoin_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Join_clauseContext join_clause() throws RecognitionException {
		Join_clauseContext _localctx = new Join_clauseContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_join_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1522);
				table_or_subquery();
				setState(1529);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA || _la==K_CROSS || ((((_la - 87)) & ~0x3f) == 0 && ((1L << (_la - 87)) & ((1L << (K_INNER - 87)) | (1L << (K_JOIN - 87)) | (1L << (K_LEFT - 87)) | (1L << (K_NATURAL - 87)))) != 0)) {
					{
						{
							setState(1523);
							join_operator();
							setState(1524);
							table_or_subquery();
							setState(1525);
							join_constraint();
						}
					}
					setState(1531);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_operatorContext extends ParserRuleContext {
		public TerminalNode K_JOIN() { return getToken(SQLiteParser.K_JOIN, 0); }
		public TerminalNode K_NATURAL() { return getToken(SQLiteParser.K_NATURAL, 0); }
		public TerminalNode K_LEFT() { return getToken(SQLiteParser.K_LEFT, 0); }
		public TerminalNode K_INNER() { return getToken(SQLiteParser.K_INNER, 0); }
		public TerminalNode K_CROSS() { return getToken(SQLiteParser.K_CROSS, 0); }
		public TerminalNode K_OUTER() { return getToken(SQLiteParser.K_OUTER, 0); }
		public Join_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterJoin_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitJoin_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitJoin_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Join_operatorContext join_operator() throws RecognitionException {
		Join_operatorContext _localctx = new Join_operatorContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_join_operator);
		int _la;
		try {
			setState(1545);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case COMMA:
					enterOuterAlt(_localctx, 1);
				{
					setState(1532);
					match(COMMA);
				}
				break;
				case K_CROSS:
				case K_INNER:
				case K_JOIN:
				case K_LEFT:
				case K_NATURAL:
					enterOuterAlt(_localctx, 2);
				{
					setState(1534);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_NATURAL) {
						{
							setState(1533);
							match(K_NATURAL);
						}
					}

					setState(1542);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
						case K_LEFT:
						{
							setState(1536);
							match(K_LEFT);
							setState(1538);
							_errHandler.sync(this);
							_la = _input.LA(1);
							if (_la==K_OUTER) {
								{
									setState(1537);
									match(K_OUTER);
								}
							}

						}
						break;
						case K_INNER:
						{
							setState(1540);
							match(K_INNER);
						}
						break;
						case K_CROSS:
						{
							setState(1541);
							match(K_CROSS);
						}
						break;
						case K_JOIN:
							break;
						default:
							break;
					}
					setState(1544);
					match(K_JOIN);
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Join_constraintContext extends ParserRuleContext {
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode K_USING() { return getToken(SQLiteParser.K_USING, 0); }
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Join_constraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_join_constraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterJoin_constraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitJoin_constraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitJoin_constraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Join_constraintContext join_constraint() throws RecognitionException {
		Join_constraintContext _localctx = new Join_constraintContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_join_constraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1561);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case K_ON:
					{
						setState(1547);
						match(K_ON);
						setState(1548);
						expr(0);
					}
					break;
					case K_USING:
					{
						setState(1549);
						match(K_USING);
						setState(1550);
						match(OPEN_PAR);
						setState(1551);
						column_name();
						setState(1556);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(1552);
									match(COMMA);
									setState(1553);
									column_name();
								}
							}
							setState(1558);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(1559);
						match(CLOSE_PAR);
					}
					break;
					case EOF:
					case SCOL:
					case CLOSE_PAR:
					case COMMA:
					case K_ALTER:
					case K_ANALYZE:
					case K_ATTACH:
					case K_BEGIN:
					case K_COMMIT:
					case K_CREATE:
					case K_CROSS:
					case K_DELETE:
					case K_DETACH:
					case K_DROP:
					case K_END:
					case K_EXCEPT:
					case K_EXPLAIN:
					case K_GROUP:
					case K_INNER:
					case K_INSERT:
					case K_INTERSECT:
					case K_JOIN:
					case K_LEFT:
					case K_LIMIT:
					case K_NATURAL:
					case K_ORDER:
					case K_PRAGMA:
					case K_REINDEX:
					case K_RELEASE:
					case K_REPLACE:
					case K_ROLLBACK:
					case K_SAVEPOINT:
					case K_SELECT:
					case K_UNION:
					case K_UPDATE:
					case K_VACUUM:
					case K_VALUES:
					case K_WHERE:
					case K_WITH:
					case UNEXPECTED_CHAR:
						break;
					default:
						break;
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_coreContext extends ParserRuleContext {
		public TerminalNode K_SELECT() { return getToken(SQLiteParser.K_SELECT, 0); }
		public List<Result_columnContext> result_column() {
			return getRuleContexts(Result_columnContext.class);
		}
		public Result_columnContext result_column(int i) {
			return getRuleContext(Result_columnContext.class,i);
		}
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode K_GROUP() { return getToken(SQLiteParser.K_GROUP, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public Group_byContext group_by() {
			return getRuleContext(Group_byContext.class,0);
		}
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public List<Table_or_subqueryContext> table_or_subquery() {
			return getRuleContexts(Table_or_subqueryContext.class);
		}
		public Table_or_subqueryContext table_or_subquery(int i) {
			return getRuleContext(Table_or_subqueryContext.class,i);
		}
		public Join_clauseContext join_clause() {
			return getRuleContext(Join_clauseContext.class,0);
		}
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public Select_coreContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_core; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSelect_core(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSelect_core(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSelect_core(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_coreContext select_core() throws RecognitionException {
		Select_coreContext _localctx = new Select_coreContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_select_core);
		int _la;
		try {
			setState(1626);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case K_SELECT:
					enterOuterAlt(_localctx, 1);
				{
					setState(1563);
					match(K_SELECT);
					setState(1565);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,223,_ctx) ) {
						case 1:
						{
							setState(1564);
							_la = _input.LA(1);
							if ( !(_la==K_ALL || _la==K_DISTINCT) ) {
								_errHandler.recoverInline(this);
							}
							else {
								if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
								_errHandler.reportMatch(this);
								consume();
							}
						}
						break;
					}
					setState(1567);
					result_column();
					setState(1572);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
							{
								setState(1568);
								match(COMMA);
								setState(1569);
								result_column();
							}
						}
						setState(1574);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1587);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_FROM) {
						{
							setState(1575);
							match(K_FROM);
							setState(1585);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,226,_ctx) ) {
								case 1:
								{
									setState(1576);
									table_or_subquery();
									setState(1581);
									_errHandler.sync(this);
									_la = _input.LA(1);
									while (_la==COMMA) {
										{
											{
												setState(1577);
												match(COMMA);
												setState(1578);
												table_or_subquery();
											}
										}
										setState(1583);
										_errHandler.sync(this);
										_la = _input.LA(1);
									}
								}
								break;
								case 2:
								{
									setState(1584);
									join_clause();
								}
								break;
							}
						}
					}

					setState(1591);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_WHERE) {
						{
							setState(1589);
							match(K_WHERE);
							setState(1590);
							expr(0);
						}
					}

					setState(1596);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==K_GROUP) {
						{
							setState(1593);
							match(K_GROUP);
							setState(1594);
							match(K_BY);
							setState(1595);
							group_by();
						}
					}

				}
				break;
				case K_VALUES:
					enterOuterAlt(_localctx, 2);
				{
					setState(1598);
					match(K_VALUES);
					setState(1599);
					match(OPEN_PAR);
					setState(1600);
					expr(0);
					setState(1605);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
							{
								setState(1601);
								match(COMMA);
								setState(1602);
								expr(0);
							}
						}
						setState(1607);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1608);
					match(CLOSE_PAR);
					setState(1623);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
							{
								setState(1609);
								match(COMMA);
								setState(1610);
								match(OPEN_PAR);
								setState(1611);
								expr(0);
								setState(1616);
								_errHandler.sync(this);
								_la = _input.LA(1);
								while (_la==COMMA) {
									{
										{
											setState(1612);
											match(COMMA);
											setState(1613);
											expr(0);
										}
									}
									setState(1618);
									_errHandler.sync(this);
									_la = _input.LA(1);
								}
								setState(1619);
								match(CLOSE_PAR);
							}
						}
						setState(1625);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Group_byContext extends ParserRuleContext {
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Group_byContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_group_by; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterGroup_by(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitGroup_by(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitGroup_by(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Group_byContext group_by() throws RecognitionException {
		Group_byContext _localctx = new Group_byContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_group_by);
		try {
			setState(1633);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,234,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(1628);
					column_name();
					{
						setState(1629);
						match(COMMA);
						setState(1630);
						column_name();
					}
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(1632);
					column_name();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Compound_operatorContext extends ParserRuleContext {
		public TerminalNode K_UNION() { return getToken(SQLiteParser.K_UNION, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public TerminalNode K_INTERSECT() { return getToken(SQLiteParser.K_INTERSECT, 0); }
		public TerminalNode K_EXCEPT() { return getToken(SQLiteParser.K_EXCEPT, 0); }
		public Compound_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compound_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCompound_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCompound_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCompound_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Compound_operatorContext compound_operator() throws RecognitionException {
		Compound_operatorContext _localctx = new Compound_operatorContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_compound_operator);
		try {
			setState(1640);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(1635);
					match(K_UNION);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(1636);
					match(K_UNION);
					setState(1637);
					match(K_ALL);
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(1638);
					match(K_INTERSECT);
				}
				break;
				case 4:
					enterOuterAlt(_localctx, 4);
				{
					setState(1639);
					match(K_EXCEPT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Cte_table_nameContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public Cte_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cte_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCte_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCte_table_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCte_table_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Cte_table_nameContext cte_table_name() throws RecognitionException {
		Cte_table_nameContext _localctx = new Cte_table_nameContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_cte_table_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1642);
				table_name();
				setState(1654);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPEN_PAR) {
					{
						setState(1643);
						match(OPEN_PAR);
						setState(1644);
						column_name();
						setState(1649);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
								{
									setState(1645);
									match(COMMA);
									setState(1646);
									column_name();
								}
							}
							setState(1651);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(1652);
						match(CLOSE_PAR);
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Signed_numberContext extends ParserRuleContext {
		public TerminalNode NUMERIC_LITERAL() { return getToken(SQLiteParser.NUMERIC_LITERAL, 0); }
		public Signed_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signed_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSigned_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSigned_number(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSigned_number(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Signed_numberContext signed_number() throws RecognitionException {
		Signed_numberContext _localctx = new Signed_numberContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_signed_number);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1657);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
						setState(1656);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
					}
				}

				setState(1659);
				match(NUMERIC_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Literal_valueContext extends ParserRuleContext {
		public TerminalNode NUMERIC_LITERAL() { return getToken(SQLiteParser.NUMERIC_LITERAL, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public TerminalNode BLOB_LITERAL() { return getToken(SQLiteParser.BLOB_LITERAL, 0); }
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public TerminalNode K_CURRENT_TIME() { return getToken(SQLiteParser.K_CURRENT_TIME, 0); }
		public TerminalNode K_CURRENT_DATE() { return getToken(SQLiteParser.K_CURRENT_DATE, 0); }
		public TerminalNode K_CURRENT_TIMESTAMP() { return getToken(SQLiteParser.K_CURRENT_TIMESTAMP, 0); }
		public Literal_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterLiteral_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitLiteral_value(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitLiteral_value(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Literal_valueContext literal_value() throws RecognitionException {
		Literal_valueContext _localctx = new Literal_valueContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_literal_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1661);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << K_CURRENT_DATE) | (1L << K_CURRENT_TIME) | (1L << K_CURRENT_TIMESTAMP))) != 0) || ((((_la - 104)) & ~0x3f) == 0 && ((1L << (_la - 104)) & ((1L << (K_NULL - 104)) | (1L << (NUMERIC_LITERAL - 104)) | (1L << (STRING_LITERAL - 104)) | (1L << (BLOB_LITERAL - 104)))) != 0)) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Unary_operatorContext extends ParserRuleContext {
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public Unary_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterUnary_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitUnary_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitUnary_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Unary_operatorContext unary_operator() throws RecognitionException {
		Unary_operatorContext _localctx = new Unary_operatorContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_unary_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1663);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << PLUS) | (1L << MINUS) | (1L << TILDE))) != 0) || _la==K_NOT) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Error_messageContext extends ParserRuleContext {
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public Error_messageContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_error_message; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterError_message(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitError_message(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitError_message(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Error_messageContext error_message() throws RecognitionException {
		Error_messageContext _localctx = new Error_messageContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_error_message);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1665);
				match(STRING_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Module_argumentContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Column_defContext column_def() {
			return getRuleContext(Column_defContext.class,0);
		}
		public Module_argumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_module_argument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterModule_argument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitModule_argument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitModule_argument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Module_argumentContext module_argument() throws RecognitionException {
		Module_argumentContext _localctx = new Module_argumentContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_module_argument);
		try {
			setState(1669);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,239,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(1667);
					expr(0);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(1668);
					column_def();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_aliasContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLiteParser.IDENTIFIER, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public Column_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_alias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitColumn_alias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_aliasContext column_alias() throws RecognitionException {
		Column_aliasContext _localctx = new Column_aliasContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_column_alias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1671);
				_la = _input.LA(1);
				if ( !(_la==IDENTIFIER || _la==STRING_LITERAL) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeywordContext extends ParserRuleContext {
		public TerminalNode K_ABORT() { return getToken(SQLiteParser.K_ABORT, 0); }
		public TerminalNode K_ACTION() { return getToken(SQLiteParser.K_ACTION, 0); }
		public TerminalNode K_ADD() { return getToken(SQLiteParser.K_ADD, 0); }
		public TerminalNode K_AFTER() { return getToken(SQLiteParser.K_AFTER, 0); }
		public TerminalNode K_ALL() { return getToken(SQLiteParser.K_ALL, 0); }
		public TerminalNode K_ALTER() { return getToken(SQLiteParser.K_ALTER, 0); }
		public TerminalNode K_ANALYZE() { return getToken(SQLiteParser.K_ANALYZE, 0); }
		public TerminalNode K_AND() { return getToken(SQLiteParser.K_AND, 0); }
		public TerminalNode K_AS() { return getToken(SQLiteParser.K_AS, 0); }
		public TerminalNode K_ASC() { return getToken(SQLiteParser.K_ASC, 0); }
		public TerminalNode K_ATTACH() { return getToken(SQLiteParser.K_ATTACH, 0); }
		public TerminalNode K_AUTOINCREMENT() { return getToken(SQLiteParser.K_AUTOINCREMENT, 0); }
		public TerminalNode K_BEFORE() { return getToken(SQLiteParser.K_BEFORE, 0); }
		public TerminalNode K_BEGIN() { return getToken(SQLiteParser.K_BEGIN, 0); }
		public TerminalNode K_BETWEEN() { return getToken(SQLiteParser.K_BETWEEN, 0); }
		public TerminalNode K_BY() { return getToken(SQLiteParser.K_BY, 0); }
		public TerminalNode K_CASCADE() { return getToken(SQLiteParser.K_CASCADE, 0); }
		public TerminalNode K_CASE() { return getToken(SQLiteParser.K_CASE, 0); }
		public TerminalNode K_CAST() { return getToken(SQLiteParser.K_CAST, 0); }
		public TerminalNode K_CHECK() { return getToken(SQLiteParser.K_CHECK, 0); }
		public TerminalNode K_COLLATE() { return getToken(SQLiteParser.K_COLLATE, 0); }
		public TerminalNode K_COLUMN() { return getToken(SQLiteParser.K_COLUMN, 0); }
		public TerminalNode K_COMMIT() { return getToken(SQLiteParser.K_COMMIT, 0); }
		public TerminalNode K_CONFLICT() { return getToken(SQLiteParser.K_CONFLICT, 0); }
		public TerminalNode K_CONSTRAINT() { return getToken(SQLiteParser.K_CONSTRAINT, 0); }
		public TerminalNode K_CREATE() { return getToken(SQLiteParser.K_CREATE, 0); }
		public TerminalNode K_CROSS() { return getToken(SQLiteParser.K_CROSS, 0); }
		public TerminalNode K_CURRENT_DATE() { return getToken(SQLiteParser.K_CURRENT_DATE, 0); }
		public TerminalNode K_CURRENT_TIME() { return getToken(SQLiteParser.K_CURRENT_TIME, 0); }
		public TerminalNode K_CURRENT_TIMESTAMP() { return getToken(SQLiteParser.K_CURRENT_TIMESTAMP, 0); }
		public TerminalNode K_DATABASE() { return getToken(SQLiteParser.K_DATABASE, 0); }
		public TerminalNode K_DEFAULT() { return getToken(SQLiteParser.K_DEFAULT, 0); }
		public TerminalNode K_DEFERRABLE() { return getToken(SQLiteParser.K_DEFERRABLE, 0); }
		public TerminalNode K_DEFERRED() { return getToken(SQLiteParser.K_DEFERRED, 0); }
		public TerminalNode K_DELETE() { return getToken(SQLiteParser.K_DELETE, 0); }
		public TerminalNode K_DESC() { return getToken(SQLiteParser.K_DESC, 0); }
		public TerminalNode K_DETACH() { return getToken(SQLiteParser.K_DETACH, 0); }
		public TerminalNode K_DISTINCT() { return getToken(SQLiteParser.K_DISTINCT, 0); }
		public TerminalNode K_DROP() { return getToken(SQLiteParser.K_DROP, 0); }
		public TerminalNode K_EACH() { return getToken(SQLiteParser.K_EACH, 0); }
		public TerminalNode K_ELSE() { return getToken(SQLiteParser.K_ELSE, 0); }
		public TerminalNode K_END() { return getToken(SQLiteParser.K_END, 0); }
		public TerminalNode K_ESCAPE() { return getToken(SQLiteParser.K_ESCAPE, 0); }
		public TerminalNode K_EXCEPT() { return getToken(SQLiteParser.K_EXCEPT, 0); }
		public TerminalNode K_EXCLUSIVE() { return getToken(SQLiteParser.K_EXCLUSIVE, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLiteParser.K_EXISTS, 0); }
		public TerminalNode K_EXPLAIN() { return getToken(SQLiteParser.K_EXPLAIN, 0); }
		public TerminalNode K_FAIL() { return getToken(SQLiteParser.K_FAIL, 0); }
		public TerminalNode K_FOR() { return getToken(SQLiteParser.K_FOR, 0); }
		public TerminalNode K_FOREIGN() { return getToken(SQLiteParser.K_FOREIGN, 0); }
		public TerminalNode K_FROM() { return getToken(SQLiteParser.K_FROM, 0); }
		public TerminalNode K_FULL() { return getToken(SQLiteParser.K_FULL, 0); }
		public TerminalNode K_GLOB() { return getToken(SQLiteParser.K_GLOB, 0); }
		public TerminalNode K_GROUP() { return getToken(SQLiteParser.K_GROUP, 0); }
		public TerminalNode K_HAVING() { return getToken(SQLiteParser.K_HAVING, 0); }
		public TerminalNode K_IF() { return getToken(SQLiteParser.K_IF, 0); }
		public TerminalNode K_IGNORE() { return getToken(SQLiteParser.K_IGNORE, 0); }
		public TerminalNode K_IMMEDIATE() { return getToken(SQLiteParser.K_IMMEDIATE, 0); }
		public TerminalNode K_IN() { return getToken(SQLiteParser.K_IN, 0); }
		public TerminalNode K_INDEX() { return getToken(SQLiteParser.K_INDEX, 0); }
		public TerminalNode K_INDEXED() { return getToken(SQLiteParser.K_INDEXED, 0); }
		public TerminalNode K_INITIALLY() { return getToken(SQLiteParser.K_INITIALLY, 0); }
		public TerminalNode K_INNER() { return getToken(SQLiteParser.K_INNER, 0); }
		public TerminalNode K_INSERT() { return getToken(SQLiteParser.K_INSERT, 0); }
		public TerminalNode K_INSTEAD() { return getToken(SQLiteParser.K_INSTEAD, 0); }
		public TerminalNode K_INTERSECT() { return getToken(SQLiteParser.K_INTERSECT, 0); }
		public TerminalNode K_INTO() { return getToken(SQLiteParser.K_INTO, 0); }
		public TerminalNode K_IS() { return getToken(SQLiteParser.K_IS, 0); }
		public TerminalNode K_ISNULL() { return getToken(SQLiteParser.K_ISNULL, 0); }
		public TerminalNode K_JOIN() { return getToken(SQLiteParser.K_JOIN, 0); }
		public TerminalNode K_KEY() { return getToken(SQLiteParser.K_KEY, 0); }
		public TerminalNode K_LEFT() { return getToken(SQLiteParser.K_LEFT, 0); }
		public TerminalNode K_LIKE() { return getToken(SQLiteParser.K_LIKE, 0); }
		public TerminalNode K_LIMIT() { return getToken(SQLiteParser.K_LIMIT, 0); }
		public TerminalNode K_MATCH() { return getToken(SQLiteParser.K_MATCH, 0); }
		public TerminalNode K_NATURAL() { return getToken(SQLiteParser.K_NATURAL, 0); }
		public TerminalNode K_NO() { return getToken(SQLiteParser.K_NO, 0); }
		public TerminalNode K_NOT() { return getToken(SQLiteParser.K_NOT, 0); }
		public TerminalNode K_NOTNULL() { return getToken(SQLiteParser.K_NOTNULL, 0); }
		public TerminalNode K_NULL() { return getToken(SQLiteParser.K_NULL, 0); }
		public TerminalNode K_OF() { return getToken(SQLiteParser.K_OF, 0); }
		public TerminalNode K_OFFSET() { return getToken(SQLiteParser.K_OFFSET, 0); }
		public TerminalNode K_ON() { return getToken(SQLiteParser.K_ON, 0); }
		public TerminalNode K_OR() { return getToken(SQLiteParser.K_OR, 0); }
		public TerminalNode K_ORDER() { return getToken(SQLiteParser.K_ORDER, 0); }
		public TerminalNode K_OUTER() { return getToken(SQLiteParser.K_OUTER, 0); }
		public TerminalNode K_PLAN() { return getToken(SQLiteParser.K_PLAN, 0); }
		public TerminalNode K_PRAGMA() { return getToken(SQLiteParser.K_PRAGMA, 0); }
		public TerminalNode K_PRIMARY() { return getToken(SQLiteParser.K_PRIMARY, 0); }
		public TerminalNode K_QUERY() { return getToken(SQLiteParser.K_QUERY, 0); }
		public TerminalNode K_RAISE() { return getToken(SQLiteParser.K_RAISE, 0); }
		public TerminalNode K_RECURSIVE() { return getToken(SQLiteParser.K_RECURSIVE, 0); }
		public TerminalNode K_REFERENCES() { return getToken(SQLiteParser.K_REFERENCES, 0); }
		public TerminalNode K_REGEXP() { return getToken(SQLiteParser.K_REGEXP, 0); }
		public TerminalNode K_REINDEX() { return getToken(SQLiteParser.K_REINDEX, 0); }
		public TerminalNode K_RELEASE() { return getToken(SQLiteParser.K_RELEASE, 0); }
		public TerminalNode K_RENAME() { return getToken(SQLiteParser.K_RENAME, 0); }
		public TerminalNode K_REPLACE() { return getToken(SQLiteParser.K_REPLACE, 0); }
		public TerminalNode K_RESTRICT() { return getToken(SQLiteParser.K_RESTRICT, 0); }
		public TerminalNode K_RIGHT() { return getToken(SQLiteParser.K_RIGHT, 0); }
		public TerminalNode K_ROLLBACK() { return getToken(SQLiteParser.K_ROLLBACK, 0); }
		public TerminalNode K_ROW() { return getToken(SQLiteParser.K_ROW, 0); }
		public TerminalNode K_SAVEPOINT() { return getToken(SQLiteParser.K_SAVEPOINT, 0); }
		public TerminalNode K_SELECT() { return getToken(SQLiteParser.K_SELECT, 0); }
		public TerminalNode K_SET() { return getToken(SQLiteParser.K_SET, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLiteParser.K_TABLE, 0); }
		public TerminalNode K_TEMP() { return getToken(SQLiteParser.K_TEMP, 0); }
		public TerminalNode K_TEMPORARY() { return getToken(SQLiteParser.K_TEMPORARY, 0); }
		public TerminalNode K_THEN() { return getToken(SQLiteParser.K_THEN, 0); }
		public TerminalNode K_TO() { return getToken(SQLiteParser.K_TO, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLiteParser.K_TRANSACTION, 0); }
		public TerminalNode K_TRIGGER() { return getToken(SQLiteParser.K_TRIGGER, 0); }
		public TerminalNode K_UNION() { return getToken(SQLiteParser.K_UNION, 0); }
		public TerminalNode K_UNIQUE() { return getToken(SQLiteParser.K_UNIQUE, 0); }
		public TerminalNode K_UPDATE() { return getToken(SQLiteParser.K_UPDATE, 0); }
		public TerminalNode K_USING() { return getToken(SQLiteParser.K_USING, 0); }
		public TerminalNode K_VACUUM() { return getToken(SQLiteParser.K_VACUUM, 0); }
		public TerminalNode K_VALUES() { return getToken(SQLiteParser.K_VALUES, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLiteParser.K_VIEW, 0); }
		public TerminalNode K_VIRTUAL() { return getToken(SQLiteParser.K_VIRTUAL, 0); }
		public TerminalNode K_WHEN() { return getToken(SQLiteParser.K_WHEN, 0); }
		public TerminalNode K_WHERE() { return getToken(SQLiteParser.K_WHERE, 0); }
		public TerminalNode K_WITH() { return getToken(SQLiteParser.K_WITH, 0); }
		public TerminalNode K_WITHOUT() { return getToken(SQLiteParser.K_WITHOUT, 0); }
		public KeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyword; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterKeyword(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitKeyword(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitKeyword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeywordContext keyword() throws RecognitionException {
		KeywordContext _localctx = new KeywordContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_keyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1673);
				_la = _input.LA(1);
				if ( !(((((_la - 25)) & ~0x3f) == 0 && ((1L << (_la - 25)) & ((1L << (K_ABORT - 25)) | (1L << (K_ACTION - 25)) | (1L << (K_ADD - 25)) | (1L << (K_AFTER - 25)) | (1L << (K_ALL - 25)) | (1L << (K_ALTER - 25)) | (1L << (K_ANALYZE - 25)) | (1L << (K_AND - 25)) | (1L << (K_AS - 25)) | (1L << (K_ASC - 25)) | (1L << (K_ATTACH - 25)) | (1L << (K_AUTOINCREMENT - 25)) | (1L << (K_BEFORE - 25)) | (1L << (K_BEGIN - 25)) | (1L << (K_BETWEEN - 25)) | (1L << (K_BY - 25)) | (1L << (K_CASCADE - 25)) | (1L << (K_CASE - 25)) | (1L << (K_CAST - 25)) | (1L << (K_CHECK - 25)) | (1L << (K_COLLATE - 25)) | (1L << (K_COLUMN - 25)) | (1L << (K_COMMIT - 25)) | (1L << (K_CONFLICT - 25)) | (1L << (K_CONSTRAINT - 25)) | (1L << (K_CREATE - 25)) | (1L << (K_CROSS - 25)) | (1L << (K_CURRENT_DATE - 25)) | (1L << (K_CURRENT_TIME - 25)) | (1L << (K_CURRENT_TIMESTAMP - 25)) | (1L << (K_DATABASE - 25)) | (1L << (K_DEFAULT - 25)) | (1L << (K_DEFERRABLE - 25)) | (1L << (K_DEFERRED - 25)) | (1L << (K_DELETE - 25)) | (1L << (K_DESC - 25)) | (1L << (K_DETACH - 25)) | (1L << (K_DISTINCT - 25)) | (1L << (K_DROP - 25)) | (1L << (K_EACH - 25)) | (1L << (K_ELSE - 25)) | (1L << (K_END - 25)) | (1L << (K_ESCAPE - 25)) | (1L << (K_EXCEPT - 25)) | (1L << (K_EXCLUSIVE - 25)) | (1L << (K_EXISTS - 25)) | (1L << (K_EXPLAIN - 25)) | (1L << (K_FAIL - 25)) | (1L << (K_FOR - 25)) | (1L << (K_FOREIGN - 25)) | (1L << (K_FROM - 25)) | (1L << (K_FULL - 25)) | (1L << (K_GLOB - 25)) | (1L << (K_GROUP - 25)) | (1L << (K_HAVING - 25)) | (1L << (K_IF - 25)) | (1L << (K_IGNORE - 25)) | (1L << (K_IMMEDIATE - 25)) | (1L << (K_IN - 25)) | (1L << (K_INDEX - 25)) | (1L << (K_INDEXED - 25)) | (1L << (K_INITIALLY - 25)) | (1L << (K_INNER - 25)) | (1L << (K_INSERT - 25)))) != 0) || ((((_la - 89)) & ~0x3f) == 0 && ((1L << (_la - 89)) & ((1L << (K_INSTEAD - 89)) | (1L << (K_INTERSECT - 89)) | (1L << (K_INTO - 89)) | (1L << (K_IS - 89)) | (1L << (K_ISNULL - 89)) | (1L << (K_JOIN - 89)) | (1L << (K_KEY - 89)) | (1L << (K_LEFT - 89)) | (1L << (K_LIKE - 89)) | (1L << (K_LIMIT - 89)) | (1L << (K_MATCH - 89)) | (1L << (K_NATURAL - 89)) | (1L << (K_NO - 89)) | (1L << (K_NOT - 89)) | (1L << (K_NOTNULL - 89)) | (1L << (K_NULL - 89)) | (1L << (K_OF - 89)) | (1L << (K_OFFSET - 89)) | (1L << (K_ON - 89)) | (1L << (K_OR - 89)) | (1L << (K_ORDER - 89)) | (1L << (K_OUTER - 89)) | (1L << (K_PLAN - 89)) | (1L << (K_PRAGMA - 89)) | (1L << (K_PRIMARY - 89)) | (1L << (K_QUERY - 89)) | (1L << (K_RAISE - 89)) | (1L << (K_RECURSIVE - 89)) | (1L << (K_REFERENCES - 89)) | (1L << (K_REGEXP - 89)) | (1L << (K_REINDEX - 89)) | (1L << (K_RELEASE - 89)) | (1L << (K_RENAME - 89)) | (1L << (K_REPLACE - 89)) | (1L << (K_RESTRICT - 89)) | (1L << (K_RIGHT - 89)) | (1L << (K_ROLLBACK - 89)) | (1L << (K_ROW - 89)) | (1L << (K_SAVEPOINT - 89)) | (1L << (K_SELECT - 89)) | (1L << (K_SET - 89)) | (1L << (K_TABLE - 89)) | (1L << (K_TEMP - 89)) | (1L << (K_TEMPORARY - 89)) | (1L << (K_THEN - 89)) | (1L << (K_TO - 89)) | (1L << (K_TRANSACTION - 89)) | (1L << (K_TRIGGER - 89)) | (1L << (K_UNION - 89)) | (1L << (K_UNIQUE - 89)) | (1L << (K_UPDATE - 89)) | (1L << (K_USING - 89)) | (1L << (K_VACUUM - 89)) | (1L << (K_VALUES - 89)) | (1L << (K_VIEW - 89)) | (1L << (K_VIRTUAL - 89)) | (1L << (K_WHEN - 89)) | (1L << (K_WHERE - 89)) | (1L << (K_WITH - 89)) | (1L << (K_WITHOUT - 89)))) != 0)) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public NameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NameContext name() throws RecognitionException {
		NameContext _localctx = new NameContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1675);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Function_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Function_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterFunction_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitFunction_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitFunction_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Function_nameContext function_name() throws RecognitionException {
		Function_nameContext _localctx = new Function_nameContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_function_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1677);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Database_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Database_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_database_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterDatabase_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitDatabase_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitDatabase_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Database_nameContext database_name() throws RecognitionException {
		Database_nameContext _localctx = new Database_nameContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_database_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1679);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitTable_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1681);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_or_index_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Table_or_index_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_or_index_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_or_index_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_or_index_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitTable_or_index_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_or_index_nameContext table_or_index_name() throws RecognitionException {
		Table_or_index_nameContext _localctx = new Table_or_index_nameContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_table_or_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1683);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class New_table_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public New_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_new_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterNew_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitNew_table_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitNew_table_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final New_table_nameContext new_table_name() throws RecognitionException {
		New_table_nameContext _localctx = new New_table_nameContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_new_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1685);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterColumn_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitColumn_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitColumn_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_nameContext column_name() throws RecognitionException {
		Column_nameContext _localctx = new Column_nameContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1687);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Collation_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Collation_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_collation_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterCollation_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitCollation_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitCollation_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Collation_nameContext collation_name() throws RecognitionException {
		Collation_nameContext _localctx = new Collation_nameContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_collation_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1689);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Foreign_tableContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Foreign_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_foreign_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterForeign_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitForeign_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitForeign_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Foreign_tableContext foreign_table() throws RecognitionException {
		Foreign_tableContext _localctx = new Foreign_tableContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_foreign_table);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1691);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Index_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Index_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterIndex_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitIndex_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitIndex_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Index_nameContext index_name() throws RecognitionException {
		Index_nameContext _localctx = new Index_nameContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1693);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Trigger_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Trigger_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trigger_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTrigger_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTrigger_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitTrigger_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Trigger_nameContext trigger_name() throws RecognitionException {
		Trigger_nameContext _localctx = new Trigger_nameContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_trigger_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1695);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class View_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public View_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_view_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterView_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitView_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitView_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final View_nameContext view_name() throws RecognitionException {
		View_nameContext _localctx = new View_nameContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_view_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1697);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Module_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Module_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_module_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterModule_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitModule_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitModule_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Module_nameContext module_name() throws RecognitionException {
		Module_nameContext _localctx = new Module_nameContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_module_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1699);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Pragma_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Pragma_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pragma_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterPragma_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitPragma_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitPragma_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Pragma_nameContext pragma_name() throws RecognitionException {
		Pragma_nameContext _localctx = new Pragma_nameContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_pragma_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1701);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Savepoint_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Savepoint_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_savepoint_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterSavepoint_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitSavepoint_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitSavepoint_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Savepoint_nameContext savepoint_name() throws RecognitionException {
		Savepoint_nameContext _localctx = new Savepoint_nameContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_savepoint_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1703);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_aliasContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Table_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTable_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTable_alias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitTable_alias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_aliasContext table_alias() throws RecognitionException {
		Table_aliasContext _localctx = new Table_aliasContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_table_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1705);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Transaction_nameContext extends ParserRuleContext {
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Transaction_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transaction_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterTransaction_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitTransaction_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitTransaction_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Transaction_nameContext transaction_name() throws RecognitionException {
		Transaction_nameContext _localctx = new Transaction_nameContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_transaction_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(1707);
				any_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Any_nameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLiteParser.IDENTIFIER, 0); }
		public KeywordContext keyword() {
			return getRuleContext(KeywordContext.class,0);
		}
		public TerminalNode STRING_LITERAL() { return getToken(SQLiteParser.STRING_LITERAL, 0); }
		public Any_nameContext any_name() {
			return getRuleContext(Any_nameContext.class,0);
		}
		public Any_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_any_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).enterAny_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLiteListener ) ((SQLiteListener)listener).exitAny_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLiteVisitor ) return ((SQLiteVisitor<? extends T>)visitor).visitAny_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Any_nameContext any_name() throws RecognitionException {
		Any_nameContext _localctx = new Any_nameContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_any_name);
		try {
			setState(1716);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case IDENTIFIER:
					enterOuterAlt(_localctx, 1);
				{
					setState(1709);
					match(IDENTIFIER);
				}
				break;
				case K_ABORT:
				case K_ACTION:
				case K_ADD:
				case K_AFTER:
				case K_ALL:
				case K_ALTER:
				case K_ANALYZE:
				case K_AND:
				case K_AS:
				case K_ASC:
				case K_ATTACH:
				case K_AUTOINCREMENT:
				case K_BEFORE:
				case K_BEGIN:
				case K_BETWEEN:
				case K_BY:
				case K_CASCADE:
				case K_CASE:
				case K_CAST:
				case K_CHECK:
				case K_COLLATE:
				case K_COLUMN:
				case K_COMMIT:
				case K_CONFLICT:
				case K_CONSTRAINT:
				case K_CREATE:
				case K_CROSS:
				case K_CURRENT_DATE:
				case K_CURRENT_TIME:
				case K_CURRENT_TIMESTAMP:
				case K_DATABASE:
				case K_DEFAULT:
				case K_DEFERRABLE:
				case K_DEFERRED:
				case K_DELETE:
				case K_DESC:
				case K_DETACH:
				case K_DISTINCT:
				case K_DROP:
				case K_EACH:
				case K_ELSE:
				case K_END:
				case K_ESCAPE:
				case K_EXCEPT:
				case K_EXCLUSIVE:
				case K_EXISTS:
				case K_EXPLAIN:
				case K_FAIL:
				case K_FOR:
				case K_FOREIGN:
				case K_FROM:
				case K_FULL:
				case K_GLOB:
				case K_GROUP:
				case K_HAVING:
				case K_IF:
				case K_IGNORE:
				case K_IMMEDIATE:
				case K_IN:
				case K_INDEX:
				case K_INDEXED:
				case K_INITIALLY:
				case K_INNER:
				case K_INSERT:
				case K_INSTEAD:
				case K_INTERSECT:
				case K_INTO:
				case K_IS:
				case K_ISNULL:
				case K_JOIN:
				case K_KEY:
				case K_LEFT:
				case K_LIKE:
				case K_LIMIT:
				case K_MATCH:
				case K_NATURAL:
				case K_NO:
				case K_NOT:
				case K_NOTNULL:
				case K_NULL:
				case K_OF:
				case K_OFFSET:
				case K_ON:
				case K_OR:
				case K_ORDER:
				case K_OUTER:
				case K_PLAN:
				case K_PRAGMA:
				case K_PRIMARY:
				case K_QUERY:
				case K_RAISE:
				case K_RECURSIVE:
				case K_REFERENCES:
				case K_REGEXP:
				case K_REINDEX:
				case K_RELEASE:
				case K_RENAME:
				case K_REPLACE:
				case K_RESTRICT:
				case K_RIGHT:
				case K_ROLLBACK:
				case K_ROW:
				case K_SAVEPOINT:
				case K_SELECT:
				case K_SET:
				case K_TABLE:
				case K_TEMP:
				case K_TEMPORARY:
				case K_THEN:
				case K_TO:
				case K_TRANSACTION:
				case K_TRIGGER:
				case K_UNION:
				case K_UNIQUE:
				case K_UPDATE:
				case K_USING:
				case K_VACUUM:
				case K_VALUES:
				case K_VIEW:
				case K_VIRTUAL:
				case K_WHEN:
				case K_WHERE:
				case K_WITH:
				case K_WITHOUT:
					enterOuterAlt(_localctx, 2);
				{
					setState(1710);
					keyword();
				}
				break;
				case STRING_LITERAL:
					enterOuterAlt(_localctx, 3);
				{
					setState(1711);
					match(STRING_LITERAL);
				}
				break;
				case OPEN_PAR:
					enterOuterAlt(_localctx, 4);
				{
					setState(1712);
					match(OPEN_PAR);
					setState(1713);
					any_name();
					setState(1714);
					match(CLOSE_PAR);
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
			case 39:
				return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
			case 0:
				return precpred(_ctx, 20);
			case 1:
				return precpred(_ctx, 19);
			case 2:
				return precpred(_ctx, 18);
			case 3:
				return precpred(_ctx, 17);
			case 4:
				return precpred(_ctx, 16);
			case 5:
				return precpred(_ctx, 15);
			case 6:
				return precpred(_ctx, 14);
			case 7:
				return precpred(_ctx, 13);
			case 8:
				return precpred(_ctx, 6);
			case 9:
				return precpred(_ctx, 5);
			case 10:
				return precpred(_ctx, 9);
			case 11:
				return precpred(_ctx, 8);
			case 12:
				return precpred(_ctx, 7);
			case 13:
				return precpred(_ctx, 4);
		}
		return true;
	}

	public static final String _serializedATN =
			"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u009f\u06b9\4\2\t"+
					"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
					"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
					"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
					"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
					"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
					",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
					"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
					"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
					"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
					"\3\2\3\2\7\2\u00ab\n\2\f\2\16\2\u00ae\13\2\3\2\3\2\3\3\3\3\3\3\3\4\7\4"+
					"\u00b6\n\4\f\4\16\4\u00b9\13\4\3\4\3\4\6\4\u00bd\n\4\r\4\16\4\u00be\3"+
					"\4\7\4\u00c2\n\4\f\4\16\4\u00c5\13\4\3\4\7\4\u00c8\n\4\f\4\16\4\u00cb"+
					"\13\4\3\5\3\5\3\5\5\5\u00d0\n\5\5\5\u00d2\n\5\3\5\3\5\3\5\3\5\3\5\3\5"+
					"\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
					"\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u00f2\n\5\3\6\3\6\3\6\3\6\3\6\5\6\u00f9"+
					"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0101\n\6\3\6\5\6\u0104\n\6\3\7\3\7\3"+
					"\7\3\7\3\7\3\7\3\7\5\7\u010d\n\7\3\b\3\b\5\b\u0111\n\b\3\b\3\b\3\b\3\b"+
					"\3\t\3\t\5\t\u0119\n\t\3\t\3\t\5\t\u011d\n\t\5\t\u011f\n\t\3\n\3\n\3\n"+
					"\5\n\u0124\n\n\5\n\u0126\n\n\3\13\3\13\5\13\u012a\n\13\3\13\3\13\3\13"+
					"\7\13\u012f\n\13\f\13\16\13\u0132\13\13\5\13\u0134\n\13\3\13\3\13\3\13"+
					"\5\13\u0139\n\13\3\13\3\13\5\13\u013d\n\13\3\13\6\13\u0140\n\13\r\13\16"+
					"\13\u0141\3\13\3\13\3\13\3\13\3\13\7\13\u0149\n\13\f\13\16\13\u014c\13"+
					"\13\5\13\u014e\n\13\3\13\3\13\3\13\3\13\5\13\u0154\n\13\5\13\u0156\n\13"+
					"\3\f\3\f\5\f\u015a\n\f\3\f\3\f\3\f\3\f\5\f\u0160\n\f\3\f\3\f\3\f\5\f\u0165"+
					"\n\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\7\f\u016e\n\f\f\f\16\f\u0171\13\f\3\f"+
					"\3\f\3\f\5\f\u0176\n\f\3\r\3\r\5\r\u017a\n\r\3\r\3\r\3\r\3\r\5\r\u0180"+
					"\n\r\3\r\3\r\3\r\5\r\u0185\n\r\3\r\3\r\3\r\3\r\3\r\7\r\u018c\n\r\f\r\16"+
					"\r\u018f\13\r\3\r\3\r\7\r\u0193\n\r\f\r\16\r\u0196\13\r\3\r\3\r\3\r\5"+
					"\r\u019b\n\r\3\r\3\r\5\r\u019f\n\r\3\16\3\16\5\16\u01a3\n\16\3\16\3\16"+
					"\3\16\3\16\5\16\u01a9\n\16\3\16\3\16\3\16\5\16\u01ae\n\16\3\16\3\16\3"+
					"\16\3\16\3\16\5\16\u01b5\n\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\7\16"+
					"\u01be\n\16\f\16\16\16\u01c1\13\16\5\16\u01c3\n\16\5\16\u01c5\n\16\3\16"+
					"\3\16\3\16\3\16\5\16\u01cb\n\16\3\16\3\16\3\16\3\16\5\16\u01d1\n\16\3"+
					"\16\3\16\5\16\u01d5\n\16\3\16\3\16\3\16\3\16\3\16\5\16\u01dc\n\16\3\16"+
					"\3\16\6\16\u01e0\n\16\r\16\16\16\u01e1\3\16\3\16\3\17\3\17\5\17\u01e8"+
					"\n\17\3\17\3\17\3\17\3\17\5\17\u01ee\n\17\3\17\3\17\3\17\5\17\u01f3\n"+
					"\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u01ff\n\20"+
					"\3\20\3\20\3\20\5\20\u0204\n\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\7\20"+
					"\u020d\n\20\f\20\16\20\u0210\13\20\3\20\3\20\5\20\u0214\n\20\3\21\5\21"+
					"\u0217\n\21\3\21\3\21\3\21\3\21\3\21\5\21\u021e\n\21\3\22\5\22\u0221\n"+
					"\22\3\22\3\22\3\22\3\22\3\22\5\22\u0228\n\22\3\22\3\22\3\22\3\22\3\22"+
					"\7\22\u022f\n\22\f\22\16\22\u0232\13\22\5\22\u0234\n\22\3\22\3\22\3\22"+
					"\3\22\5\22\u023a\n\22\5\22\u023c\n\22\3\23\3\23\5\23\u0240\n\23\3\23\3"+
					"\23\3\24\3\24\3\24\3\24\5\24\u0248\n\24\3\24\3\24\3\24\5\24\u024d\n\24"+
					"\3\24\3\24\3\25\3\25\3\25\3\25\5\25\u0255\n\25\3\25\3\25\3\25\5\25\u025a"+
					"\n\25\3\25\3\25\3\26\3\26\3\26\3\26\5\26\u0262\n\26\3\26\3\26\3\26\5\26"+
					"\u0267\n\26\3\26\3\26\3\27\3\27\3\27\3\27\5\27\u026f\n\27\3\27\3\27\3"+
					"\27\5\27\u0274\n\27\3\27\3\27\3\30\3\30\5\30\u027a\n\30\3\30\3\30\3\30"+
					"\7\30\u027f\n\30\f\30\16\30\u0282\13\30\5\30\u0284\n\30\3\30\3\30\3\30"+
					"\3\30\7\30\u028a\n\30\f\30\16\30\u028d\13\30\3\30\3\30\3\30\3\30\3\30"+
					"\7\30\u0294\n\30\f\30\16\30\u0297\13\30\5\30\u0299\n\30\3\30\3\30\3\30"+
					"\3\30\5\30\u029f\n\30\5\30\u02a1\n\30\3\31\5\31\u02a4\n\31\3\31\3\31\3"+
					"\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3"+
					"\31\5\31\u02b7\n\31\3\31\3\31\3\31\3\31\5\31\u02bd\n\31\3\31\3\31\3\31"+
					"\3\31\3\31\7\31\u02c4\n\31\f\31\16\31\u02c7\13\31\3\31\3\31\5\31\u02cb"+
					"\n\31\3\31\3\31\3\31\3\31\3\31\7\31\u02d2\n\31\f\31\16\31\u02d5\13\31"+
					"\3\31\3\31\3\31\3\31\3\31\3\31\7\31\u02dd\n\31\f\31\16\31\u02e0\13\31"+
					"\3\31\3\31\7\31\u02e4\n\31\f\31\16\31\u02e7\13\31\3\31\3\31\3\31\5\31"+
					"\u02ec\n\31\3\32\3\32\3\32\3\32\5\32\u02f2\n\32\3\32\3\32\3\32\3\32\3"+
					"\32\3\32\3\32\5\32\u02fb\n\32\3\33\3\33\3\33\3\33\3\33\5\33\u0302\n\33"+
					"\3\33\3\33\5\33\u0306\n\33\5\33\u0308\n\33\3\34\3\34\5\34\u030c\n\34\3"+
					"\34\3\34\3\35\3\35\3\35\5\35\u0313\n\35\5\35\u0315\n\35\3\35\3\35\5\35"+
					"\u0319\n\35\3\35\5\35\u031c\n\35\3\36\3\36\3\36\3\37\3\37\5\37\u0323\n"+
					"\37\3\37\3\37\3\37\7\37\u0328\n\37\f\37\16\37\u032b\13\37\5\37\u032d\n"+
					"\37\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u0335\n\37\f\37\16\37\u0338\13"+
					"\37\5\37\u033a\n\37\3\37\3\37\3\37\3\37\5\37\u0340\n\37\5\37\u0342\n\37"+
					"\3 \3 \5 \u0346\n \3 \3 \3 \7 \u034b\n \f \16 \u034e\13 \5 \u0350\n \3"+
					" \3 \3 \3 \7 \u0356\n \f \16 \u0359\13 \3 \3 \3 \3 \3 \7 \u0360\n \f "+
					"\16 \u0363\13 \5 \u0365\n \3 \3 \3 \3 \5 \u036b\n \5 \u036d\n \3!\3!\5"+
					"!\u0371\n!\3!\3!\3!\7!\u0376\n!\f!\16!\u0379\13!\3!\3!\3!\3!\7!\u037f"+
					"\n!\f!\16!\u0382\13!\3!\5!\u0385\n!\5!\u0387\n!\3!\3!\5!\u038b\n!\3!\3"+
					"!\3!\5!\u0390\n!\3!\3!\3!\3!\3!\7!\u0397\n!\f!\16!\u039a\13!\3!\3!\3!"+
					"\3!\3!\3!\7!\u03a2\n!\f!\16!\u03a5\13!\3!\3!\7!\u03a9\n!\f!\16!\u03ac"+
					"\13!\5!\u03ae\n!\3\"\5\"\u03b1\n\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\""+
					"\3\"\3\"\5\"\u03be\n\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\7\"\u03ca"+
					"\n\"\f\"\16\"\u03cd\13\"\3\"\3\"\5\"\u03d1\n\"\3#\5#\u03d4\n#\3#\3#\3"+
					"#\3#\3#\3#\3#\3#\3#\3#\3#\5#\u03e1\n#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\7"+
					"#\u03ed\n#\f#\16#\u03f0\13#\3#\3#\5#\u03f4\n#\3#\3#\3#\3#\3#\7#\u03fb"+
					"\n#\f#\16#\u03fe\13#\5#\u0400\n#\3#\3#\3#\3#\5#\u0406\n#\5#\u0408\n#\3"+
					"$\3$\3%\3%\5%\u040e\n%\3%\7%\u0411\n%\f%\16%\u0414\13%\3&\6&\u0417\n&"+
					"\r&\16&\u0418\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\5&\u0425\n&\3\'\3\'\5\'\u0429"+
					"\n\'\3\'\3\'\3\'\5\'\u042e\n\'\3\'\3\'\5\'\u0432\n\'\3\'\5\'\u0435\n\'"+
					"\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5\'\u0447"+
					"\n\'\3\'\3\'\3\'\5\'\u044c\n\'\3(\3(\3(\5(\u0451\n(\3)\3)\3)\3)\3)\3)"+
					"\5)\u0459\n)\3)\3)\3)\5)\u045e\n)\3)\3)\3)\3)\3)\3)\3)\5)\u0467\n)\3)"+
					"\3)\3)\7)\u046c\n)\f)\16)\u046f\13)\3)\5)\u0472\n)\3)\3)\3)\3)\3)\3)\3"+
					")\3)\3)\3)\3)\3)\3)\3)\5)\u0482\n)\3)\5)\u0485\n)\3)\3)\3)\3)\3)\3)\5"+
					")\u048d\n)\3)\3)\3)\3)\3)\6)\u0494\n)\r)\16)\u0495\3)\3)\5)\u049a\n)\3"+
					")\3)\3)\5)\u049f\n)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3"+
					")\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\5)\u04bd\n)\3)\3)\3)\3)\3)\3)\3)\3"+
					")\3)\3)\5)\u04c9\n)\3)\3)\3)\5)\u04ce\n)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3"+
					")\5)\u04da\n)\3)\3)\3)\3)\5)\u04e0\n)\3)\3)\3)\3)\3)\5)\u04e7\n)\3)\3"+
					")\5)\u04eb\n)\3)\3)\3)\3)\3)\3)\7)\u04f3\n)\f)\16)\u04f6\13)\5)\u04f8"+
					"\n)\3)\3)\3)\3)\5)\u04fe\n)\3)\5)\u0501\n)\7)\u0503\n)\f)\16)\u0506\13"+
					")\3*\3*\3*\3*\3*\3*\7*\u050e\n*\f*\16*\u0511\13*\3*\3*\5*\u0515\n*\3*"+
					"\3*\3*\3*\3*\3*\3*\3*\3*\3*\5*\u0521\n*\3*\3*\5*\u0525\n*\7*\u0527\n*"+
					"\f*\16*\u052a\13*\3*\5*\u052d\n*\3*\3*\3*\3*\3*\5*\u0534\n*\5*\u0536\n"+
					"*\3+\3+\3+\3+\3+\3+\5+\u053e\n+\3+\3+\3,\3,\3,\5,\u0545\n,\3,\5,\u0548"+
					"\n,\3-\3-\5-\u054c\n-\3-\3-\3-\5-\u0551\n-\3-\3-\3-\3-\7-\u0557\n-\f-"+
					"\16-\u055a\13-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\3-\7-\u056a\n-\f"+
					"-\16-\u056d\13-\3-\3-\3-\5-\u0572\n-\3.\3.\5.\u0576\n.\3.\3.\3.\3.\3."+
					"\3.\3.\3.\3.\3.\3.\3.\7.\u0584\n.\f.\16.\u0587\13.\3/\3/\3/\5/\u058c\n"+
					"/\3/\3/\3/\3/\3/\3/\5/\u0594\n/\3\60\3\60\3\60\5\60\u0599\n\60\3\60\5"+
					"\60\u059c\n\60\3\61\3\61\3\61\5\61\u05a1\n\61\3\62\3\62\3\62\3\62\3\62"+
					"\7\62\u05a8\n\62\f\62\16\62\u05ab\13\62\3\62\3\62\5\62\u05af\n\62\3\62"+
					"\3\62\3\62\3\62\3\62\3\63\3\63\3\63\3\63\3\63\3\63\3\63\5\63\u05bd\n\63"+
					"\3\63\5\63\u05c0\n\63\5\63\u05c2\n\63\3\64\3\64\3\64\5\64\u05c7\n\64\3"+
					"\64\3\64\5\64\u05cb\n\64\3\64\5\64\u05ce\n\64\3\64\3\64\3\64\3\64\3\64"+
					"\5\64\u05d5\n\64\3\64\3\64\3\64\3\64\7\64\u05db\n\64\f\64\16\64\u05de"+
					"\13\64\3\64\5\64\u05e1\n\64\3\64\3\64\5\64\u05e5\n\64\3\64\5\64\u05e8"+
					"\n\64\3\64\3\64\3\64\3\64\5\64\u05ee\n\64\3\64\5\64\u05f1\n\64\5\64\u05f3"+
					"\n\64\3\65\3\65\3\65\3\65\3\65\7\65\u05fa\n\65\f\65\16\65\u05fd\13\65"+
					"\3\66\3\66\5\66\u0601\n\66\3\66\3\66\5\66\u0605\n\66\3\66\3\66\5\66\u0609"+
					"\n\66\3\66\5\66\u060c\n\66\3\67\3\67\3\67\3\67\3\67\3\67\3\67\7\67\u0615"+
					"\n\67\f\67\16\67\u0618\13\67\3\67\3\67\5\67\u061c\n\67\38\38\58\u0620"+
					"\n8\38\38\38\78\u0625\n8\f8\168\u0628\138\38\38\38\38\78\u062e\n8\f8\16"+
					"8\u0631\138\38\58\u0634\n8\58\u0636\n8\38\38\58\u063a\n8\38\38\38\58\u063f"+
					"\n8\38\38\38\38\38\78\u0646\n8\f8\168\u0649\138\38\38\38\38\38\38\78\u0651"+
					"\n8\f8\168\u0654\138\38\38\78\u0658\n8\f8\168\u065b\138\58\u065d\n8\3"+
					"9\39\39\39\39\59\u0664\n9\3:\3:\3:\3:\3:\5:\u066b\n:\3;\3;\3;\3;\3;\7"+
					";\u0672\n;\f;\16;\u0675\13;\3;\3;\5;\u0679\n;\3<\5<\u067c\n<\3<\3<\3="+
					"\3=\3>\3>\3?\3?\3@\3@\5@\u0688\n@\3A\3A\3B\3B\3C\3C\3D\3D\3E\3E\3F\3F"+
					"\3G\3G\3H\3H\3I\3I\3J\3J\3K\3K\3L\3L\3M\3M\3N\3N\3O\3O\3P\3P\3Q\3Q\3R"+
					"\3R\3S\3S\3T\3T\3T\3T\3T\3T\3T\5T\u06b7\nT\3T\2\3PU\2\4\6\b\n\f\16\20"+
					"\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhj"+
					"lnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092"+
					"\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\2\24\5\2"+
					"<<GGTT\4\2\61\61DD\4\2\7\7ll\3\2\u0085\u0086\4\2\37\37@@\4\2$$>>\7\2\33"+
					"\33JJSS||\177\177\4\2\t\t\16\17\3\2\n\13\3\2\20\23\3\2\24\27\6\2OOcce"+
					"exx\4\2==\u008d\u008d\5\2\33\33JJ\177\177\6\2\668jj\u0098\u0098\u009a"+
					"\u009b\4\2\n\fhh\4\2\u0097\u0097\u009a\u009a\3\2\33\u0096\2\u07c2\2\u00ac"+
					"\3\2\2\2\4\u00b1\3\2\2\2\6\u00b7\3\2\2\2\b\u00d1\3\2\2\2\n\u00f3\3\2\2"+
					"\2\f\u0105\3\2\2\2\16\u010e\3\2\2\2\20\u0116\3\2\2\2\22\u0120\3\2\2\2"+
					"\24\u0133\3\2\2\2\26\u0157\3\2\2\2\30\u0177\3\2\2\2\32\u01a0\3\2\2\2\34"+
					"\u01e5\3\2\2\2\36\u01f8\3\2\2\2 \u0216\3\2\2\2\"\u0220\3\2\2\2$\u023d"+
					"\3\2\2\2&\u0243\3\2\2\2(\u0250\3\2\2\2*\u025d\3\2\2\2,\u026a\3\2\2\2."+
					"\u0283\3\2\2\2\60\u02a3\3\2\2\2\62\u02ed\3\2\2\2\64\u02fc\3\2\2\2\66\u0309"+
					"\3\2\2\28\u030f\3\2\2\2:\u031d\3\2\2\2<\u032c\3\2\2\2>\u034f\3\2\2\2@"+
					"\u03ad\3\2\2\2B\u03b0\3\2\2\2D\u03d3\3\2\2\2F\u0409\3\2\2\2H\u040b\3\2"+
					"\2\2J\u0416\3\2\2\2L\u0428\3\2\2\2N\u0450\3\2\2\2P\u049e\3\2\2\2R\u0507"+
					"\3\2\2\2T\u0537\3\2\2\2V\u0541\3\2\2\2X\u054b\3\2\2\2Z\u0573\3\2\2\2\\"+
					"\u058b\3\2\2\2^\u0595\3\2\2\2`\u05a0\3\2\2\2b\u05a2\3\2\2\2d\u05c1\3\2"+
					"\2\2f\u05f2\3\2\2\2h\u05f4\3\2\2\2j\u060b\3\2\2\2l\u061b\3\2\2\2n\u065c"+
					"\3\2\2\2p\u0663\3\2\2\2r\u066a\3\2\2\2t\u066c\3\2\2\2v\u067b\3\2\2\2x"+
					"\u067f\3\2\2\2z\u0681\3\2\2\2|\u0683\3\2\2\2~\u0687\3\2\2\2\u0080\u0689"+
					"\3\2\2\2\u0082\u068b\3\2\2\2\u0084\u068d\3\2\2\2\u0086\u068f\3\2\2\2\u0088"+
					"\u0691\3\2\2\2\u008a\u0693\3\2\2\2\u008c\u0695\3\2\2\2\u008e\u0697\3\2"+
					"\2\2\u0090\u0699\3\2\2\2\u0092\u069b\3\2\2\2\u0094\u069d\3\2\2\2\u0096"+
					"\u069f\3\2\2\2\u0098\u06a1\3\2\2\2\u009a\u06a3\3\2\2\2\u009c\u06a5\3\2"+
					"\2\2\u009e\u06a7\3\2\2\2\u00a0\u06a9\3\2\2\2\u00a2\u06ab\3\2\2\2\u00a4"+
					"\u06ad\3\2\2\2\u00a6\u06b6\3\2\2\2\u00a8\u00ab\5\6\4\2\u00a9\u00ab\5\4"+
					"\3\2\u00aa\u00a8\3\2\2\2\u00aa\u00a9\3\2\2\2\u00ab\u00ae\3\2\2\2\u00ac"+
					"\u00aa\3\2\2\2\u00ac\u00ad\3\2\2\2\u00ad\u00af\3\2\2\2\u00ae\u00ac\3\2"+
					"\2\2\u00af\u00b0\7\2\2\3\u00b0\3\3\2\2\2\u00b1\u00b2\7\u009f\2\2\u00b2"+
					"\u00b3\b\3\1\2\u00b3\5\3\2\2\2\u00b4\u00b6\7\3\2\2\u00b5\u00b4\3\2\2\2"+
					"\u00b6\u00b9\3\2\2\2\u00b7\u00b5\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00ba"+
					"\3\2\2\2\u00b9\u00b7\3\2\2\2\u00ba\u00c3\5\b\5\2\u00bb\u00bd\7\3\2\2\u00bc"+
					"\u00bb\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00bc\3\2\2\2\u00be\u00bf\3\2"+
					"\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c2\5\b\5\2\u00c1\u00bc\3\2\2\2\u00c2"+
					"\u00c5\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c9\3\2"+
					"\2\2\u00c5\u00c3\3\2\2\2\u00c6\u00c8\7\3\2\2\u00c7\u00c6\3\2\2\2\u00c8"+
					"\u00cb\3\2\2\2\u00c9\u00c7\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\7\3\2\2\2"+
					"\u00cb\u00c9\3\2\2\2\u00cc\u00cf\7I\2\2\u00cd\u00ce\7t\2\2\u00ce\u00d0"+
					"\7q\2\2\u00cf\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d2\3\2\2\2\u00d1"+
					"\u00cc\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00f1\3\2\2\2\u00d3\u00f2\5\n"+
					"\6\2\u00d4\u00f2\5\f\7\2\u00d5\u00f2\5\16\b\2\u00d6\u00f2\5\20\t\2\u00d7"+
					"\u00f2\5\22\n\2\u00d8\u00f2\5\24\13\2\u00d9\u00f2\5\26\f\2\u00da\u00f2"+
					"\5\30\r\2\u00db\u00f2\5\32\16\2\u00dc\u00f2\5\34\17\2\u00dd\u00f2\5\36"+
					"\20\2\u00de\u00f2\5 \21\2\u00df\u00f2\5\"\22\2\u00e0\u00f2\5$\23\2\u00e1"+
					"\u00f2\5&\24\2\u00e2\u00f2\5(\25\2\u00e3\u00f2\5*\26\2\u00e4\u00f2\5,"+
					"\27\2\u00e5\u00f2\5.\30\2\u00e6\u00f2\5\60\31\2\u00e7\u00f2\5\62\32\2"+
					"\u00e8\u00f2\5\64\33\2\u00e9\u00f2\5\66\34\2\u00ea\u00f2\58\35\2\u00eb"+
					"\u00f2\5:\36\2\u00ec\u00f2\5<\37\2\u00ed\u00f2\5> \2\u00ee\u00f2\5B\""+
					"\2\u00ef\u00f2\5D#\2\u00f0\u00f2\5F$\2\u00f1\u00d3\3\2\2\2\u00f1\u00d4"+
					"\3\2\2\2\u00f1\u00d5\3\2\2\2\u00f1\u00d6\3\2\2\2\u00f1\u00d7\3\2\2\2\u00f1"+
					"\u00d8\3\2\2\2\u00f1\u00d9\3\2\2\2\u00f1\u00da\3\2\2\2\u00f1\u00db\3\2"+
					"\2\2\u00f1\u00dc\3\2\2\2\u00f1\u00dd\3\2\2\2\u00f1\u00de\3\2\2\2\u00f1"+
					"\u00df\3\2\2\2\u00f1\u00e0\3\2\2\2\u00f1\u00e1\3\2\2\2\u00f1\u00e2\3\2"+
					"\2\2\u00f1\u00e3\3\2\2\2\u00f1\u00e4\3\2\2\2\u00f1\u00e5\3\2\2\2\u00f1"+
					"\u00e6\3\2\2\2\u00f1\u00e7\3\2\2\2\u00f1\u00e8\3\2\2\2\u00f1\u00e9\3\2"+
					"\2\2\u00f1\u00ea\3\2\2\2\u00f1\u00eb\3\2\2\2\u00f1\u00ec\3\2\2\2\u00f1"+
					"\u00ed\3\2\2\2\u00f1\u00ee\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f1\u00f0\3\2"+
					"\2\2\u00f2\t\3\2\2\2\u00f3\u00f4\7 \2\2\u00f4\u00f8\7\u0084\2\2\u00f5"+
					"\u00f6\5\u0088E\2\u00f6\u00f7\7\4\2\2\u00f7\u00f9\3\2\2\2\u00f8\u00f5"+
					"\3\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u0103\5\u008aF"+
					"\2\u00fb\u00fc\7{\2\2\u00fc\u00fd\7\u0088\2\2\u00fd\u0104\5\u008eH\2\u00fe"+
					"\u0100\7\35\2\2\u00ff\u0101\7\60\2\2\u0100\u00ff\3\2\2\2\u0100\u0101\3"+
					"\2\2\2\u0101\u0102\3\2\2\2\u0102\u0104\5H%\2\u0103\u00fb\3\2\2\2\u0103"+
					"\u00fe\3\2\2\2\u0104\13\3\2\2\2\u0105\u010c\7!\2\2\u0106\u010d\5\u0088"+
					"E\2\u0107\u010d\5\u008cG\2\u0108\u0109\5\u0088E\2\u0109\u010a\7\4\2\2"+
					"\u010a\u010b\5\u008cG\2\u010b\u010d\3\2\2\2\u010c\u0106\3\2\2\2\u010c"+
					"\u0107\3\2\2\2\u010c\u0108\3\2\2\2\u010c\u010d\3\2\2\2\u010d\r\3\2\2\2"+
					"\u010e\u0110\7%\2\2\u010f\u0111\79\2\2\u0110\u010f\3\2\2\2\u0110\u0111"+
					"\3\2\2\2\u0111\u0112\3\2\2\2\u0112\u0113\5P)\2\u0113\u0114\7#\2\2\u0114"+
					"\u0115\5\u0088E\2\u0115\17\3\2\2\2\u0116\u0118\7(\2\2\u0117\u0119\t\2"+
					"\2\2\u0118\u0117\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u011e\3\2\2\2\u011a"+
					"\u011c\7\u0089\2\2\u011b\u011d\5\u00a4S\2\u011c\u011b\3\2\2\2\u011c\u011d"+
					"\3\2\2\2\u011d\u011f\3\2\2\2\u011e\u011a\3\2\2\2\u011e\u011f\3\2\2\2\u011f"+
					"\21\3\2\2\2\u0120\u0125\t\3\2\2\u0121\u0123\7\u0089\2\2\u0122\u0124\5"+
					"\u00a4S\2\u0123\u0122\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0126\3\2\2\2"+
					"\u0125\u0121\3\2\2\2\u0125\u0126\3\2\2\2\u0126\23\3\2\2\2\u0127\u0129"+
					"\7\u0095\2\2\u0128\u012a\7v\2\2\u0129\u0128\3\2\2\2\u0129\u012a\3\2\2"+
					"\2\u012a\u012b\3\2\2\2\u012b\u0130\5b\62\2\u012c\u012d\7\7\2\2\u012d\u012f"+
					"\5b\62\2\u012e\u012c\3\2\2\2\u012f\u0132\3\2\2\2\u0130\u012e\3\2\2\2\u0130"+
					"\u0131\3\2\2\2\u0131\u0134\3\2\2\2\u0132\u0130\3\2\2\2\u0133\u0127\3\2"+
					"\2\2\u0133\u0134\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u013f\5n8\2\u0136\u0138"+
					"\7\u008b\2\2\u0137\u0139\7\37\2\2\u0138\u0137\3\2\2\2\u0138\u0139\3\2"+
					"\2\2\u0139\u013d\3\2\2\2\u013a\u013d\7\\\2\2\u013b\u013d\7F\2\2\u013c"+
					"\u0136\3\2\2\2\u013c\u013a\3\2\2\2\u013c\u013b\3\2\2\2\u013d\u013e\3\2"+
					"\2\2\u013e\u0140\5n8\2\u013f\u013c\3\2\2\2\u0140\u0141\3\2\2\2\u0141\u013f"+
					"\3\2\2\2\u0141\u0142\3\2\2\2\u0142\u014d\3\2\2\2\u0143\u0144\7o\2\2\u0144"+
					"\u0145\7*\2\2\u0145\u014a\5^\60\2\u0146\u0147\7\7\2\2\u0147\u0149\5^\60"+
					"\2\u0148\u0146\3\2\2\2\u0149\u014c\3\2\2\2\u014a\u0148\3\2\2\2\u014a\u014b"+
					"\3\2\2\2\u014b\u014e\3\2\2\2\u014c\u014a\3\2\2\2\u014d\u0143\3\2\2\2\u014d"+
					"\u014e\3\2\2\2\u014e\u0155\3\2\2\2\u014f\u0150\7d\2\2\u0150\u0153\5P)"+
					"\2\u0151\u0152\t\4\2\2\u0152\u0154\5P)\2\u0153\u0151\3\2\2\2\u0153\u0154"+
					"\3\2\2\2\u0154\u0156\3\2\2\2\u0155\u014f\3\2\2\2\u0155\u0156\3\2\2\2\u0156"+
					"\25\3\2\2\2\u0157\u0159\7\64\2\2\u0158\u015a\7\u008c\2\2\u0159\u0158\3"+
					"\2\2\2\u0159\u015a\3\2\2\2\u015a\u015b\3\2\2\2\u015b\u015f\7V\2\2\u015c"+
					"\u015d\7R\2\2\u015d\u015e\7h\2\2\u015e\u0160\7H\2\2\u015f\u015c\3\2\2"+
					"\2\u015f\u0160\3\2\2\2\u0160\u0164\3\2\2\2\u0161\u0162\5\u0088E\2\u0162"+
					"\u0163\7\4\2\2\u0163\u0165\3\2\2\2\u0164\u0161\3\2\2\2\u0164\u0165\3\2"+
					"\2\2\u0165\u0166\3\2\2\2\u0166\u0167\5\u0096L\2\u0167\u0168\7m\2\2\u0168"+
					"\u0169\5\u008aF\2\u0169\u016a\7\5\2\2\u016a\u016f\5V,\2\u016b\u016c\7"+
					"\7\2\2\u016c\u016e\5V,\2\u016d\u016b\3\2\2\2\u016e\u0171\3\2\2\2\u016f"+
					"\u016d\3\2\2\2\u016f\u0170\3\2\2\2\u0170\u0172\3\2\2\2\u0171\u016f\3\2"+
					"\2\2\u0172\u0175\7\6\2\2\u0173\u0174\7\u0094\2\2\u0174\u0176\5P)\2\u0175"+
					"\u0173\3\2\2\2\u0175\u0176\3\2\2\2\u0176\27\3\2\2\2\u0177\u0179\7\64\2"+
					"\2\u0178\u017a\t\5\2\2\u0179\u0178\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u017b"+
					"\3\2\2\2\u017b\u017f\7\u0084\2\2\u017c\u017d\7R\2\2\u017d\u017e\7h\2\2"+
					"\u017e\u0180\7H\2\2\u017f\u017c\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u0184"+
					"\3\2\2\2\u0181\u0182\5\u0088E\2\u0182\u0183\7\4\2\2\u0183\u0185\3\2\2"+
					"\2\u0184\u0181\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u0186\3\2\2\2\u0186\u019e"+
					"\5\u008aF\2\u0187\u0188\7\5\2\2\u0188\u018d\5H%\2\u0189\u018a\7\7\2\2"+
					"\u018a\u018c\5H%\2\u018b\u0189\3\2\2\2\u018c\u018f\3\2\2\2\u018d\u018b"+
					"\3\2\2\2\u018d\u018e\3\2\2\2\u018e\u0194\3\2\2\2\u018f\u018d\3\2\2\2\u0190"+
					"\u0191\7\7\2\2\u0191\u0193\5X-\2\u0192\u0190\3\2\2\2\u0193\u0196\3\2\2"+
					"\2\u0194\u0192\3\2\2\2\u0194\u0195\3\2\2\2\u0195\u0197\3\2\2\2\u0196\u0194"+
					"\3\2\2\2\u0197\u019a\7\6\2\2\u0198\u0199\7\u0096\2\2\u0199\u019b\7\u0097"+
					"\2\2\u019a\u0198\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019f\3\2\2\2\u019c"+
					"\u019d\7#\2\2\u019d\u019f\5> \2\u019e\u0187\3\2\2\2\u019e\u019c\3\2\2"+
					"\2\u019f\31\3\2\2\2\u01a0\u01a2\7\64\2\2\u01a1\u01a3\t\5\2\2\u01a2\u01a1"+
					"\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3\u01a4\3\2\2\2\u01a4\u01a8\7\u008a\2"+
					"\2\u01a5\u01a6\7R\2\2\u01a6\u01a7\7h\2\2\u01a7\u01a9\7H\2\2\u01a8\u01a5"+
					"\3\2\2\2\u01a8\u01a9\3\2\2\2\u01a9\u01ad\3\2\2\2\u01aa\u01ab\5\u0088E"+
					"\2\u01ab\u01ac\7\4\2\2\u01ac\u01ae\3\2\2\2\u01ad\u01aa\3\2\2\2\u01ad\u01ae"+
					"\3\2\2\2\u01ae\u01af\3\2\2\2\u01af\u01b4\5\u0098M\2\u01b0\u01b5\7\'\2"+
					"\2\u01b1\u01b5\7\36\2\2\u01b2\u01b3\7[\2\2\u01b3\u01b5\7k\2\2\u01b4\u01b0"+
					"\3\2\2\2\u01b4\u01b1\3\2\2\2\u01b4\u01b2\3\2\2\2\u01b4\u01b5\3\2\2\2\u01b5"+
					"\u01c4\3\2\2\2\u01b6\u01c5\7=\2\2\u01b7\u01c5\7Z\2\2\u01b8\u01c2\7\u008d"+
					"\2\2\u01b9\u01ba\7k\2\2\u01ba\u01bf\5\u0090I\2\u01bb\u01bc\7\7\2\2\u01bc"+
					"\u01be\5\u0090I\2\u01bd\u01bb\3\2\2\2\u01be\u01c1\3\2\2\2\u01bf\u01bd"+
					"\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0\u01c3\3\2\2\2\u01c1\u01bf\3\2\2\2\u01c2"+
					"\u01b9\3\2\2\2\u01c2\u01c3\3\2\2\2\u01c3\u01c5\3\2\2\2\u01c4\u01b6\3\2"+
					"\2\2\u01c4\u01b7\3\2\2\2\u01c4\u01b8\3\2\2\2\u01c5\u01c6\3\2\2\2\u01c6"+
					"\u01ca\7m\2\2\u01c7\u01c8\5\u0088E\2\u01c8\u01c9\7\4\2\2\u01c9\u01cb\3"+
					"\2\2\2\u01ca\u01c7\3\2\2\2\u01ca\u01cb\3\2\2\2\u01cb\u01cc\3\2\2\2\u01cc"+
					"\u01d0\5\u008aF\2\u01cd\u01ce\7K\2\2\u01ce\u01cf\7B\2\2\u01cf\u01d1\7"+
					"\u0080\2\2\u01d0\u01cd\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d4\3\2\2\2"+
					"\u01d2\u01d3\7\u0093\2\2\u01d3\u01d5\5P)\2\u01d4\u01d2\3\2\2\2\u01d4\u01d5"+
					"\3\2\2\2\u01d5\u01d6\3\2\2\2\u01d6\u01df\7(\2\2\u01d7\u01dc\5B\"\2\u01d8"+
					"\u01dc\5\60\31\2\u01d9\u01dc\5 \21\2\u01da\u01dc\5> \2\u01db\u01d7\3\2"+
					"\2\2\u01db\u01d8\3\2\2\2\u01db\u01d9\3\2\2\2\u01db\u01da\3\2\2\2\u01dc"+
					"\u01dd\3\2\2\2\u01dd\u01de\7\3\2\2\u01de\u01e0\3\2\2\2\u01df\u01db\3\2"+
					"\2\2\u01e0\u01e1\3\2\2\2\u01e1\u01df\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2"+
					"\u01e3\3\2\2\2\u01e3\u01e4\7D\2\2\u01e4\33\3\2\2\2\u01e5\u01e7\7\64\2"+
					"\2\u01e6\u01e8\t\5\2\2\u01e7\u01e6\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8\u01e9"+
					"\3\2\2\2\u01e9\u01ed\7\u0091\2\2\u01ea\u01eb\7R\2\2\u01eb\u01ec\7h\2\2"+
					"\u01ec\u01ee\7H\2\2\u01ed\u01ea\3\2\2\2\u01ed\u01ee\3\2\2\2\u01ee\u01f2"+
					"\3\2\2\2\u01ef\u01f0\5\u0088E\2\u01f0\u01f1\7\4\2\2\u01f1\u01f3\3\2\2"+
					"\2\u01f2\u01ef\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4\u01f5"+
					"\5\u009aN\2\u01f5\u01f6\7#\2\2\u01f6\u01f7\5> \2\u01f7\35\3\2\2\2\u01f8"+
					"\u01f9\7\64\2\2\u01f9\u01fa\7\u0092\2\2\u01fa\u01fe\7\u0084\2\2\u01fb"+
					"\u01fc\7R\2\2\u01fc\u01fd\7h\2\2\u01fd\u01ff\7H\2\2\u01fe\u01fb\3\2\2"+
					"\2\u01fe\u01ff\3\2\2\2\u01ff\u0203\3\2\2\2\u0200\u0201\5\u0088E\2\u0201"+
					"\u0202\7\4\2\2\u0202\u0204\3\2\2\2\u0203\u0200\3\2\2\2\u0203\u0204\3\2"+
					"\2\2\u0204\u0205\3\2\2\2\u0205\u0206\5\u008aF\2\u0206\u0207\7\u008e\2"+
					"\2\u0207\u0213\5\u009cO\2\u0208\u0209\7\5\2\2\u0209\u020e\5~@\2\u020a"+
					"\u020b\7\7\2\2\u020b\u020d\5~@\2\u020c\u020a\3\2\2\2\u020d\u0210\3\2\2"+
					"\2\u020e\u020c\3\2\2\2\u020e\u020f\3\2\2\2\u020f\u0211\3\2\2\2\u0210\u020e"+
					"\3\2\2\2\u0211\u0212\7\6\2\2\u0212\u0214\3\2\2\2\u0213\u0208\3\2\2\2\u0213"+
					"\u0214\3\2\2\2\u0214\37\3\2\2\2\u0215\u0217\5Z.\2\u0216\u0215\3\2\2\2"+
					"\u0216\u0217\3\2\2\2\u0217\u0218\3\2\2\2\u0218\u0219\7=\2\2\u0219\u021a"+
					"\7M\2\2\u021a\u021d\5\\/\2\u021b\u021c\7\u0094\2\2\u021c\u021e\5P)\2\u021d"+
					"\u021b\3\2\2\2\u021d\u021e\3\2\2\2\u021e!\3\2\2\2\u021f\u0221\5Z.\2\u0220"+
					"\u021f\3\2\2\2\u0220\u0221\3\2\2\2\u0221\u0222\3\2\2\2\u0222\u0223\7="+
					"\2\2\u0223\u0224\7M\2\2\u0224\u0227\5\\/\2\u0225\u0226\7\u0094\2\2\u0226"+
					"\u0228\5P)\2\u0227\u0225\3\2\2\2\u0227\u0228\3\2\2\2\u0228\u023b\3\2\2"+
					"\2\u0229\u022a\7o\2\2\u022a\u022b\7*\2\2\u022b\u0230\5^\60\2\u022c\u022d"+
					"\7\7\2\2\u022d\u022f\5^\60\2\u022e\u022c\3\2\2\2\u022f\u0232\3\2\2\2\u0230"+
					"\u022e\3\2\2\2\u0230\u0231\3\2\2\2\u0231\u0234\3\2\2\2\u0232\u0230\3\2"+
					"\2\2\u0233\u0229\3\2\2\2\u0233\u0234\3\2\2\2\u0234\u0235\3\2\2\2\u0235"+
					"\u0236\7d\2\2\u0236\u0239\5P)\2\u0237\u0238\t\4\2\2\u0238\u023a\5P)\2"+
					"\u0239\u0237\3\2\2\2\u0239\u023a\3\2\2\2\u023a\u023c\3\2\2\2\u023b\u0233"+
					"\3\2\2\2\u023b\u023c\3\2\2\2\u023c#\3\2\2\2\u023d\u023f\7?\2\2\u023e\u0240"+
					"\79\2\2\u023f\u023e\3\2\2\2\u023f\u0240\3\2\2\2\u0240\u0241\3\2\2\2\u0241"+
					"\u0242\5\u0088E\2\u0242%\3\2\2\2\u0243\u0244\7A\2\2\u0244\u0247\7V\2\2"+
					"\u0245\u0246\7R\2\2\u0246\u0248\7H\2\2\u0247\u0245\3\2\2\2\u0247\u0248"+
					"\3\2\2\2\u0248\u024c\3\2\2\2\u0249\u024a\5\u0088E\2\u024a\u024b\7\4\2"+
					"\2\u024b\u024d\3\2\2\2\u024c\u0249\3\2\2\2\u024c\u024d\3\2\2\2\u024d\u024e"+
					"\3\2\2\2\u024e\u024f\5\u0096L\2\u024f\'\3\2\2\2\u0250\u0251\7A\2\2\u0251"+
					"\u0254\7\u0084\2\2\u0252\u0253\7R\2\2\u0253\u0255\7H\2\2\u0254\u0252\3"+
					"\2\2\2\u0254\u0255\3\2\2\2\u0255\u0259\3\2\2\2\u0256\u0257\5\u0088E\2"+
					"\u0257\u0258\7\4\2\2\u0258\u025a\3\2\2\2\u0259\u0256\3\2\2\2\u0259\u025a"+
					"\3\2\2\2\u025a\u025b\3\2\2\2\u025b\u025c\5\u008aF\2\u025c)\3\2\2\2\u025d"+
					"\u025e\7A\2\2\u025e\u0261\7\u008a\2\2\u025f\u0260\7R\2\2\u0260\u0262\7"+
					"H\2\2\u0261\u025f\3\2\2\2\u0261\u0262\3\2\2\2\u0262\u0266\3\2\2\2\u0263"+
					"\u0264\5\u0088E\2\u0264\u0265\7\4\2\2\u0265\u0267\3\2\2\2\u0266\u0263"+
					"\3\2\2\2\u0266\u0267\3\2\2\2\u0267\u0268\3\2\2\2\u0268\u0269\5\u0098M"+
					"\2\u0269+\3\2\2\2\u026a\u026b\7A\2\2\u026b\u026e\7\u0091\2\2\u026c\u026d"+
					"\7R\2\2\u026d\u026f\7H\2\2\u026e\u026c\3\2\2\2\u026e\u026f\3\2\2\2\u026f"+
					"\u0273\3\2\2\2\u0270\u0271\5\u0088E\2\u0271\u0272\7\4\2\2\u0272\u0274"+
					"\3\2\2\2\u0273\u0270\3\2\2\2\u0273\u0274\3\2\2\2\u0274\u0275\3\2\2\2\u0275"+
					"\u0276\5\u009aN\2\u0276-\3\2\2\2\u0277\u0279\7\u0095\2\2\u0278\u027a\7"+
					"v\2\2\u0279\u0278\3\2\2\2\u0279\u027a\3\2\2\2\u027a\u027b\3\2\2\2\u027b"+
					"\u0280\5b\62\2\u027c\u027d\7\7\2\2\u027d\u027f\5b\62\2\u027e\u027c\3\2"+
					"\2\2\u027f\u0282\3\2\2\2\u0280\u027e\3\2\2\2\u0280\u0281\3\2\2\2\u0281"+
					"\u0284\3\2\2\2\u0282\u0280\3\2\2\2\u0283\u0277\3\2\2\2\u0283\u0284\3\2"+
					"\2\2\u0284\u0285\3\2\2\2\u0285\u028b\5n8\2\u0286\u0287\5r:\2\u0287\u0288"+
					"\5n8\2\u0288\u028a\3\2\2\2\u0289\u0286\3\2\2\2\u028a\u028d\3\2\2\2\u028b"+
					"\u0289\3\2\2\2\u028b\u028c\3\2\2\2\u028c\u0298\3\2\2\2\u028d\u028b\3\2"+
					"\2\2\u028e\u028f\7o\2\2\u028f\u0290\7*\2\2\u0290\u0295\5^\60\2\u0291\u0292"+
					"\7\7\2\2\u0292\u0294\5^\60\2\u0293\u0291\3\2\2\2\u0294\u0297\3\2\2\2\u0295"+
					"\u0293\3\2\2\2\u0295\u0296\3\2\2\2\u0296\u0299\3\2\2\2\u0297\u0295\3\2"+
					"\2\2\u0298\u028e\3\2\2\2\u0298\u0299\3\2\2\2\u0299\u02a0\3\2\2\2\u029a"+
					"\u029b\7d\2\2\u029b\u029e\5P)\2\u029c\u029d\t\4\2\2\u029d\u029f\5P)\2"+
					"\u029e\u029c\3\2\2\2\u029e\u029f\3\2\2\2\u029f\u02a1\3\2\2\2\u02a0\u029a"+
					"\3\2\2\2\u02a0\u02a1\3\2\2\2\u02a1/\3\2\2\2\u02a2\u02a4\5Z.\2\u02a3\u02a2"+
					"\3\2\2\2\u02a3\u02a4\3\2\2\2\u02a4\u02b6\3\2\2\2\u02a5\u02b7\7Z\2\2\u02a6"+
					"\u02b7\7|\2\2\u02a7\u02a8\7Z\2\2\u02a8\u02a9\7n\2\2\u02a9\u02b7\7|\2\2"+
					"\u02aa\u02ab\7Z\2\2\u02ab\u02ac\7n\2\2\u02ac\u02b7\7\177\2\2\u02ad\u02ae"+
					"\7Z\2\2\u02ae\u02af\7n\2\2\u02af\u02b7\7\33\2\2\u02b0\u02b1\7Z\2\2\u02b1"+
					"\u02b2\7n\2\2\u02b2\u02b7\7J\2\2\u02b3\u02b4\7Z\2\2\u02b4\u02b5\7n\2\2"+
					"\u02b5\u02b7\7S\2\2\u02b6\u02a5\3\2\2\2\u02b6\u02a6\3\2\2\2\u02b6\u02a7"+
					"\3\2\2\2\u02b6\u02aa\3\2\2\2\u02b6\u02ad\3\2\2\2\u02b6\u02b0\3\2\2\2\u02b6"+
					"\u02b3\3\2\2\2\u02b7\u02b8\3\2\2\2\u02b8\u02bc\7]\2\2\u02b9\u02ba\5\u0088"+
					"E\2\u02ba\u02bb\7\4\2\2\u02bb\u02bd\3\2\2\2\u02bc\u02b9\3\2\2\2\u02bc"+
					"\u02bd\3\2\2\2\u02bd\u02be\3\2\2\2\u02be\u02ca\5\u008aF\2\u02bf\u02c0"+
					"\7\5\2\2\u02c0\u02c5\5\u0090I\2\u02c1\u02c2\7\7\2\2\u02c2\u02c4\5\u0090"+
					"I\2\u02c3\u02c1\3\2\2\2\u02c4\u02c7\3\2\2\2\u02c5\u02c3\3\2\2\2\u02c5"+
					"\u02c6\3\2\2\2\u02c6\u02c8\3\2\2\2\u02c7\u02c5\3\2\2\2\u02c8\u02c9\7\6"+
					"\2\2\u02c9\u02cb\3\2\2\2\u02ca\u02bf\3\2\2\2\u02ca\u02cb\3\2\2\2\u02cb"+
					"\u02eb\3\2\2\2\u02cc\u02cd\7\u0090\2\2\u02cd\u02ce\7\5\2\2\u02ce\u02d3"+
					"\5P)\2\u02cf\u02d0\7\7\2\2\u02d0\u02d2\5P)\2\u02d1\u02cf\3\2\2\2\u02d2"+
					"\u02d5\3\2\2\2\u02d3\u02d1\3\2\2\2\u02d3\u02d4\3\2\2\2\u02d4\u02d6\3\2"+
					"\2\2\u02d5\u02d3\3\2\2\2\u02d6\u02e5\7\6\2\2\u02d7\u02d8\7\7\2\2\u02d8"+
					"\u02d9\7\5\2\2\u02d9\u02de\5P)\2\u02da\u02db\7\7\2\2\u02db\u02dd\5P)\2"+
					"\u02dc\u02da\3\2\2\2\u02dd\u02e0\3\2\2\2\u02de\u02dc\3\2\2\2\u02de\u02df"+
					"\3\2\2\2\u02df\u02e1\3\2\2\2\u02e0\u02de\3\2\2\2\u02e1\u02e2\7\6\2\2\u02e2"+
					"\u02e4\3\2\2\2\u02e3\u02d7\3\2\2\2\u02e4\u02e7\3\2\2\2\u02e5\u02e3\3\2"+
					"\2\2\u02e5\u02e6\3\2\2\2\u02e6\u02ec\3\2\2\2\u02e7\u02e5\3\2\2\2\u02e8"+
					"\u02ec\5> \2\u02e9\u02ea\7:\2\2\u02ea\u02ec\7\u0090\2\2\u02eb\u02cc\3"+
					"\2\2\2\u02eb\u02e8\3\2\2\2\u02eb\u02e9\3\2\2\2\u02ec\61\3\2\2\2\u02ed"+
					"\u02f1\7r\2\2\u02ee\u02ef\5\u0088E\2\u02ef\u02f0\7\4\2\2\u02f0\u02f2\3"+
					"\2\2\2\u02f1\u02ee\3\2\2\2\u02f1\u02f2\3\2\2\2\u02f2\u02f3\3\2\2\2\u02f3"+
					"\u02fa\5\u009eP\2\u02f4\u02f5\7\b\2\2\u02f5\u02fb\5`\61\2\u02f6\u02f7"+
					"\7\5\2\2\u02f7\u02f8\5`\61\2\u02f8\u02f9\7\6\2\2\u02f9\u02fb\3\2\2\2\u02fa"+
					"\u02f4\3\2\2\2\u02fa\u02f6\3\2\2\2\u02fa\u02fb\3\2\2\2\u02fb\63\3\2\2"+
					"\2\u02fc\u0307\7y\2\2\u02fd\u0308\5\u0092J\2\u02fe\u02ff\5\u0088E\2\u02ff"+
					"\u0300\7\4\2\2\u0300\u0302\3\2\2\2\u0301\u02fe\3\2\2\2\u0301\u0302\3\2"+
					"\2\2\u0302\u0305\3\2\2\2\u0303\u0306\5\u008aF\2\u0304\u0306\5\u0096L\2"+
					"\u0305\u0303\3\2\2\2\u0305\u0304\3\2\2\2\u0306\u0308\3\2\2\2\u0307\u02fd"+
					"\3\2\2\2\u0307\u0301\3\2\2\2\u0307\u0308\3\2\2\2\u0308\65\3\2\2\2\u0309"+
					"\u030b\7z\2\2\u030a\u030c\7\u0081\2\2\u030b\u030a\3\2\2\2\u030b\u030c"+
					"\3\2\2\2\u030c\u030d\3\2\2\2\u030d\u030e\5\u00a0Q\2\u030e\67\3\2\2\2\u030f"+
					"\u0314\7\177\2\2\u0310\u0312\7\u0089\2\2\u0311\u0313\5\u00a4S\2\u0312"+
					"\u0311\3\2\2\2\u0312\u0313\3\2\2\2\u0313\u0315\3\2\2\2\u0314\u0310\3\2"+
					"\2\2\u0314\u0315\3\2\2\2\u0315\u031b\3\2\2\2\u0316\u0318\7\u0088\2\2\u0317"+
					"\u0319\7\u0081\2\2\u0318\u0317\3\2\2\2\u0318\u0319\3\2\2\2\u0319\u031a"+
					"\3\2\2\2\u031a\u031c\5\u00a0Q\2\u031b\u0316\3\2\2\2\u031b\u031c\3\2\2"+
					"\2\u031c9\3\2\2\2\u031d\u031e\7\u0081\2\2\u031e\u031f\5\u00a0Q\2\u031f"+
					";\3\2\2\2\u0320\u0322\7\u0095\2\2\u0321\u0323\7v\2\2\u0322\u0321\3\2\2"+
					"\2\u0322\u0323\3\2\2\2\u0323\u0324\3\2\2\2\u0324\u0329\5b\62\2\u0325\u0326"+
					"\7\7\2\2\u0326\u0328\5b\62\2\u0327\u0325\3\2\2\2\u0328\u032b\3\2\2\2\u0329"+
					"\u0327\3\2\2\2\u0329\u032a\3\2\2\2\u032a\u032d\3\2\2\2\u032b\u0329\3\2"+
					"\2\2\u032c\u0320\3\2\2\2\u032c\u032d\3\2\2\2\u032d\u032e\3\2\2\2\u032e"+
					"\u0339\5n8\2\u032f\u0330\7o\2\2\u0330\u0331\7*\2\2\u0331\u0336\5^\60\2"+
					"\u0332\u0333\7\7\2\2\u0333\u0335\5^\60\2\u0334\u0332\3\2\2\2\u0335\u0338"+
					"\3\2\2\2\u0336\u0334\3\2\2\2\u0336\u0337\3\2\2\2\u0337\u033a\3\2\2\2\u0338"+
					"\u0336\3\2\2\2\u0339\u032f\3\2\2\2\u0339\u033a\3\2\2\2\u033a\u0341\3\2"+
					"\2\2\u033b\u033c\7d\2\2\u033c\u033f\5P)\2\u033d\u033e\t\4\2\2\u033e\u0340"+
					"\5P)\2\u033f\u033d\3\2\2\2\u033f\u0340\3\2\2\2\u0340\u0342\3\2\2\2\u0341"+
					"\u033b\3\2\2\2\u0341\u0342\3\2\2\2\u0342=\3\2\2\2\u0343\u0345\7\u0095"+
					"\2\2\u0344\u0346\7v\2\2\u0345\u0344\3\2\2\2\u0345\u0346\3\2\2\2\u0346"+
					"\u0347\3\2\2\2\u0347\u034c\5b\62\2\u0348\u0349\7\7\2\2\u0349\u034b\5b"+
					"\62\2\u034a\u0348\3\2\2\2\u034b\u034e\3\2\2\2\u034c\u034a\3\2\2\2\u034c"+
					"\u034d\3\2\2\2\u034d\u0350\3\2\2\2\u034e\u034c\3\2\2\2\u034f\u0343\3\2"+
					"\2\2\u034f\u0350\3\2\2\2\u0350\u0351\3\2\2\2\u0351\u0357\5@!\2\u0352\u0353"+
					"\5r:\2\u0353\u0354\5@!\2\u0354\u0356\3\2\2\2\u0355\u0352\3\2\2\2\u0356"+
					"\u0359\3\2\2\2\u0357\u0355\3\2\2\2\u0357\u0358\3\2\2\2\u0358\u0364\3\2"+
					"\2\2\u0359\u0357\3\2\2\2\u035a\u035b\7o\2\2\u035b\u035c\7*\2\2\u035c\u0361"+
					"\5^\60\2\u035d\u035e\7\7\2\2\u035e\u0360\5^\60\2\u035f\u035d\3\2\2\2\u0360"+
					"\u0363\3\2\2\2\u0361\u035f\3\2\2\2\u0361\u0362\3\2\2\2\u0362\u0365\3\2"+
					"\2\2\u0363\u0361\3\2\2\2\u0364\u035a\3\2\2\2\u0364\u0365\3\2\2\2\u0365"+
					"\u036c\3\2\2\2\u0366\u0367\7d\2\2\u0367\u036a\5P)\2\u0368\u0369\t\4\2"+
					"\2\u0369\u036b\5P)\2\u036a\u0368\3\2\2\2\u036a\u036b\3\2\2\2\u036b\u036d"+
					"\3\2\2\2\u036c\u0366\3\2\2\2\u036c\u036d\3\2\2\2\u036d?\3\2\2\2\u036e"+
					"\u0370\7\u0082\2\2\u036f\u0371\t\6\2\2\u0370\u036f\3\2\2\2\u0370\u0371"+
					"\3\2\2\2\u0371\u0372\3\2\2\2\u0372\u0377\5d\63\2\u0373\u0374\7\7\2\2\u0374"+
					"\u0376\5d\63\2\u0375\u0373\3\2\2\2\u0376\u0379\3\2\2\2\u0377\u0375\3\2"+
					"\2\2\u0377\u0378\3\2\2\2\u0378\u0386\3\2\2\2\u0379\u0377\3\2\2\2\u037a"+
					"\u0384\7M\2\2\u037b\u0380\5f\64\2\u037c\u037d\7\7\2\2\u037d\u037f\5f\64"+
					"\2\u037e\u037c\3\2\2\2\u037f\u0382\3\2\2\2\u0380\u037e\3\2\2\2\u0380\u0381"+
					"\3\2\2\2\u0381\u0385\3\2\2\2\u0382\u0380\3\2\2\2\u0383\u0385\5h\65\2\u0384"+
					"\u037b\3\2\2\2\u0384\u0383\3\2\2\2\u0385\u0387\3\2\2\2\u0386\u037a\3\2"+
					"\2\2\u0386\u0387\3\2\2\2\u0387\u038a\3\2\2\2\u0388\u0389\7\u0094\2\2\u0389"+
					"\u038b\5P)\2\u038a\u0388\3\2\2\2\u038a\u038b\3\2\2\2\u038b\u038f\3\2\2"+
					"\2\u038c\u038d\7P\2\2\u038d\u038e\7*\2\2\u038e\u0390\5p9\2\u038f\u038c"+
					"\3\2\2\2\u038f\u0390\3\2\2\2\u0390\u03ae\3\2\2\2\u0391\u0392\7\u0090\2"+
					"\2\u0392\u0393\7\5\2\2\u0393\u0398\5P)\2\u0394\u0395\7\7\2\2\u0395\u0397"+
					"\5P)\2\u0396\u0394\3\2\2\2\u0397\u039a\3\2\2\2\u0398\u0396\3\2\2\2\u0398"+
					"\u0399\3\2\2\2\u0399\u039b\3\2\2\2\u039a\u0398\3\2\2\2\u039b\u03aa\7\6"+
					"\2\2\u039c\u039d\7\7\2\2\u039d\u039e\7\5\2\2\u039e\u03a3\5P)\2\u039f\u03a0"+
					"\7\7\2\2\u03a0\u03a2\5P)\2\u03a1\u039f\3\2\2\2\u03a2\u03a5\3\2\2\2\u03a3"+
					"\u03a1\3\2\2\2\u03a3\u03a4\3\2\2\2\u03a4\u03a6\3\2\2\2\u03a5\u03a3\3\2"+
					"\2\2\u03a6\u03a7\7\6\2\2\u03a7\u03a9\3\2\2\2\u03a8\u039c\3\2\2\2\u03a9"+
					"\u03ac\3\2\2\2\u03aa\u03a8\3\2\2\2\u03aa\u03ab\3\2\2\2\u03ab\u03ae\3\2"+
					"\2\2\u03ac\u03aa\3\2\2\2\u03ad\u036e\3\2\2\2\u03ad\u0391\3\2\2\2\u03ae"+
					"A\3\2\2\2\u03af\u03b1\5Z.\2\u03b0\u03af\3\2\2\2\u03b0\u03b1\3\2\2\2\u03b1"+
					"\u03b2\3\2\2\2\u03b2\u03bd\7\u008d\2\2\u03b3\u03b4\7n\2\2\u03b4\u03be"+
					"\7\177\2\2\u03b5\u03b6\7n\2\2\u03b6\u03be\7\33\2\2\u03b7\u03b8\7n\2\2"+
					"\u03b8\u03be\7|\2\2\u03b9\u03ba\7n\2\2\u03ba\u03be\7J\2\2\u03bb\u03bc"+
					"\7n\2\2\u03bc\u03be\7S\2\2\u03bd\u03b3\3\2\2\2\u03bd\u03b5\3\2\2\2\u03bd"+
					"\u03b7\3\2\2\2\u03bd\u03b9\3\2\2\2\u03bd\u03bb\3\2\2\2\u03bd\u03be\3\2"+
					"\2\2\u03be\u03bf\3\2\2\2\u03bf\u03c0\5\\/\2\u03c0\u03c1\7\u0083\2\2\u03c1"+
					"\u03c2\5\u0090I\2\u03c2\u03c3\7\b\2\2\u03c3\u03cb\5P)\2\u03c4\u03c5\7"+
					"\7\2\2\u03c5\u03c6\5\u0090I\2\u03c6\u03c7\7\b\2\2\u03c7\u03c8\5P)\2\u03c8"+
					"\u03ca\3\2\2\2\u03c9\u03c4\3\2\2\2\u03ca\u03cd\3\2\2\2\u03cb\u03c9\3\2"+
					"\2\2\u03cb\u03cc\3\2\2\2\u03cc\u03d0\3\2\2\2\u03cd\u03cb\3\2\2\2\u03ce"+
					"\u03cf\7\u0094\2\2\u03cf\u03d1\5P)\2\u03d0\u03ce\3\2\2\2\u03d0\u03d1\3"+
					"\2\2\2\u03d1C\3\2\2\2\u03d2\u03d4\5Z.\2\u03d3\u03d2\3\2\2\2\u03d3\u03d4"+
					"\3\2\2\2\u03d4\u03d5\3\2\2\2\u03d5\u03e0\7\u008d\2\2\u03d6\u03d7\7n\2"+
					"\2\u03d7\u03e1\7\177\2\2\u03d8\u03d9\7n\2\2\u03d9\u03e1\7\33\2\2\u03da"+
					"\u03db\7n\2\2\u03db\u03e1\7|\2\2\u03dc\u03dd\7n\2\2\u03dd\u03e1\7J\2\2"+
					"\u03de\u03df\7n\2\2\u03df\u03e1\7S\2\2\u03e0\u03d6\3\2\2\2\u03e0\u03d8"+
					"\3\2\2\2\u03e0\u03da\3\2\2\2\u03e0\u03dc\3\2\2\2\u03e0\u03de\3\2\2\2\u03e0"+
					"\u03e1\3\2\2\2\u03e1\u03e2\3\2\2\2\u03e2\u03e3\5\\/\2\u03e3\u03e4\7\u0083"+
					"\2\2\u03e4\u03e5\5\u0090I\2\u03e5\u03e6\7\b\2\2\u03e6\u03ee\5P)\2\u03e7"+
					"\u03e8\7\7\2\2\u03e8\u03e9\5\u0090I\2\u03e9\u03ea\7\b\2\2\u03ea\u03eb"+
					"\5P)\2\u03eb\u03ed\3\2\2\2\u03ec\u03e7\3\2\2\2\u03ed\u03f0\3\2\2\2\u03ee"+
					"\u03ec\3\2\2\2\u03ee\u03ef\3\2\2\2\u03ef\u03f3\3\2\2\2\u03f0\u03ee\3\2"+
					"\2\2\u03f1\u03f2\7\u0094\2\2\u03f2\u03f4\5P)\2\u03f3\u03f1\3\2\2\2\u03f3"+
					"\u03f4\3\2\2\2\u03f4\u0407\3\2\2\2\u03f5\u03f6\7o\2\2\u03f6\u03f7\7*\2"+
					"\2\u03f7\u03fc\5^\60\2\u03f8\u03f9\7\7\2\2\u03f9\u03fb\5^\60\2\u03fa\u03f8"+
					"\3\2\2\2\u03fb\u03fe\3\2\2\2\u03fc\u03fa\3\2\2\2\u03fc\u03fd\3\2\2\2\u03fd"+
					"\u0400\3\2\2\2\u03fe\u03fc\3\2\2\2\u03ff\u03f5\3\2\2\2\u03ff\u0400\3\2"+
					"\2\2\u0400\u0401\3\2\2\2\u0401\u0402\7d\2\2\u0402\u0405\5P)\2\u0403\u0404"+
					"\t\4\2\2\u0404\u0406\5P)\2\u0405\u0403\3\2\2\2\u0405\u0406\3\2\2\2\u0406"+
					"\u0408\3\2\2\2\u0407\u03ff\3\2\2\2\u0407\u0408\3\2\2\2\u0408E\3\2\2\2"+
					"\u0409\u040a\7\u008f\2\2\u040aG\3\2\2\2\u040b\u040d\5\u0090I\2\u040c\u040e"+
					"\5J&\2\u040d\u040c\3\2\2\2\u040d\u040e\3\2\2\2\u040e\u0412\3\2\2\2\u040f"+
					"\u0411\5L\'\2\u0410\u040f\3\2\2\2\u0411\u0414\3\2\2\2\u0412\u0410\3\2"+
					"\2\2\u0412\u0413\3\2\2\2\u0413I\3\2\2\2\u0414\u0412\3\2\2\2\u0415\u0417"+
					"\5\u0084C\2\u0416\u0415\3\2\2\2\u0417\u0418\3\2\2\2\u0418\u0416\3\2\2"+
					"\2\u0418\u0419\3\2\2\2\u0419\u0424\3\2\2\2\u041a\u041b\7\5\2\2\u041b\u041c"+
					"\5v<\2\u041c\u041d\7\6\2\2\u041d\u0425\3\2\2\2\u041e\u041f\7\5\2\2\u041f"+
					"\u0420\5v<\2\u0420\u0421\7\7\2\2\u0421\u0422\5v<\2\u0422\u0423\7\6\2\2"+
					"\u0423\u0425\3\2\2\2\u0424\u041a\3\2\2\2\u0424\u041e\3\2\2\2\u0424\u0425"+
					"\3\2\2\2\u0425K\3\2\2\2\u0426\u0427\7\63\2\2\u0427\u0429\5\u0084C\2\u0428"+
					"\u0426\3\2\2\2\u0428\u0429\3\2\2\2\u0429\u044b\3\2\2\2\u042a\u042b\7s"+
					"\2\2\u042b\u042d\7a\2\2\u042c\u042e\t\7\2\2\u042d\u042c\3\2\2\2\u042d"+
					"\u042e\3\2\2\2\u042e\u042f\3\2\2\2\u042f\u0431\5N(\2\u0430\u0432\7&\2"+
					"\2\u0431\u0430\3\2\2\2\u0431\u0432\3\2\2\2\u0432\u044c\3\2\2\2\u0433\u0435"+
					"\7h\2\2\u0434\u0433\3\2\2\2\u0434\u0435\3\2\2\2\u0435\u0436\3\2\2\2\u0436"+
					"\u0437\7j\2\2\u0437\u044c\5N(\2\u0438\u0439\7\u008c\2\2\u0439\u044c\5"+
					"N(\2\u043a\u043b\7.\2\2\u043b\u043c\7\5\2\2\u043c\u043d\5P)\2\u043d\u043e"+
					"\7\6\2\2\u043e\u044c\3\2\2\2\u043f\u0446\7:\2\2\u0440\u0447\5v<\2\u0441"+
					"\u0447\5x=\2\u0442\u0443\7\5\2\2\u0443\u0444\5P)\2\u0444\u0445\7\6\2\2"+
					"\u0445\u0447\3\2\2\2\u0446\u0440\3\2\2\2\u0446\u0441\3\2\2\2\u0446\u0442"+
					"\3\2\2\2\u0447\u044c\3\2\2\2\u0448\u0449\7/\2\2\u0449\u044c\5\u0092J\2"+
					"\u044a\u044c\5R*\2\u044b\u042a\3\2\2\2\u044b\u0434\3\2\2\2\u044b\u0438"+
					"\3\2\2\2\u044b\u043a\3\2\2\2\u044b\u043f\3\2\2\2\u044b\u0448\3\2\2\2\u044b"+
					"\u044a\3\2\2\2\u044cM\3\2\2\2\u044d\u044e\7m\2\2\u044e\u044f\7\62\2\2"+
					"\u044f\u0451\t\b\2\2\u0450\u044d\3\2\2\2\u0450\u0451\3\2\2\2\u0451O\3"+
					"\2\2\2\u0452\u0453\b)\1\2\u0453\u049f\5x=\2\u0454\u049f\7\u0099\2\2\u0455"+
					"\u0456\5\u0088E\2\u0456\u0457\7\4\2\2\u0457\u0459\3\2\2\2\u0458\u0455"+
					"\3\2\2\2\u0458\u0459\3\2\2\2\u0459\u045a\3\2\2\2\u045a\u045b\5\u008aF"+
					"\2\u045b\u045c\7\4\2\2\u045c\u045e\3\2\2\2\u045d\u0458\3\2\2\2\u045d\u045e"+
					"\3\2\2\2\u045e\u045f\3\2\2\2\u045f\u049f\5\u0090I\2\u0460\u0461\5z>\2"+
					"\u0461\u0462\5P)\27\u0462\u049f\3\2\2\2\u0463\u0464\5\u0086D\2\u0464\u0471"+
					"\7\5\2\2\u0465\u0467\7@\2\2\u0466\u0465\3\2\2\2\u0466\u0467\3\2\2\2\u0467"+
					"\u0468\3\2\2\2\u0468\u046d\5P)\2\u0469\u046a\7\7\2\2\u046a\u046c\5P)\2"+
					"\u046b\u0469\3\2\2\2\u046c\u046f\3\2\2\2\u046d\u046b\3\2\2\2\u046d\u046e"+
					"\3\2\2\2\u046e\u0472\3\2\2\2\u046f\u046d\3\2\2\2\u0470\u0472\7\t\2\2\u0471"+
					"\u0466\3\2\2\2\u0471\u0470\3\2\2\2\u0471\u0472\3\2\2\2\u0472\u0473\3\2"+
					"\2\2\u0473\u0474\7\6\2\2\u0474\u049f\3\2\2\2\u0475\u0476\7\5\2\2\u0476"+
					"\u0477\5P)\2\u0477\u0478\7\6\2\2\u0478\u049f\3\2\2\2\u0479\u047a\7-\2"+
					"\2\u047a\u047b\7\5\2\2\u047b\u047c\5P)\2\u047c\u047d\7#\2\2\u047d\u047e"+
					"\5J&\2\u047e\u047f\7\6\2\2\u047f\u049f\3\2\2\2\u0480\u0482\7h\2\2\u0481"+
					"\u0480\3\2\2\2\u0481\u0482\3\2\2\2\u0482\u0483\3\2\2\2\u0483\u0485\7H"+
					"\2\2\u0484\u0481\3\2\2\2\u0484\u0485\3\2\2\2\u0485\u0486\3\2\2\2\u0486"+
					"\u0487\7\5\2\2\u0487\u0488\5> \2\u0488\u0489\7\6\2\2\u0489\u049f\3\2\2"+
					"\2\u048a\u048c\7,\2\2\u048b\u048d\5P)\2\u048c\u048b\3\2\2\2\u048c\u048d"+
					"\3\2\2\2\u048d\u0493\3\2\2\2\u048e\u048f\7\u0093\2\2\u048f\u0490\5P)\2"+
					"\u0490\u0491\7\u0087\2\2\u0491\u0492\5P)\2\u0492\u0494\3\2\2\2\u0493\u048e"+
					"\3\2\2\2\u0494\u0495\3\2\2\2\u0495\u0493\3\2\2\2\u0495\u0496\3\2\2\2\u0496"+
					"\u0499\3\2\2\2\u0497\u0498\7C\2\2\u0498\u049a\5P)\2\u0499\u0497\3\2\2"+
					"\2\u0499\u049a\3\2\2\2\u049a\u049b\3\2\2\2\u049b\u049c\7D\2\2\u049c\u049f"+
					"\3\2\2\2\u049d\u049f\5T+\2\u049e\u0452\3\2\2\2\u049e\u0454\3\2\2\2\u049e"+
					"\u045d\3\2\2\2\u049e\u0460\3\2\2\2\u049e\u0463\3\2\2\2\u049e\u0475\3\2"+
					"\2\2\u049e\u0479\3\2\2\2\u049e\u0484\3\2\2\2\u049e\u048a\3\2\2\2\u049e"+
					"\u049d\3\2\2\2\u049f\u0504\3\2\2\2\u04a0\u04a1\f\26\2\2\u04a1\u04a2\7"+
					"\r\2\2\u04a2\u0503\5P)\27\u04a3\u04a4\f\25\2\2\u04a4\u04a5\t\t\2\2\u04a5"+
					"\u0503\5P)\26\u04a6\u04a7\f\24\2\2\u04a7\u04a8\t\n\2\2\u04a8\u0503\5P"+
					")\25\u04a9\u04aa\f\23\2\2\u04aa\u04ab\t\13\2\2\u04ab\u0503\5P)\24\u04ac"+
					"\u04ad\f\22\2\2\u04ad\u04ae\t\f\2\2\u04ae\u0503\5P)\23\u04af\u04bc\f\21"+
					"\2\2\u04b0\u04bd\7\b\2\2\u04b1\u04bd\7\30\2\2\u04b2\u04bd\7\31\2\2\u04b3"+
					"\u04bd\7\32\2\2\u04b4\u04bd\7^\2\2\u04b5\u04b6\7^\2\2\u04b6\u04bd\7h\2"+
					"\2\u04b7\u04bd\7U\2\2\u04b8\u04bd\7c\2\2\u04b9\u04bd\7O\2\2\u04ba\u04bd"+
					"\7e\2\2\u04bb\u04bd\7x\2\2\u04bc\u04b0\3\2\2\2\u04bc\u04b1\3\2\2\2\u04bc"+
					"\u04b2\3\2\2\2\u04bc\u04b3\3\2\2\2\u04bc\u04b4\3\2\2\2\u04bc\u04b5\3\2"+
					"\2\2\u04bc\u04b7\3\2\2\2\u04bc\u04b8\3\2\2\2\u04bc\u04b9\3\2\2\2\u04bc"+
					"\u04ba\3\2\2\2\u04bc\u04bb\3\2\2\2\u04bd\u04be\3\2\2\2\u04be\u0503\5P"+
					")\22\u04bf\u04c0\f\20\2\2\u04c0\u04c1\7\"\2\2\u04c1\u0503\5P)\21\u04c2"+
					"\u04c3\f\17\2\2\u04c3\u04c4\7n\2\2\u04c4\u0503\5P)\20\u04c5\u04c6\f\b"+
					"\2\2\u04c6\u04c8\7^\2\2\u04c7\u04c9\7h\2\2\u04c8\u04c7\3\2\2\2\u04c8\u04c9"+
					"\3\2\2\2\u04c9\u04ca\3\2\2\2\u04ca\u0503\5P)\t\u04cb\u04cd\f\7\2\2\u04cc"+
					"\u04ce\7h\2\2\u04cd\u04cc\3\2\2\2\u04cd\u04ce\3\2\2\2\u04ce\u04cf\3\2"+
					"\2\2\u04cf\u04d0\7)\2\2\u04d0\u04d1\5P)\2\u04d1\u04d2\7\"\2\2\u04d2\u04d3"+
					"\5P)\b\u04d3\u0503\3\2\2\2\u04d4\u04d5\f\13\2\2\u04d5\u04d6\7/\2\2\u04d6"+
					"\u0503\5\u0092J\2\u04d7\u04d9\f\n\2\2\u04d8\u04da\7h\2\2\u04d9\u04d8\3"+
					"\2\2\2\u04d9\u04da\3\2\2\2\u04da\u04db\3\2\2\2\u04db\u04dc\t\r\2\2\u04dc"+
					"\u04df\5P)\2\u04dd\u04de\7E\2\2\u04de\u04e0\5P)\2\u04df\u04dd\3\2\2\2"+
					"\u04df\u04e0\3\2\2\2\u04e0\u0503\3\2\2\2\u04e1\u04e6\f\t\2\2\u04e2\u04e7"+
					"\7_\2\2\u04e3\u04e7\7i\2\2\u04e4\u04e5\7h\2\2\u04e5\u04e7\7j\2\2\u04e6"+
					"\u04e2\3\2\2\2\u04e6\u04e3\3\2\2\2\u04e6\u04e4\3\2\2\2\u04e7\u0503\3\2"+
					"\2\2\u04e8\u04ea\f\6\2\2\u04e9\u04eb\7h\2\2\u04ea\u04e9\3\2\2\2\u04ea"+
					"\u04eb\3\2\2\2\u04eb\u04ec\3\2\2\2\u04ec\u0500\7U\2\2\u04ed\u04f7\7\5"+
					"\2\2\u04ee\u04f8\5> \2\u04ef\u04f4\5P)\2\u04f0\u04f1\7\7\2\2\u04f1\u04f3"+
					"\5P)\2\u04f2\u04f0\3\2\2\2\u04f3\u04f6\3\2\2\2\u04f4\u04f2\3\2\2\2\u04f4"+
					"\u04f5\3\2\2\2\u04f5\u04f8\3\2\2\2\u04f6\u04f4\3\2\2\2\u04f7\u04ee\3\2"+
					"\2\2\u04f7\u04ef\3\2\2\2\u04f7\u04f8\3\2\2\2\u04f8\u04f9\3\2\2\2\u04f9"+
					"\u0501\7\6\2\2\u04fa\u04fb\5\u0088E\2\u04fb\u04fc\7\4\2\2\u04fc\u04fe"+
					"\3\2\2\2\u04fd\u04fa\3\2\2\2\u04fd\u04fe\3\2\2\2\u04fe\u04ff\3\2\2\2\u04ff"+
					"\u0501\5\u008aF\2\u0500\u04ed\3\2\2\2\u0500\u04fd\3\2\2\2\u0501\u0503"+
					"\3\2\2\2\u0502\u04a0\3\2\2\2\u0502\u04a3\3\2\2\2\u0502\u04a6\3\2\2\2\u0502"+
					"\u04a9\3\2\2\2\u0502\u04ac\3\2\2\2\u0502\u04af\3\2\2\2\u0502\u04bf\3\2"+
					"\2\2\u0502\u04c2\3\2\2\2\u0502\u04c5\3\2\2\2\u0502\u04cb\3\2\2\2\u0502"+
					"\u04d4\3\2\2\2\u0502\u04d7\3\2\2\2\u0502\u04e1\3\2\2\2\u0502\u04e8\3\2"+
					"\2\2\u0503\u0506\3\2\2\2\u0504\u0502\3\2\2\2\u0504\u0505\3\2\2\2\u0505"+
					"Q\3\2\2\2\u0506\u0504\3\2\2\2\u0507\u0508\7w\2\2\u0508\u0514\5\u0094K"+
					"\2\u0509\u050a\7\5\2\2\u050a\u050f\5\u0090I\2\u050b\u050c\7\7\2\2\u050c"+
					"\u050e\5\u0090I\2\u050d\u050b\3\2\2\2\u050e\u0511\3\2\2\2\u050f\u050d"+
					"\3\2\2\2\u050f\u0510\3\2\2\2\u0510\u0512\3\2\2\2\u0511\u050f\3\2\2\2\u0512"+
					"\u0513\7\6\2\2\u0513\u0515\3\2\2\2\u0514\u0509\3\2\2\2\u0514\u0515\3\2"+
					"\2\2\u0515\u0528\3\2\2\2\u0516\u0517\7m\2\2\u0517\u0520\t\16\2\2\u0518"+
					"\u0519\7\u0083\2\2\u0519\u0521\7j\2\2\u051a\u051b\7\u0083\2\2\u051b\u0521"+
					"\7:\2\2\u051c\u0521\7+\2\2\u051d\u0521\7}\2\2\u051e\u051f\7g\2\2\u051f"+
					"\u0521\7\34\2\2\u0520\u0518\3\2\2\2\u0520\u051a\3\2\2\2\u0520\u051c\3"+
					"\2\2\2\u0520\u051d\3\2\2\2\u0520\u051e\3\2\2\2\u0521\u0525\3\2\2\2\u0522"+
					"\u0523\7e\2\2\u0523\u0525\5\u0084C\2\u0524\u0516\3\2\2\2\u0524\u0522\3"+
					"\2\2\2\u0525\u0527\3\2\2\2\u0526\u0524\3\2\2\2\u0527\u052a\3\2\2\2\u0528"+
					"\u0526\3\2\2\2\u0528\u0529\3\2\2\2\u0529\u0535\3\2\2\2\u052a\u0528\3\2"+
					"\2\2\u052b\u052d\7h\2\2\u052c\u052b\3\2\2\2\u052c\u052d\3\2\2\2\u052d"+
					"\u052e\3\2\2\2\u052e\u0533\7;\2\2\u052f\u0530\7X\2\2\u0530\u0534\7<\2"+
					"\2\u0531\u0532\7X\2\2\u0532\u0534\7T\2\2\u0533\u052f\3\2\2\2\u0533\u0531"+
					"\3\2\2\2\u0533\u0534\3\2\2\2\u0534\u0536\3\2\2\2\u0535\u052c\3\2\2\2\u0535"+
					"\u0536\3\2\2\2\u0536S\3\2\2\2\u0537\u0538\7u\2\2\u0538\u053d\7\5\2\2\u0539"+
					"\u053e\7S\2\2\u053a\u053b\t\17\2\2\u053b\u053c\7\7\2\2\u053c\u053e\5|"+
					"?\2\u053d\u0539\3\2\2\2\u053d\u053a\3\2\2\2\u053e\u053f\3\2\2\2\u053f"+
					"\u0540\7\6\2\2\u0540U\3\2\2\2\u0541\u0544\5\u0090I\2\u0542\u0543\7/\2"+
					"\2\u0543\u0545\5\u0092J\2\u0544\u0542\3\2\2\2\u0544\u0545\3\2\2\2\u0545"+
					"\u0547\3\2\2\2\u0546\u0548\t\7\2\2\u0547\u0546\3\2\2\2\u0547\u0548\3\2"+
					"\2\2\u0548W\3\2\2\2\u0549\u054a\7\63\2\2\u054a\u054c\5\u0084C\2\u054b"+
					"\u0549\3\2\2\2\u054b\u054c\3\2\2\2\u054c\u0571\3\2\2\2\u054d\u054e\7s"+
					"\2\2\u054e\u0551\7a\2\2\u054f\u0551\7\u008c\2\2\u0550\u054d\3\2\2\2\u0550"+
					"\u054f\3\2\2\2\u0551\u0552\3\2\2\2\u0552\u0553\7\5\2\2\u0553\u0558\5V"+
					",\2\u0554\u0555\7\7\2\2\u0555\u0557\5V,\2\u0556\u0554\3\2\2\2\u0557\u055a"+
					"\3\2\2\2\u0558\u0556\3\2\2\2\u0558\u0559\3\2\2\2\u0559\u055b\3\2\2\2\u055a"+
					"\u0558\3\2\2\2\u055b\u055c\7\6\2\2\u055c\u055d\5N(\2\u055d\u0572\3\2\2"+
					"\2\u055e\u055f\7.\2\2\u055f\u0560\7\5\2\2\u0560\u0561\5P)\2\u0561\u0562"+
					"\7\6\2\2\u0562\u0572\3\2\2\2\u0563\u0564\7L\2\2\u0564\u0565\7a\2\2\u0565"+
					"\u0566\7\5\2\2\u0566\u056b\5\u0090I\2\u0567\u0568\7\7\2\2\u0568\u056a"+
					"\5\u0090I\2\u0569\u0567\3\2\2\2\u056a\u056d\3\2\2\2\u056b\u0569\3\2\2"+
					"\2\u056b\u056c\3\2\2\2\u056c\u056e\3\2\2\2\u056d\u056b\3\2\2\2\u056e\u056f"+
					"\7\6\2\2\u056f\u0570\5R*\2\u0570\u0572\3\2\2\2\u0571\u0550\3\2\2\2\u0571"+
					"\u055e\3\2\2\2\u0571\u0563\3\2\2\2\u0572Y\3\2\2\2\u0573\u0575\7\u0095"+
					"\2\2\u0574\u0576\7v\2\2\u0575\u0574\3\2\2\2\u0575\u0576\3\2\2\2\u0576"+
					"\u0577\3\2\2\2\u0577\u0578\5t;\2\u0578\u0579\7#\2\2\u0579\u057a\7\5\2"+
					"\2\u057a\u057b\5> \2\u057b\u0585\7\6\2\2\u057c\u057d\7\7\2\2\u057d\u057e"+
					"\5t;\2\u057e\u057f\7#\2\2\u057f\u0580\7\5\2\2\u0580\u0581\5> \2\u0581"+
					"\u0582\7\6\2\2\u0582\u0584\3\2\2\2\u0583\u057c\3\2\2\2\u0584\u0587\3\2"+
					"\2\2\u0585\u0583\3\2\2\2\u0585\u0586\3\2\2\2\u0586[\3\2\2\2\u0587\u0585"+
					"\3\2\2\2\u0588\u0589\5\u0088E\2\u0589\u058a\7\4\2\2\u058a\u058c\3\2\2"+
					"\2\u058b\u0588\3\2\2\2\u058b\u058c\3\2\2\2\u058c\u058d\3\2\2\2\u058d\u0593"+
					"\5\u008aF\2\u058e\u058f\7W\2\2\u058f\u0590\7*\2\2\u0590\u0594\5\u0096"+
					"L\2\u0591\u0592\7h\2\2\u0592\u0594\7W\2\2\u0593\u058e\3\2\2\2\u0593\u0591"+
					"\3\2\2\2\u0593\u0594\3\2\2\2\u0594]\3\2\2\2\u0595\u0598\5P)\2\u0596\u0597"+
					"\7/\2\2\u0597\u0599\5\u0092J\2\u0598\u0596\3\2\2\2\u0598\u0599\3\2\2\2"+
					"\u0599\u059b\3\2\2\2\u059a\u059c\t\7\2\2\u059b\u059a\3\2\2\2\u059b\u059c"+
					"\3\2\2\2\u059c_\3\2\2\2\u059d\u05a1\5v<\2\u059e\u05a1\5\u0084C\2\u059f"+
					"\u05a1\7\u009a\2\2\u05a0\u059d\3\2\2\2\u05a0\u059e\3\2\2\2\u05a0\u059f"+
					"\3\2\2\2\u05a1a\3\2\2\2\u05a2\u05ae\5\u008aF\2\u05a3\u05a4\7\5\2\2\u05a4"+
					"\u05a9\5\u0090I\2\u05a5\u05a6\7\7\2\2\u05a6\u05a8\5\u0090I\2\u05a7\u05a5"+
					"\3\2\2\2\u05a8\u05ab\3\2\2\2\u05a9\u05a7\3\2\2\2\u05a9\u05aa\3\2\2\2\u05aa"+
					"\u05ac\3\2\2\2\u05ab\u05a9\3\2\2\2\u05ac\u05ad\7\6\2\2\u05ad\u05af\3\2"+
					"\2\2\u05ae\u05a3\3\2\2\2\u05ae\u05af\3\2\2\2\u05af\u05b0\3\2\2\2\u05b0"+
					"\u05b1\7#\2\2\u05b1\u05b2\7\5\2\2\u05b2\u05b3\5> \2\u05b3\u05b4\7\6\2"+
					"\2\u05b4c\3\2\2\2\u05b5\u05c2\7\t\2\2\u05b6\u05b7\5\u008aF\2\u05b7\u05b8"+
					"\7\4\2\2\u05b8\u05b9\7\t\2\2\u05b9\u05c2\3\2\2\2\u05ba\u05bf\5P)\2\u05bb"+
					"\u05bd\7#\2\2\u05bc\u05bb\3\2\2\2\u05bc\u05bd\3\2\2\2\u05bd\u05be\3\2"+
					"\2\2\u05be\u05c0\5\u0080A\2\u05bf\u05bc\3\2\2\2\u05bf\u05c0\3\2\2\2\u05c0"+
					"\u05c2\3\2\2\2\u05c1\u05b5\3\2\2\2\u05c1\u05b6\3\2\2\2\u05c1\u05ba\3\2"+
					"\2\2\u05c2e\3\2\2\2\u05c3\u05c4\5\u0088E\2\u05c4\u05c5\7\4\2\2\u05c5\u05c7"+
					"\3\2\2\2\u05c6\u05c3\3\2\2\2\u05c6\u05c7\3\2\2\2\u05c7\u05c8\3\2\2\2\u05c8"+
					"\u05cd\5\u008aF\2\u05c9\u05cb\7#\2\2\u05ca\u05c9\3\2\2\2\u05ca\u05cb\3"+
					"\2\2\2\u05cb\u05cc\3\2\2\2\u05cc\u05ce\5\u00a2R\2\u05cd\u05ca\3\2\2\2"+
					"\u05cd\u05ce\3\2\2\2\u05ce\u05d4\3\2\2\2\u05cf\u05d0\7W\2\2\u05d0\u05d1"+
					"\7*\2\2\u05d1\u05d5\5\u0096L\2\u05d2\u05d3\7h\2\2\u05d3\u05d5\7W\2\2\u05d4"+
					"\u05cf\3\2\2\2\u05d4\u05d2\3\2\2\2\u05d4\u05d5\3\2\2\2\u05d5\u05f3\3\2"+
					"\2\2\u05d6\u05e0\7\5\2\2\u05d7\u05dc\5f\64\2\u05d8\u05d9\7\7\2\2\u05d9"+
					"\u05db\5f\64\2\u05da\u05d8\3\2\2\2\u05db\u05de\3\2\2\2\u05dc\u05da\3\2"+
					"\2\2\u05dc\u05dd\3\2\2\2\u05dd\u05e1\3\2\2\2\u05de\u05dc\3\2\2\2\u05df"+
					"\u05e1\5h\65\2\u05e0\u05d7\3\2\2\2\u05e0\u05df\3\2\2\2\u05e1\u05e2\3\2"+
					"\2\2\u05e2\u05e7\7\6\2\2\u05e3\u05e5\7#\2\2\u05e4\u05e3\3\2\2\2\u05e4"+
					"\u05e5\3\2\2\2\u05e5\u05e6\3\2\2\2\u05e6\u05e8\5\u00a2R\2\u05e7\u05e4"+
					"\3\2\2\2\u05e7\u05e8\3\2\2\2\u05e8\u05f3\3\2\2\2\u05e9\u05ea\7\5\2\2\u05ea"+
					"\u05eb\5> \2\u05eb\u05f0\7\6\2\2\u05ec\u05ee\7#\2\2\u05ed\u05ec\3\2\2"+
					"\2\u05ed\u05ee\3\2\2\2\u05ee\u05ef\3\2\2\2\u05ef\u05f1\5\u00a2R\2\u05f0"+
					"\u05ed\3\2\2\2\u05f0\u05f1\3\2\2\2\u05f1\u05f3\3\2\2\2\u05f2\u05c6\3\2"+
					"\2\2\u05f2\u05d6\3\2\2\2\u05f2\u05e9\3\2\2\2\u05f3g\3\2\2\2\u05f4\u05fb"+
					"\5f\64\2\u05f5\u05f6\5j\66\2\u05f6\u05f7\5f\64\2\u05f7\u05f8\5l\67\2\u05f8"+
					"\u05fa\3\2\2\2\u05f9\u05f5\3\2\2\2\u05fa\u05fd\3\2\2\2\u05fb\u05f9\3\2"+
					"\2\2\u05fb\u05fc\3\2\2\2\u05fci\3\2\2\2\u05fd\u05fb\3\2\2\2\u05fe\u060c"+
					"\7\7\2\2\u05ff\u0601\7f\2\2\u0600\u05ff\3\2\2\2\u0600\u0601\3\2\2\2\u0601"+
					"\u0608\3\2\2\2\u0602\u0604\7b\2\2\u0603\u0605\7p\2\2\u0604\u0603\3\2\2"+
					"\2\u0604\u0605\3\2\2\2\u0605\u0609\3\2\2\2\u0606\u0609\7Y\2\2\u0607\u0609"+
					"\7\65\2\2\u0608\u0602\3\2\2\2\u0608\u0606\3\2\2\2\u0608\u0607\3\2\2\2"+
					"\u0608\u0609\3\2\2\2\u0609\u060a\3\2\2\2\u060a\u060c\7`\2\2\u060b\u05fe"+
					"\3\2\2\2\u060b\u0600\3\2\2\2\u060ck\3\2\2\2\u060d\u060e\7m\2\2\u060e\u061c"+
					"\5P)\2\u060f\u0610\7\u008e\2\2\u0610\u0611\7\5\2\2\u0611\u0616\5\u0090"+
					"I\2\u0612\u0613\7\7\2\2\u0613\u0615\5\u0090I\2\u0614\u0612\3\2\2\2\u0615"+
					"\u0618\3\2\2\2\u0616\u0614\3\2\2\2\u0616\u0617\3\2\2\2\u0617\u0619\3\2"+
					"\2\2\u0618\u0616\3\2\2\2\u0619\u061a\7\6\2\2\u061a\u061c\3\2\2\2\u061b"+
					"\u060d\3\2\2\2\u061b\u060f\3\2\2\2\u061b\u061c\3\2\2\2\u061cm\3\2\2\2"+
					"\u061d\u061f\7\u0082\2\2\u061e\u0620\t\6\2\2\u061f\u061e\3\2\2\2\u061f"+
					"\u0620\3\2\2\2\u0620\u0621\3\2\2\2\u0621\u0626\5d\63\2\u0622\u0623\7\7"+
					"\2\2\u0623\u0625\5d\63\2\u0624\u0622\3\2\2\2\u0625\u0628\3\2\2\2\u0626"+
					"\u0624\3\2\2\2\u0626\u0627\3\2\2\2\u0627\u0635\3\2\2\2\u0628\u0626\3\2"+
					"\2\2\u0629\u0633\7M\2\2\u062a\u062f\5f\64\2\u062b\u062c\7\7\2\2\u062c"+
					"\u062e\5f\64\2\u062d\u062b\3\2\2\2\u062e\u0631\3\2\2\2\u062f\u062d\3\2"+
					"\2\2\u062f\u0630\3\2\2\2\u0630\u0634\3\2\2\2\u0631\u062f\3\2\2\2\u0632"+
					"\u0634\5h\65\2\u0633\u062a\3\2\2\2\u0633\u0632\3\2\2\2\u0634\u0636\3\2"+
					"\2\2\u0635\u0629\3\2\2\2\u0635\u0636\3\2\2\2\u0636\u0639\3\2\2\2\u0637"+
					"\u0638\7\u0094\2\2\u0638\u063a\5P)\2\u0639\u0637\3\2\2\2\u0639\u063a\3"+
					"\2\2\2\u063a\u063e\3\2\2\2\u063b\u063c\7P\2\2\u063c\u063d\7*\2\2\u063d"+
					"\u063f\5p9\2\u063e\u063b\3\2\2\2\u063e\u063f\3\2\2\2\u063f\u065d\3\2\2"+
					"\2\u0640\u0641\7\u0090\2\2\u0641\u0642\7\5\2\2\u0642\u0647\5P)\2\u0643"+
					"\u0644\7\7\2\2\u0644\u0646\5P)\2\u0645\u0643\3\2\2\2\u0646\u0649\3\2\2"+
					"\2\u0647\u0645\3\2\2\2\u0647\u0648\3\2\2\2\u0648\u064a\3\2\2\2\u0649\u0647"+
					"\3\2\2\2\u064a\u0659\7\6\2\2\u064b\u064c\7\7\2\2\u064c\u064d\7\5\2\2\u064d"+
					"\u0652\5P)\2\u064e\u064f\7\7\2\2\u064f\u0651\5P)\2\u0650\u064e\3\2\2\2"+
					"\u0651\u0654\3\2\2\2\u0652\u0650\3\2\2\2\u0652\u0653\3\2\2\2\u0653\u0655"+
					"\3\2\2\2\u0654\u0652\3\2\2\2\u0655\u0656\7\6\2\2\u0656\u0658\3\2\2\2\u0657"+
					"\u064b\3\2\2\2\u0658\u065b\3\2\2\2\u0659\u0657\3\2\2\2\u0659\u065a\3\2"+
					"\2\2\u065a\u065d\3\2\2\2\u065b\u0659\3\2\2\2\u065c\u061d\3\2\2\2\u065c"+
					"\u0640\3\2\2\2\u065do\3\2\2\2\u065e\u065f\5\u0090I\2\u065f\u0660\7\7\2"+
					"\2\u0660\u0661\5\u0090I\2\u0661\u0664\3\2\2\2\u0662\u0664\5\u0090I\2\u0663"+
					"\u065e\3\2\2\2\u0663\u0662\3\2\2\2\u0664q\3\2\2\2\u0665\u066b\7\u008b"+
					"\2\2\u0666\u0667\7\u008b\2\2\u0667\u066b\7\37\2\2\u0668\u066b\7\\\2\2"+
					"\u0669\u066b\7F\2\2\u066a\u0665\3\2\2\2\u066a\u0666\3\2\2\2\u066a\u0668"+
					"\3\2\2\2\u066a\u0669\3\2\2\2\u066bs\3\2\2\2\u066c\u0678\5\u008aF\2\u066d"+
					"\u066e\7\5\2\2\u066e\u0673\5\u0090I\2\u066f\u0670\7\7\2\2\u0670\u0672"+
					"\5\u0090I\2\u0671\u066f\3\2\2\2\u0672\u0675\3\2\2\2\u0673\u0671\3\2\2"+
					"\2\u0673\u0674\3\2\2\2\u0674\u0676\3\2\2\2\u0675\u0673\3\2\2\2\u0676\u0677"+
					"\7\6\2\2\u0677\u0679\3\2\2\2\u0678\u066d\3\2\2\2\u0678\u0679\3\2\2\2\u0679"+
					"u\3\2\2\2\u067a\u067c\t\n\2\2\u067b\u067a\3\2\2\2\u067b\u067c\3\2\2\2"+
					"\u067c\u067d\3\2\2\2\u067d\u067e\7\u0098\2\2\u067ew\3\2\2\2\u067f\u0680"+
					"\t\20\2\2\u0680y\3\2\2\2\u0681\u0682\t\21\2\2\u0682{\3\2\2\2\u0683\u0684"+
					"\7\u009a\2\2\u0684}\3\2\2\2\u0685\u0688\5P)\2\u0686\u0688\5H%\2\u0687"+
					"\u0685\3\2\2\2\u0687\u0686\3\2\2\2\u0688\177\3\2\2\2\u0689\u068a\t\22"+
					"\2\2\u068a\u0081\3\2\2\2\u068b\u068c\t\23\2\2\u068c\u0083\3\2\2\2\u068d"+
					"\u068e\5\u00a6T\2\u068e\u0085\3\2\2\2\u068f\u0690\5\u00a6T\2\u0690\u0087"+
					"\3\2\2\2\u0691\u0692\5\u00a6T\2\u0692\u0089\3\2\2\2\u0693\u0694\5\u00a6"+
					"T\2\u0694\u008b\3\2\2\2\u0695\u0696\5\u00a6T\2\u0696\u008d\3\2\2\2\u0697"+
					"\u0698\5\u00a6T\2\u0698\u008f\3\2\2\2\u0699\u069a\5\u00a6T\2\u069a\u0091"+
					"\3\2\2\2\u069b\u069c\5\u00a6T\2\u069c\u0093\3\2\2\2\u069d\u069e\5\u00a6"+
					"T\2\u069e\u0095\3\2\2\2\u069f\u06a0\5\u00a6T\2\u06a0\u0097\3\2\2\2\u06a1"+
					"\u06a2\5\u00a6T\2\u06a2\u0099\3\2\2\2\u06a3\u06a4\5\u00a6T\2\u06a4\u009b"+
					"\3\2\2\2\u06a5\u06a6\5\u00a6T\2\u06a6\u009d\3\2\2\2\u06a7\u06a8\5\u00a6"+
					"T\2\u06a8\u009f\3\2\2\2\u06a9\u06aa\5\u00a6T\2\u06aa\u00a1\3\2\2\2\u06ab"+
					"\u06ac\5\u00a6T\2\u06ac\u00a3\3\2\2\2\u06ad\u06ae\5\u00a6T\2\u06ae\u00a5"+
					"\3\2\2\2\u06af\u06b7\7\u0097\2\2\u06b0\u06b7\5\u0082B\2\u06b1\u06b7\7"+
					"\u009a\2\2\u06b2\u06b3\7\5\2\2\u06b3\u06b4\5\u00a6T\2\u06b4\u06b5\7\6"+
					"\2\2\u06b5\u06b7\3\2\2\2\u06b6\u06af\3\2\2\2\u06b6\u06b0\3\2\2\2\u06b6"+
					"\u06b1\3\2\2\2\u06b6\u06b2\3\2\2\2\u06b7\u00a7\3\2\2\2\u00f3\u00aa\u00ac"+
					"\u00b7\u00be\u00c3\u00c9\u00cf\u00d1\u00f1\u00f8\u0100\u0103\u010c\u0110"+
					"\u0118\u011c\u011e\u0123\u0125\u0129\u0130\u0133\u0138\u013c\u0141\u014a"+
					"\u014d\u0153\u0155\u0159\u015f\u0164\u016f\u0175\u0179\u017f\u0184\u018d"+
					"\u0194\u019a\u019e\u01a2\u01a8\u01ad\u01b4\u01bf\u01c2\u01c4\u01ca\u01d0"+
					"\u01d4\u01db\u01e1\u01e7\u01ed\u01f2\u01fe\u0203\u020e\u0213\u0216\u021d"+
					"\u0220\u0227\u0230\u0233\u0239\u023b\u023f\u0247\u024c\u0254\u0259\u0261"+
					"\u0266\u026e\u0273\u0279\u0280\u0283\u028b\u0295\u0298\u029e\u02a0\u02a3"+
					"\u02b6\u02bc\u02c5\u02ca\u02d3\u02de\u02e5\u02eb\u02f1\u02fa\u0301\u0305"+
					"\u0307\u030b\u0312\u0314\u0318\u031b\u0322\u0329\u032c\u0336\u0339\u033f"+
					"\u0341\u0345\u034c\u034f\u0357\u0361\u0364\u036a\u036c\u0370\u0377\u0380"+
					"\u0384\u0386\u038a\u038f\u0398\u03a3\u03aa\u03ad\u03b0\u03bd\u03cb\u03d0"+
					"\u03d3\u03e0\u03ee\u03f3\u03fc\u03ff\u0405\u0407\u040d\u0412\u0418\u0424"+
					"\u0428\u042d\u0431\u0434\u0446\u044b\u0450\u0458\u045d\u0466\u046d\u0471"+
					"\u0481\u0484\u048c\u0495\u0499\u049e\u04bc\u04c8\u04cd\u04d9\u04df\u04e6"+
					"\u04ea\u04f4\u04f7\u04fd\u0500\u0502\u0504\u050f\u0514\u0520\u0524\u0528"+
					"\u052c\u0533\u0535\u053d\u0544\u0547\u054b\u0550\u0558\u056b\u0571\u0575"+
					"\u0585\u058b\u0593\u0598\u059b\u05a0\u05a9\u05ae\u05bc\u05bf\u05c1\u05c6"+
					"\u05ca\u05cd\u05d4\u05dc\u05e0\u05e4\u05e7\u05ed\u05f0\u05f2\u05fb\u0600"+
					"\u0604\u0608\u060b\u0616\u061b\u061f\u0626\u062f\u0633\u0635\u0639\u063e"+
					"\u0647\u0652\u0659\u065c\u0663\u066a\u0673\u0678\u067b\u0687\u06b6";
	public static final ATN _ATN =
			new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}