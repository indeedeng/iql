/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.sqltoiql;

import com.indeed.iql2.sqltoiql.antlr.SQLiteBaseListener;
import com.indeed.iql2.sqltoiql.antlr.SQLiteListener;
import com.indeed.iql2.sqltoiql.antlr.SQLiteParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.log4j.Logger;


/**
 * This class provides an empty implementation of {@link SQLiteListener},
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
public class SQLiteBaseLogListener extends SQLiteBaseListener {

	private static final Logger LOG = Logger.getLogger(SQLiteBaseLogListener.class);

	@Override public void enterParse(SQLiteParser.ParseContext ctx) {
	    LOG.debug("Running listener: enterParse: "+ctx.getText());
	}

	@Override public void exitParse(SQLiteParser.ParseContext ctx) {
	    LOG.debug("Running listener: exitParse: "+ctx.getText());
	}

	@Override public void enterError(SQLiteParser.ErrorContext ctx) {
        LOG.debug("Running: listenenterError:er  "+ctx.getText());
	    throw new NotImplementedException("The enterError method is not supported yet");
	}

	@Override public void exitError(SQLiteParser.ErrorContext ctx) {
        LOG.debug("Running: listeexitError:ner  "+ctx.getText());
	    throw new NotImplementedException("The exitError method is not supported yet");
	}

	@Override public void enterSql_stmt_list(SQLiteParser.Sql_stmt_listContext ctx) {
	    LOG.debug("Running listener: enterSql_stmt_list: "+ctx.getText());
	}

	@Override public void enterGroup_by(SQLiteParser.Group_byContext ctx) {
		LOG.debug("Running listener: enterSql_stmt_list: "+ctx.getText());
	}

	@Override public void exitGroup_by(SQLiteParser.Group_byContext ctx) {
		LOG.debug("Running listener: enterSql_stmt_list: "+ctx.getText());
	}

	@Override public void exitSql_stmt_list(SQLiteParser.Sql_stmt_listContext ctx) {
	    LOG.debug("Running listener: exitSql_stmt_list: "+ctx.getText());
	}

	@Override public void enterSql_stmt(SQLiteParser.Sql_stmtContext ctx) {
	    LOG.debug("Running listener: enterSql_stmt: "+ctx.getText());
	}

	@Override public void exitSql_stmt(SQLiteParser.Sql_stmtContext ctx) {
	    LOG.debug("Running listener: exitSql_stmt: "+ctx.getText());
	}

	@Override public void enterAlter_table_stmt(SQLiteParser.Alter_table_stmtContext ctx) {
        LOG.debug("Running listener: enterAlter_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterAlter_table_stmt method is not supported yet");
	}

	@Override public void exitAlter_table_stmt(SQLiteParser.Alter_table_stmtContext ctx) {
        LOG.debug("Running listener: exitAlter_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitAlter_table_stmt method is not supported yet");
	}

	@Override public void enterAnalyze_stmt(SQLiteParser.Analyze_stmtContext ctx) {
        LOG.debug("Running listener: enterAnalyze_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterAnalyze_stmt method is not supported yet");
	}

	@Override public void exitAnalyze_stmt(SQLiteParser.Analyze_stmtContext ctx) {
        LOG.debug("Running listener: exitAnalyze_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitAnalyze_stmt method is not supported yet");
	}

	@Override public void enterAttach_stmt(SQLiteParser.Attach_stmtContext ctx) {
        LOG.debug("Running listener: enterAttach_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterAttach_stmt method is not supported yet");
	}

	@Override public void exitAttach_stmt(SQLiteParser.Attach_stmtContext ctx) {
        LOG.debug("Running listener: exitAttach_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitAttach_stmt method is not supported yet");
	}

	@Override public void enterBegin_stmt(SQLiteParser.Begin_stmtContext ctx) {
        LOG.debug("Running listener: enterBegin_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterBegin_stmt method is not supported yet");
	}

	@Override public void exitBegin_stmt(SQLiteParser.Begin_stmtContext ctx) {
        LOG.debug("Running listener: exitBegin_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitBegin_stmt method is not supported yet");
	}

	@Override public void enterCommit_stmt(SQLiteParser.Commit_stmtContext ctx) {
        LOG.debug("Running listener: enterCommit_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterCommit_stmt method is not supported yet");
	}

	@Override public void exitCommit_stmt(SQLiteParser.Commit_stmtContext ctx) {
        LOG.debug("Running listener: exitCommit_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitCommit_stmt method is not supported yet");
	}

	@Override public void enterCompound_select_stmt(SQLiteParser.Compound_select_stmtContext ctx) {
        LOG.debug("Running listener: enterCompound_select_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterCompound_select_stmt method is not supported yet");
	}

	@Override public void exitCompound_select_stmt(SQLiteParser.Compound_select_stmtContext ctx) {
        LOG.debug("Running listener: exitCompound_select_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitCompound_select_stmt method is not supported yet");
	}

	@Override public void enterCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {
        LOG.debug("Running listener: enterCreate_index_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterCreate_index_stmt method is not supported yet");
	}

	@Override public void exitCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {
        LOG.debug("Running listener: exitCreate_index_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitCreate_index_stmt method is not supported yet");
	}

	@Override public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        LOG.debug("Running listener: enterCreate_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterCreate_table_stmt method is not supported yet");
	}

	@Override public void exitCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        LOG.debug("Running listener: exitCreate_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitCreate_table_stmt method is not supported yet");
	}

	@Override public void enterCreate_trigger_stmt(SQLiteParser.Create_trigger_stmtContext ctx) {
        LOG.debug("Running listener: enterCreate_trigger_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterCreate_trigger_stmt method is not supported yet");
	}

	@Override public void exitCreate_trigger_stmt(SQLiteParser.Create_trigger_stmtContext ctx) {
        LOG.debug("Running listener: exitCreate_trigger_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitCreate_trigger_stmt method is not supported yet");
	}

	@Override public void enterCreate_view_stmt(SQLiteParser.Create_view_stmtContext ctx) {
        LOG.debug("Running listener: enterCreate_view_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterCreate_view_stmt method is not supported yet");
	}

	@Override public void exitCreate_view_stmt(SQLiteParser.Create_view_stmtContext ctx) {
        LOG.debug("Running listener: exitCreate_view_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitCreate_view_stmt method is not supported yet");
	}

	@Override public void enterCreate_virtual_table_stmt(SQLiteParser.Create_virtual_table_stmtContext ctx) {
        LOG.debug("Running listener: enterCreate_virtual_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterCreate_virtual_table_stmt method is not supported yet");
	}

	@Override public void exitCreate_virtual_table_stmt(SQLiteParser.Create_virtual_table_stmtContext ctx) {
        LOG.debug("Running listener: exitCreate_virtual_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitCreate_virtual_table_stmt method is not supported yet");
	}

	@Override public void enterDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
        LOG.debug("Running listener: enterDelete_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterDelete_stmt method is not supported yet");
	}

	@Override public void exitDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
        LOG.debug("Running listener: exitDelete_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitDelete_stmt method is not supported yet");
	}

	@Override public void enterDelete_stmt_limited(SQLiteParser.Delete_stmt_limitedContext ctx) {
        LOG.debug("Running listener: enterDelete_stmt_limited: "+ctx.getText());
	    throw new NotImplementedException("The enterDelete_stmt_limited method is not supported yet");
	}

	@Override public void exitDelete_stmt_limited(SQLiteParser.Delete_stmt_limitedContext ctx) {
        LOG.debug("Running listener: exitDelete_stmt_limited: "+ctx.getText());
	    throw new NotImplementedException("The exitDelete_stmt_limited method is not supported yet");
	}

	@Override public void enterDetach_stmt(SQLiteParser.Detach_stmtContext ctx) {
        LOG.debug("Running listener: enterDetach_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterDetach_stmt method is not supported yet");
	}

	@Override public void exitDetach_stmt(SQLiteParser.Detach_stmtContext ctx) {
        LOG.debug("Running listener: exitDetach_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitDetach_stmt method is not supported yet");
	}

	@Override public void enterDrop_index_stmt(SQLiteParser.Drop_index_stmtContext ctx) {
        LOG.debug("Running listener: enterDrop_index_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterDrop_index_stmt method is not supported yet");
	}

	@Override public void exitDrop_index_stmt(SQLiteParser.Drop_index_stmtContext ctx) {
        LOG.debug("Running listener: exitDrop_index_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitDrop_index_stmt method is not supported yet");
	}

	@Override public void enterDrop_table_stmt(SQLiteParser.Drop_table_stmtContext ctx) {
        LOG.debug("Running listener: enterDrop_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterDrop_table_stmt method is not supported yet");
	}

	@Override public void exitDrop_table_stmt(SQLiteParser.Drop_table_stmtContext ctx) {
        LOG.debug("Running listener: exitDrop_table_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitDrop_table_stmt method is not supported yet");
	}

	@Override public void enterDrop_trigger_stmt(SQLiteParser.Drop_trigger_stmtContext ctx) {
        LOG.debug("Running listener: enterDrop_trigger_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterDrop_trigger_stmt method is not supported yet");
	}

	@Override public void exitDrop_trigger_stmt(SQLiteParser.Drop_trigger_stmtContext ctx) {
        LOG.debug("Running listener: exitDrop_trigger_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitDrop_trigger_stmt method is not supported yet");
	}

	@Override public void enterDrop_view_stmt(SQLiteParser.Drop_view_stmtContext ctx) {
        LOG.debug("Running listener: enterDrop_view_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterDrop_view_stmt method is not supported yet");
	}

	@Override public void exitDrop_view_stmt(SQLiteParser.Drop_view_stmtContext ctx) {
        LOG.debug("Running listener: exitDrop_view_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitDrop_view_stmt method is not supported yet");
	}

	@Override public void enterFactored_select_stmt(SQLiteParser.Factored_select_stmtContext ctx) {
	    LOG.debug("Running listener: enterFactored_select_stmt: "+ctx.getText());
	}

	@Override public void exitFactored_select_stmt(SQLiteParser.Factored_select_stmtContext ctx) {
	    LOG.debug("Running listener: exitFactored_select_stmt: "+ctx.getText());
	}

	@Override public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {
        LOG.debug("Running listener: enterInsert_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterInsert_stmt method is not supported yet");
	}

	@Override public void exitInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {
        LOG.debug("Running listener: exitInsert_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitInsert_stmt method is not supported yet");
	}

	@Override public void enterPragma_stmt(SQLiteParser.Pragma_stmtContext ctx) {
        LOG.debug("Running listener: enterPragma_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterPragma_stmt method is not supported yet");
	}

	@Override public void exitPragma_stmt(SQLiteParser.Pragma_stmtContext ctx) {
        LOG.debug("Running listener: exitPragma_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitPragma_stmt method is not supported yet");
	}

	@Override public void enterReindex_stmt(SQLiteParser.Reindex_stmtContext ctx) {
        LOG.debug("Running listener: enterReindex_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterReindex_stmt method is not supported yet");
	}

	@Override public void exitReindex_stmt(SQLiteParser.Reindex_stmtContext ctx) {
        LOG.debug("Running listener: exitReindex_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitReindex_stmt method is not supported yet");
	}

	@Override public void enterRelease_stmt(SQLiteParser.Release_stmtContext ctx) {
        LOG.debug("Running listener: enterRelease_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterRelease_stmt method is not supported yet");
	}

	@Override public void exitRelease_stmt(SQLiteParser.Release_stmtContext ctx) {
        LOG.debug("Running listener: exitRelease_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitRelease_stmt method is not supported yet");
	}

	@Override public void enterRollback_stmt(SQLiteParser.Rollback_stmtContext ctx) {
        LOG.debug("Running listener: enterRollback_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterRollback_stmt method is not supported yet");
	}

	@Override public void exitRollback_stmt(SQLiteParser.Rollback_stmtContext ctx) {
        LOG.debug("Running listener: exitRollback_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitRollback_stmt method is not supported yet");
	}

	@Override public void enterSavepoint_stmt(SQLiteParser.Savepoint_stmtContext ctx) {
        LOG.debug("Running listener: enterSavepoint_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterSavepoint_stmt method is not supported yet");
	}

	@Override public void exitSavepoint_stmt(SQLiteParser.Savepoint_stmtContext ctx) {
        LOG.debug("Running listener: exitSavepoint_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitSavepoint_stmt method is not supported yet");
	}

	@Override public void enterSimple_select_stmt(SQLiteParser.Simple_select_stmtContext ctx) {
        LOG.debug("Running listener: enterSimple_select_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterSimple_select_stmt method is not supported yet");
	}

	@Override public void exitSimple_select_stmt(SQLiteParser.Simple_select_stmtContext ctx) {
        LOG.debug("Running listener: exitSimple_select_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitSimple_select_stmt method is not supported yet");
	}

	@Override public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
	    LOG.debug("Running listener: enterSelect_stmt: "+ctx.getText());
	}

	@Override public void exitSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
	    LOG.debug("Running listener: exitSelect_stmt: "+ctx.getText());
	}

	@Override public void enterSelect_or_values(SQLiteParser.Select_or_valuesContext ctx) {
        LOG.debug("Running listener: enterSelect_or_values: "+ctx.getText());
	    throw new NotImplementedException("The enterSelect_or_values method is not supported yet");
	}

	@Override public void exitSelect_or_values(SQLiteParser.Select_or_valuesContext ctx) {
        LOG.debug("Running listener: exitSelect_or_values: "+ctx.getText());
	    throw new NotImplementedException("The exitSelect_or_values method is not supported yet");
	}

	@Override public void enterUpdate_stmt(SQLiteParser.Update_stmtContext ctx) {
        LOG.debug("Running listener: enterUpdate_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterUpdate_stmt method is not supported yet");
	}

	@Override public void exitUpdate_stmt(SQLiteParser.Update_stmtContext ctx) {
        LOG.debug("Running listener: exitUpdate_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitUpdate_stmt method is not supported yet");
	}

	@Override public void enterUpdate_stmt_limited(SQLiteParser.Update_stmt_limitedContext ctx) {
        LOG.debug("Running listener: enterUpdate_stmt_limited: "+ctx.getText());
	    throw new NotImplementedException("The enterUpdate_stmt_limited method is not supported yet");
	}

	@Override public void exitUpdate_stmt_limited(SQLiteParser.Update_stmt_limitedContext ctx) {
        LOG.debug("Running listener: exitUpdate_stmt_limited: "+ctx.getText());
	    throw new NotImplementedException("The exitUpdate_stmt_limited method is not supported yet");
	}

	@Override public void enterVacuum_stmt(SQLiteParser.Vacuum_stmtContext ctx) {
        LOG.debug("Running listener: enterVacuum_stmt: "+ctx.getText());
	    throw new NotImplementedException("The enterVacuum_stmt method is not supported yet");
	}

	@Override public void exitVacuum_stmt(SQLiteParser.Vacuum_stmtContext ctx) {
        LOG.debug("Running listener: exitVacuum_stmt: "+ctx.getText());
	    throw new NotImplementedException("The exitVacuum_stmt method is not supported yet");
	}

	@Override public void enterColumn_def(SQLiteParser.Column_defContext ctx) {
        LOG.debug("Running listener: enterColumn_def: "+ctx.getText());
	    throw new NotImplementedException("The enterColumn_def method is not supported yet");
	}

	@Override public void exitColumn_def(SQLiteParser.Column_defContext ctx) {
        LOG.debug("Running listener: exitColumn_def: "+ctx.getText());
	    throw new NotImplementedException("The exitColumn_def method is not supported yet");
	}

	@Override public void enterType_name(SQLiteParser.Type_nameContext ctx) {
        LOG.debug("Running listener: enterType_name: "+ctx.getText());
	    throw new NotImplementedException("The enterType_name method is not supported yet");
	}

	@Override public void exitType_name(SQLiteParser.Type_nameContext ctx) {
        LOG.debug("Running listener: exitType_name: "+ctx.getText());
	    throw new NotImplementedException("The exitType_name method is not supported yet");
	}

	@Override public void enterColumn_constraint(SQLiteParser.Column_constraintContext ctx) {
        LOG.debug("Running listener: enterColumn_constraint: "+ctx.getText());
	    throw new NotImplementedException("The enterColumn_constraint method is not supported yet");
	}

	@Override public void exitColumn_constraint(SQLiteParser.Column_constraintContext ctx) {
        LOG.debug("Running listener: exitColumn_constraint: "+ctx.getText());
	    throw new NotImplementedException("The exitColumn_constraint method is not supported yet");
	}

	@Override public void enterConflict_clause(SQLiteParser.Conflict_clauseContext ctx) {
        LOG.debug("Running listener: enterConflict_clause: "+ctx.getText());
	    throw new NotImplementedException("The enterConflict_clause method is not supported yet");
	}

	@Override public void exitConflict_clause(SQLiteParser.Conflict_clauseContext ctx) {
        LOG.debug("Running listener: exitConflict_clause: "+ctx.getText());
	    throw new NotImplementedException("The exitConflict_clause method is not supported yet");
	}

	@Override public void enterExpr(SQLiteParser.ExprContext ctx) {
        LOG.debug("Running: listeenterExpr:ner  "+ctx.getText());
	}

	@Override public void exitExpr(SQLiteParser.ExprContext ctx) {
        LOG.debug("Running: listexitExpr:ener  "+ctx.getText());
	    throw new NotImplementedException("The exitExpr method is not supported yet");
	}

	@Override public void enterForeign_key_clause(SQLiteParser.Foreign_key_clauseContext ctx) {
        LOG.debug("Running listener: enterForeign_key_clause: "+ctx.getText());
	    throw new NotImplementedException("The enterForeign_key_clause method is not supported yet");
	}

	@Override public void exitForeign_key_clause(SQLiteParser.Foreign_key_clauseContext ctx) {
        LOG.debug("Running listener: exitForeign_key_clause: "+ctx.getText());
	    throw new NotImplementedException("The exitForeign_key_clause method is not supported yet");
	}

	@Override public void enterRaise_function(SQLiteParser.Raise_functionContext ctx) {
        LOG.debug("Running listener: enterRaise_function: "+ctx.getText());
	    throw new NotImplementedException("The enterRaise_function method is not supported yet");
	}

	@Override public void exitRaise_function(SQLiteParser.Raise_functionContext ctx) {
        LOG.debug("Running listener: exitRaise_function: "+ctx.getText());
	    throw new NotImplementedException("The exitRaise_function method is not supported yet");
	}

	@Override public void enterIndexed_column(SQLiteParser.Indexed_columnContext ctx) {
        LOG.debug("Running listener: enterIndexed_column: "+ctx.getText());
	    throw new NotImplementedException("The enterIndexed_column method is not supported yet");
	}

	@Override public void exitIndexed_column(SQLiteParser.Indexed_columnContext ctx) {
        LOG.debug("Running listener: exitIndexed_column: "+ctx.getText());
	    throw new NotImplementedException("The exitIndexed_column method is not supported yet");
	}

	@Override public void enterTable_constraint(SQLiteParser.Table_constraintContext ctx) {
        LOG.debug("Running listener: enterTable_constraint: "+ctx.getText());
	    throw new NotImplementedException("The enterTable_constraint method is not supported yet");
	}

	@Override public void exitTable_constraint(SQLiteParser.Table_constraintContext ctx) {
        LOG.debug("Running listener: exitTable_constraint: "+ctx.getText());
	    throw new NotImplementedException("The exitTable_constraint method is not supported yet");
	}

	@Override public void enterWith_clause(SQLiteParser.With_clauseContext ctx) {
        LOG.debug("Running listener: enterWith_clause: "+ctx.getText());
	    throw new NotImplementedException("The enterWith_clause method is not supported yet");
	}

	@Override public void exitWith_clause(SQLiteParser.With_clauseContext ctx) {
        LOG.debug("Running listener: exitWith_clause: "+ctx.getText());
	    throw new NotImplementedException("The exitWith_clause method is not supported yet");
	}

	@Override public void enterQualified_table_name(SQLiteParser.Qualified_table_nameContext ctx) {
        LOG.debug("Running listener: enterQualified_table_name: "+ctx.getText());
	    throw new NotImplementedException("The enterQualified_table_name method is not supported yet");
	}

	@Override public void exitQualified_table_name(SQLiteParser.Qualified_table_nameContext ctx) {
        LOG.debug("Running listener: exitQualified_table_name: "+ctx.getText());
	    throw new NotImplementedException("The exitQualified_table_name method is not supported yet");
	}

	@Override public void enterOrdering_term(SQLiteParser.Ordering_termContext ctx) {
        LOG.debug("Running listener: enterOrdering_term: "+ctx.getText());
	    throw new NotImplementedException("The enterOrdering_term method is not supported yet");
	}

	@Override public void exitOrdering_term(SQLiteParser.Ordering_termContext ctx) {
        LOG.debug("Running listener: exitOrdering_term: "+ctx.getText());
	    throw new NotImplementedException("The exitOrdering_term method is not supported yet");
	}

	@Override public void enterPragma_value(SQLiteParser.Pragma_valueContext ctx) {
        LOG.debug("Running listener: enterPragma_value: "+ctx.getText());
	    throw new NotImplementedException("The enterPragma_value method is not supported yet");
	}

	@Override public void exitPragma_value(SQLiteParser.Pragma_valueContext ctx) {
        LOG.debug("Running listener: exitPragma_value: "+ctx.getText());
	    throw new NotImplementedException("The exitPragma_value method is not supported yet");
	}

	@Override public void enterCommon_table_expression(SQLiteParser.Common_table_expressionContext ctx) {
        LOG.debug("Running listener: enterCommon_table_expression: "+ctx.getText());
	    throw new NotImplementedException("The enterCommon_table_expression method is not supported yet");
	}

	@Override public void exitCommon_table_expression(SQLiteParser.Common_table_expressionContext ctx) {
        LOG.debug("Running listener: exitCommon_table_expression: "+ctx.getText());
	    throw new NotImplementedException("The exitCommon_table_expression method is not supported yet");
	}

	@Override public void enterResult_column(SQLiteParser.Result_columnContext ctx) {
	    LOG.debug("Running listener: enterResult_column: "+ctx.getText());
	}

	@Override public void exitResult_column(SQLiteParser.Result_columnContext ctx) {
	    LOG.debug("Running listener: exitResult_column: "+ctx.getText());
	}

	@Override public void enterTable_or_subquery(SQLiteParser.Table_or_subqueryContext ctx) {
	    LOG.debug("Running listener: enterTable_or_subquery: "+ctx.getText());
	}

	@Override public void exitTable_or_subquery(SQLiteParser.Table_or_subqueryContext ctx) {
	    LOG.debug("Running listener: exitTable_or_subquery: "+ctx.getText());
	}

	@Override public void enterJoin_clause(SQLiteParser.Join_clauseContext ctx) {
        LOG.debug("Running listener: enterJoin_clause: "+ctx.getText());
	    throw new NotImplementedException("The enterJoin_clause method is not supported yet");
	}

	@Override public void exitJoin_clause(SQLiteParser.Join_clauseContext ctx) {
        LOG.debug("Running listener: exitJoin_clause: "+ctx.getText());
	    throw new NotImplementedException("The exitJoin_clause method is not supported yet");
	}

	@Override public void enterJoin_operator(SQLiteParser.Join_operatorContext ctx) {
        LOG.debug("Running listener: enterJoin_operator: "+ctx.getText());
	    throw new NotImplementedException("The enterJoin_operator method is not supported yet");
	}

	@Override public void exitJoin_operator(SQLiteParser.Join_operatorContext ctx) {
        LOG.debug("Running listener: exitJoin_operator: "+ctx.getText());
	    throw new NotImplementedException("The exitJoin_operator method is not supported yet");
	}

	@Override public void enterJoin_constraint(SQLiteParser.Join_constraintContext ctx) {
        LOG.debug("Running listener: enterJoin_constraint: "+ctx.getText());
	    throw new NotImplementedException("The enterJoin_constraint method is not supported yet");
	}

	@Override public void exitJoin_constraint(SQLiteParser.Join_constraintContext ctx) {
        LOG.debug("Running listener: exitJoin_constraint: "+ctx.getText());
	    throw new NotImplementedException("The exitJoin_constraint method is not supported yet");
	}

	@Override public void enterSelect_core(SQLiteParser.Select_coreContext ctx) {
	    LOG.debug("Running listener: enterSelect_core: "+ctx.getText());
	}

	@Override public void exitSelect_core(SQLiteParser.Select_coreContext ctx) {
	    LOG.debug("Running listener: exitSelect_core: "+ctx.getText());
	}

	@Override public void enterCompound_operator(SQLiteParser.Compound_operatorContext ctx) {
        LOG.debug("Running listener: enterCompound_operator: "+ctx.getText());
	    throw new NotImplementedException("The enterCompound_operator method is not supported yet");
	}

	@Override public void exitCompound_operator(SQLiteParser.Compound_operatorContext ctx) {
        LOG.debug("Running listener: exitCompound_operator: "+ctx.getText());
	    throw new NotImplementedException("The exitCompound_operator method is not supported yet");
	}

	@Override public void enterCte_table_name(SQLiteParser.Cte_table_nameContext ctx) {
        LOG.debug("Running listener: enterCte_table_name: "+ctx.getText());
	    throw new NotImplementedException("The enterCte_table_name method is not supported yet");
	}

	@Override public void exitCte_table_name(SQLiteParser.Cte_table_nameContext ctx) {
        LOG.debug("Running listener: exitCte_table_name: "+ctx.getText());
	    throw new NotImplementedException("The exitCte_table_name method is not supported yet");
	}

	@Override public void enterSigned_number(SQLiteParser.Signed_numberContext ctx) {
        LOG.debug("Running listener: enterSigned_number: "+ctx.getText());
	    throw new NotImplementedException("The enterSigned_number method is not supported yet");
	}

	@Override public void exitSigned_number(SQLiteParser.Signed_numberContext ctx) {
        LOG.debug("Running listener: exitSigned_number: "+ctx.getText());
	    throw new NotImplementedException("The exitSigned_number method is not supported yet");
	}

	@Override public void enterLiteral_value(SQLiteParser.Literal_valueContext ctx) {
	    LOG.debug("Running listener: enterLiteral_value: "+ctx.getText());
	}

	@Override public void exitLiteral_value(SQLiteParser.Literal_valueContext ctx) {
	    LOG.debug("Running listener: exitLiteral_value: "+ctx.getText());
	}

	@Override public void enterUnary_operator(SQLiteParser.Unary_operatorContext ctx) {
        LOG.debug("Running listener: enterUnary_operator: "+ctx.getText());
	    throw new NotImplementedException("The enterUnary_operator method is not supported yet");
	}

	@Override public void exitUnary_operator(SQLiteParser.Unary_operatorContext ctx) {
        LOG.debug("Running listener: exitUnary_operator: "+ctx.getText());
	    throw new NotImplementedException("The exitUnary_operator method is not supported yet");
	}

	@Override public void enterError_message(SQLiteParser.Error_messageContext ctx) {
        LOG.debug("Running listener: enterError_message: "+ctx.getText());
	    throw new NotImplementedException("The enterError_message method is not supported yet");
	}

	@Override public void exitError_message(SQLiteParser.Error_messageContext ctx) {
        LOG.debug("Running listener: exitError_message: "+ctx.getText());
	    throw new NotImplementedException("The exitError_message method is not supported yet");
	}

	@Override public void enterModule_argument(SQLiteParser.Module_argumentContext ctx) {
        LOG.debug("Running listener: enterModule_argument: "+ctx.getText());
	    throw new NotImplementedException("The enterModule_argument method is not supported yet");
	}

	@Override public void exitModule_argument(SQLiteParser.Module_argumentContext ctx) {
        LOG.debug("Running listener: exitModule_argument: "+ctx.getText());
	    throw new NotImplementedException("The exitModule_argument method is not supported yet");
	}

	@Override public void enterColumn_alias(SQLiteParser.Column_aliasContext ctx) {
        LOG.debug("Running listener: enterColumn_alias: "+ctx.getText());
	    throw new NotImplementedException("The enterColumn_alias method is not supported yet");
	}

	@Override public void exitColumn_alias(SQLiteParser.Column_aliasContext ctx) {
        LOG.debug("Running listener: exitColumn_alias: "+ctx.getText());
	    throw new NotImplementedException("The exitColumn_alias method is not supported yet");
	}

	@Override public void enterKeyword(SQLiteParser.KeywordContext ctx) {
        LOG.debug("Running: listenerenterKeyword:  "+ctx.getText());
	    throw new NotImplementedException("The enterKeyword method is not supported yet");
	}

	@Override public void exitKeyword(SQLiteParser.KeywordContext ctx) {
        LOG.debug("Running: listeneexitKeyword:r  "+ctx.getText());
	    throw new NotImplementedException("The exitKeyword method is not supported yet");
	}

	@Override public void enterName(SQLiteParser.NameContext ctx) {
        LOG.debug("Running: listeenterName:ner  "+ctx.getText());
	    throw new NotImplementedException("The enterName method is not supported yet");
	}

	@Override public void exitName(SQLiteParser.NameContext ctx) {
        LOG.debug("Running: listexitName:ener  "+ctx.getText());
	    throw new NotImplementedException("The exitName method is not supported yet");
	}

	@Override public void enterFunction_name(SQLiteParser.Function_nameContext ctx) {
	    LOG.debug("Running listener: enterFunction_name: "+ctx.getText());
	}

	@Override public void exitFunction_name(SQLiteParser.Function_nameContext ctx) {
	    LOG.debug("Running listener: exitFunction_name: "+ctx.getText());
	}

	@Override public void enterDatabase_name(SQLiteParser.Database_nameContext ctx) {
        LOG.debug("Running listener: enterDatabase_name: "+ctx.getText());
	    throw new NotImplementedException("The enterDatabase_name method is not supported yet");
	}

	@Override public void exitDatabase_name(SQLiteParser.Database_nameContext ctx) {
        LOG.debug("Running listener: exitDatabase_name: "+ctx.getText());
	    throw new NotImplementedException("The exitDatabase_name method is not supported yet");
	}

	@Override public void enterTable_name(SQLiteParser.Table_nameContext ctx) {
	    LOG.debug("Running listener: enterTable_name: "+ctx.getText());
	}

	@Override public void exitTable_name(SQLiteParser.Table_nameContext ctx) {
	    LOG.debug("Running listener: exitTable_name: "+ctx.getText());
	}

	@Override public void enterTable_or_index_name(SQLiteParser.Table_or_index_nameContext ctx) {
        LOG.debug("Running listener: enterTable_or_index_name: "+ctx.getText());
	    throw new NotImplementedException("The enterTable_or_index_name method is not supported yet");
	}

	@Override public void exitTable_or_index_name(SQLiteParser.Table_or_index_nameContext ctx) {
        LOG.debug("Running listener: exitTable_or_index_name: "+ctx.getText());
	    throw new NotImplementedException("The exitTable_or_index_name method is not supported yet");
	}

	@Override public void enterNew_table_name(SQLiteParser.New_table_nameContext ctx) {
        LOG.debug("Running listener: enterNew_table_name: "+ctx.getText());
	    throw new NotImplementedException("The enterNew_table_name method is not supported yet");
	}

	@Override public void exitNew_table_name(SQLiteParser.New_table_nameContext ctx) {
        LOG.debug("Running listener: exitNew_table_name: "+ctx.getText());
	    throw new NotImplementedException("The exitNew_table_name method is not supported yet");
	}

	@Override public void enterColumn_name(SQLiteParser.Column_nameContext ctx) {
	    LOG.debug("Running listener: enterColumn_name: "+ctx.getText());
	}

	@Override public void exitColumn_name(SQLiteParser.Column_nameContext ctx) {
	    LOG.debug("Running listener: exitColumn_name: "+ctx.getText());
	}

	@Override public void enterCollation_name(SQLiteParser.Collation_nameContext ctx) {
        LOG.debug("Running listener: enterCollation_name: "+ctx.getText());
	    throw new NotImplementedException("The enterCollation_name method is not supported yet");
	}

	@Override public void exitCollation_name(SQLiteParser.Collation_nameContext ctx) {
        LOG.debug("Running listener: exitCollation_name: "+ctx.getText());
	    throw new NotImplementedException("The exitCollation_name method is not supported yet");
	}

	@Override public void enterForeign_table(SQLiteParser.Foreign_tableContext ctx) {
        LOG.debug("Running listener: enterForeign_table: "+ctx.getText());
	    throw new NotImplementedException("The enterForeign_table method is not supported yet");
	}

	@Override public void exitForeign_table(SQLiteParser.Foreign_tableContext ctx) {
        LOG.debug("Running listener: exitForeign_table: "+ctx.getText());
	    throw new NotImplementedException("The exitForeign_table method is not supported yet");
	}

	@Override public void enterIndex_name(SQLiteParser.Index_nameContext ctx) {
        LOG.debug("Running listener: enterIndex_name: "+ctx.getText());
	    throw new NotImplementedException("The enterIndex_name method is not supported yet");
	}

	@Override public void exitIndex_name(SQLiteParser.Index_nameContext ctx) {
        LOG.debug("Running listener: exitIndex_name: "+ctx.getText());
	    throw new NotImplementedException("The exitIndex_name method is not supported yet");
	}

	@Override public void enterTrigger_name(SQLiteParser.Trigger_nameContext ctx) {
        LOG.debug("Running listener: enterTrigger_name: "+ctx.getText());
	    throw new NotImplementedException("The enterTrigger_name method is not supported yet");
	}

	@Override public void exitTrigger_name(SQLiteParser.Trigger_nameContext ctx) {
        LOG.debug("Running listener: exitTrigger_name: "+ctx.getText());
	    throw new NotImplementedException("The exitTrigger_name method is not supported yet");
	}

	@Override public void enterView_name(SQLiteParser.View_nameContext ctx) {
        LOG.debug("Running listener: enterView_name: "+ctx.getText());
	    throw new NotImplementedException("The enterView_name method is not supported yet");
	}

	@Override public void exitView_name(SQLiteParser.View_nameContext ctx) {
        LOG.debug("Running listener: exitView_name: "+ctx.getText());
	    throw new NotImplementedException("The exitView_name method is not supported yet");
	}

	@Override public void enterModule_name(SQLiteParser.Module_nameContext ctx) {
        LOG.debug("Running listener: enterModule_name: "+ctx.getText());
	    throw new NotImplementedException("The enterModule_name method is not supported yet");
	}

	@Override public void exitModule_name(SQLiteParser.Module_nameContext ctx) {
        LOG.debug("Running listener: exitModule_name: "+ctx.getText());
	    throw new NotImplementedException("The exitModule_name method is not supported yet");
	}

	@Override public void enterPragma_name(SQLiteParser.Pragma_nameContext ctx) {
        LOG.debug("Running listener: enterPragma_name: "+ctx.getText());
	    throw new NotImplementedException("The enterPragma_name method is not supported yet");
	}

	@Override public void exitPragma_name(SQLiteParser.Pragma_nameContext ctx) {
        LOG.debug("Running listener: exitPragma_name: "+ctx.getText());
	    throw new NotImplementedException("The exitPragma_name method is not supported yet");
	}

	@Override public void enterSavepoint_name(SQLiteParser.Savepoint_nameContext ctx) {
        LOG.debug("Running listener: enterSavepoint_name: "+ctx.getText());
	    throw new NotImplementedException("The enterSavepoint_name method is not supported yet");
	}

	@Override public void exitSavepoint_name(SQLiteParser.Savepoint_nameContext ctx) {
        LOG.debug("Running listener: exitSavepoint_name: "+ctx.getText());
	    throw new NotImplementedException("The exitSavepoint_name method is not supported yet");
	}

	@Override public void enterTable_alias(SQLiteParser.Table_aliasContext ctx) {
	    LOG.debug("Running listener: enterTable_alias: "+ctx.getText());
	}

	@Override public void exitTable_alias(SQLiteParser.Table_aliasContext ctx) {
	    LOG.debug("Running listener: exitTable_alias: "+ctx.getText());
	}

	@Override public void enterTransaction_name(SQLiteParser.Transaction_nameContext ctx) {
        LOG.debug("Running listener: enterTransaction_name: "+ctx.getText());
	    throw new NotImplementedException("The enterTransaction_name method is not supported yet");
	}

	@Override public void exitTransaction_name(SQLiteParser.Transaction_nameContext ctx) {
        LOG.debug("Running listener: exitTransaction_name: "+ctx.getText());
	    throw new NotImplementedException("The exitTransaction_name method is not supported yet");
	}

	@Override public void enterAny_name(SQLiteParser.Any_nameContext ctx) {
	    LOG.debug("Running listener: enterAny_name: "+ctx.getText());
	}

	@Override public void exitAny_name(SQLiteParser.Any_nameContext ctx) {
	    LOG.debug("Running listener: exitAny_name: "+ctx.getText());
	}

	@Override public void enterEveryRule(ParserRuleContext ctx) {

	}

	@Override public void exitEveryRule(ParserRuleContext ctx) {
	    LOG.debug("Running listener: exitEveryRule: "+ctx.getText());
	}

	@Override public void visitTerminal(TerminalNode node) {

	}

	@Override public void visitErrorNode(ErrorNode node) {

	}
}