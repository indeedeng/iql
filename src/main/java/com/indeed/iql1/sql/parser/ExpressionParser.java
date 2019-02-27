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

package com.indeed.iql1.sql.parser;

import com.indeed.iql1.sql.ast.BinaryExpression;
import com.indeed.iql1.sql.ast.BracketsExpression;
import com.indeed.iql1.sql.ast.Expression;
import com.indeed.iql1.sql.ast.FunctionExpression;
import com.indeed.iql1.sql.ast.NameExpression;
import com.indeed.iql1.sql.ast.NumberExpression;
import com.indeed.iql1.sql.ast.Op;
import com.indeed.iql1.sql.ast.StringExpression;
import com.indeed.iql1.sql.ast.TupleExpression;
import com.indeed.iql1.sql.ast.UnaryExpression;
import org.codehaus.jparsec.OperatorTable;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parser.Reference;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.functors.Binary;
import org.codehaus.jparsec.functors.Map2;
import org.codehaus.jparsec.functors.Unary;
import org.codehaus.jparsec.misc.Mapper;

/**
 * Parser for expressions.
 * 
 * @author Ben Yu
 */
public final class ExpressionParser {
  private ExpressionParser() {
  }

  private static final Parser<Expression> SIGNED_NUMBER =
      Parsers.sequence(TerminalParser.term("-").retn("-").optional(), TerminalParser.NUMBER, new Map2<String, String, Expression>() {
          @Override
          public Expression map(String o, String s) {
              final String val;
              if(o != null) {
                  val = "-"+s;
              } else {
                  val = s;
              }
              return new NumberExpression(val);
          }
      });
  
  private static final Parser<Expression> STRING =
       curry(StringExpression.class).sequence(TerminalParser.STRING);

  private static final Parser<Expression> UNQUOTED_STRING =
            curry(StringExpression.class).sequence(Parsers.or(TerminalParser.STRING, TerminalParser.NUMBER, TerminalParser.NAME).many1().source());

  private static final Parser<Expression> NAME =
       curry(NameExpression.class).sequence(TerminalParser.NAME);

  public static Expression parseExpression(String str) {
      return TerminalParser.parse(expression(), str);
  }

  private static Parser<Expression> functionCall(Parser<Expression> param) {
    return curry(FunctionExpression.class)
        .sequence(TerminalParser.NAME,
            TerminalParser.term("("), param.sepBy(TerminalParser.term(",")), TerminalParser.term(")")).label("function call");
  }
  
  private static Parser<Expression> tuple(Parser<Expression> expr) {
    return curry(TupleExpression.class)
        .sequence(TerminalParser.term("("), expr.sepBy(TerminalParser.term(",")), TerminalParser.term(")"));
  }
  
  private static <T> Parser<T> paren(Parser<T> parser) {
    return parser.between(TerminalParser.term("("), TerminalParser.term(")"));
  }

  private static Parser<Expression> arithmetic(Parser<Expression> atom) {
    Reference<Expression> reference = Parser.newReference();
    Parser<Expression> operand =
        Parsers.or(paren(reference.lazy()), functionCall(reference.lazy()), atom);
    Parser<Expression> parser = new OperatorTable<Expression>()
        .infixl(binary("/", Op.AGG_DIV), 5)
        .infixl(binary("<", Op.LESS), 7)
        .infixl(binary("<=", Op.LESS_EQ), 7)
        .infixl(binary("=", Op.EQ), 7)
        .infixl(binary("!=", Op.NOT_EQ), 7)
        .infixl(binary(">", Op.GREATER), 7)
        .infixl(binary(">=", Op.GREATER_EQ), 7)
        .infixl(binary("+", Op.PLUS), 10)
        .infixl(binary("-", Op.MINUS), 10)
        .infixl(binary("*", Op.MUL), 20)
        .infixl(binary("\\", Op.DIV), 20)
        .infixl(binary("%", Op.MOD), 20)
        .prefix(unary("-", Op.NEG), 30)
        .build(operand);
    reference.set(parser);
    return parser;
  }

  private static Parser<Expression> atomWhere() {
      // can't have unquoted strings at the top level of where expressions as the spaces there are meaningful
      return Parsers.or(SIGNED_NUMBER, STRING, NAME);
  }

  private static Parser<Expression> atom() {
      return Parsers.longest(SIGNED_NUMBER, STRING, NAME, UNQUOTED_STRING);
  }

  static Parser<Expression> groupByExpression() {
    return Parsers.longest(NAME,
            functionCall(expression()),
            bracketExpression(),
            explodeExpression(),
            groupByIn()).label("group by expression");
  }

    private static Parser<Expression> groupByIn() {
        return binaryExpression(Op.IN).sequence(NAME, TerminalParser.term("in"), tuple(atom())).label("IN grouping");
    }

    private static Parser<Expression> explodeExpression() {
        return unaryExpression(Op.EXPLODE).sequence(NAME, TerminalParser.term("*")).label("explode grouping");
    }

  static Parser<Expression> expression() {
    return arithmetic(atom()).label("expression");
  }

    private static Parser<BracketsExpression> bracketExpression() {
        return Mapper.curry(BracketsExpression.class).sequence(
                TerminalParser.NAME,
                TerminalParser.notTerm("]", Parsers.ANY_TOKEN).many().source().between(TerminalParser.term("["), TerminalParser.term("]")).label("bracket expression")
        );
    }

    /************************** where expressions ****************************/

    public static Expression parseWhereExpression(String str) {
        return TerminalParser.parse(whereExpression(), str);
    }
  
  private static Parser<Expression> whereExpression() {
      Parser<Expression> singleFilter = filter();

      return new OperatorTable<Expression>()
              .prefix(unary("-", Op.NEG), 30)
              .infixl(binaryOptional("and", Op.AND), 20)
              .build(singleFilter).label("where expression");
  }

  private static Parser<Expression> filter() {
    // each filter is one of: 1) simple field equality 2) metric inequality/comparison 3) IN operation 4) function call with any expressions as params
    // TODO: should be able to do a metric comparison involving functions e.g. floatscale(yearlysalary,1,0) != 0
    return Parsers.or(
            comparison(NAME, atomWhere()),
            inCondition(),
            functionCall(arithmetic(atom())),  // function parameters are away from top level so we use non-where version of atom
            arithmetic(atomWhere())
    );
  }

  private static Parser<Expression> comparison(Parser<Expression> opLeft, Parser<Expression> opRight) {
      return Parsers.or(
              compare(opLeft, "=", Op.EQ, opRight),
              compare(opLeft, ":", Op.EQ, opRight),
              compare(opLeft, "!=", Op.NOT_EQ, opRight),
              compare(opLeft, "=~", Op.REGEX_EQ, opRight),
              compare(opLeft, "!=~", Op.REGEX_NOT_EQ, opRight)).label("comparison");
  }

    private static Parser<Expression> inCondition() {
    return binaryExpression(Op.IN).sequence(NAME, TerminalParser.term("in"), tuple(atom())).label("IN condition").or(
        binaryExpression(Op.NOT_IN).sequence(NAME, TerminalParser.phrase("not in"), tuple(atom())).label("NOT IN condition")
    );
  }
  
  /************************** utility methods ****************************/
  
  private static Parser<Expression> compare(
      Parser<Expression> leftOperand, String name, Op op, Parser<Expression> rightOperand) {
    return curry(BinaryExpression.class).sequence(leftOperand, TerminalParser.term(name).retn(op), rightOperand);
  }
  
  private static Parser<Binary<Expression>> binary(String name, Op op) {
    return TerminalParser.term(name).next(binaryExpression(op).binary());
  }
  private static Parser<Binary<Expression>> binaryOptional(String name, Op op) {
    return TerminalParser.term(name).optional().next(binaryExpression(op).binary());
  }

    private static Parser<Unary<Expression>> unary(String name, Op op) {
        return TerminalParser.term(name).next(unaryExpression(op).unary());
    }
  
  private static Mapper<Expression> binaryExpression(Op op) {
    return curry(BinaryExpression.class, op);
  }

    private static Mapper<Expression> unaryExpression(Op op) {
        return curry(UnaryExpression.class, op);
    }
  
  private static Mapper<Expression> curry(Class<? extends Expression> clazz, Object... args) {
    return Mapper.curry(clazz, args);
  }
}
