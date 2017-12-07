/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.lang.sqlpp.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupBySugarVisitor;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.CheckSubqueryVisitor;
import org.apache.asterix.lang.sqlpp.visitor.DeepCopyVisitor;
import org.apache.asterix.lang.sqlpp.visitor.FreeVariableVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSubstituteExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class SqlppRewriteUtil {

    private SqlppRewriteUtil() {
    }

    // Applying sugar rewriting for group-by.
    public static Expression rewriteExpressionUsingGroupVariable(VariableExpr groupVar,
            Collection<VariableExpr> fieldVars, ILangExpression expr, LangRewritingContext context)
            throws CompilationException {
        SqlppGroupBySugarVisitor visitor = new SqlppGroupBySugarVisitor(context, groupVar, fieldVars);
        return expr.accept(visitor, null);
    }

    public static Set<VariableExpr> getFreeVariable(Expression expr) throws CompilationException {
        Set<VariableExpr> vars = new HashSet<>();
        FreeVariableVisitor visitor = new FreeVariableVisitor();
        expr.accept(visitor, vars);
        return vars;
    }

    public static ILangExpression deepCopy(ILangExpression expr) throws CompilationException {
        if (expr == null) {
            return expr;
        }
        DeepCopyVisitor visitor = new DeepCopyVisitor();
        return expr.accept(visitor, null);
    }

    // Checks if an ILangExpression contains a subquery.
    public static boolean constainsSubquery(ILangExpression expr) throws CompilationException {
        if (expr == null) {
            return false;
        }
        CheckSubqueryVisitor visitor = new CheckSubqueryVisitor();
        return expr.accept(visitor, null);
    }

    /**
     * Substitutes expression with replacement expressions according to the exprMap.
     *
     * @param expression
     *            ,
     *            an input expression.
     * @param exprMap
     *            a map that maps expressions to their corresponding replacement expressions.
     * @return an expression, where sub-expressions of the input expression (including the input expression itself)
     *         are replaced with deep copies with their mapped replacements in the exprMap if there exists such a
     *         replacement expression.
     * @throws CompilationException
     */
    public static Expression substituteExpression(Expression expression, Map<Expression, Expression> exprMap,
            LangRewritingContext context) throws CompilationException {
        if (exprMap.isEmpty()) {
            return expression;
        }
        // Creates a wrapper query for the expression so that if the expression itself
        // is the key, it can also be replaced.
        Query wrapper = new Query(false);
        wrapper.setBody(expression);
        // Creates a substitution visitor.
        SqlppSubstituteExpressionVisitor visitor = new SqlppSubstituteExpressionVisitor(context, exprMap);
        wrapper.accept(visitor, wrapper);
        return wrapper.getBody();
    }

    /**
     *
     * Creates a set of lets to represent the parameters of a functions
     * Enabling the function to be compiled to test for usability
     *
     * @param cfs
     *            The statement that is trying to create a function
     * @return List<LetClause>
     */
    public static List<LetClause> createLetsToTestFunction(CreateFunctionStatement cfs) {
        List<LetClause> lets = new ArrayList<>();
        FunctionIdentifier getJobParameter = BuiltinFunctions.GET_JOB_PARAMETER;
        FunctionSignature sig = new FunctionSignature(getJobParameter.getNamespace(), getJobParameter.getName(),
                getJobParameter.getArity());

        List<VariableExpr> varList = new ArrayList<>();
        List<String> vars = cfs.getParamList();
        for (int i = 0; i < vars.size(); i++) {
            varList.add(new VariableExpr(new VarIdentifier(vars.get(i), i)));
        }

        for (VariableExpr var : varList) {
            List<Expression> strListForCall = new ArrayList<>();
            LiteralExpr l = new LiteralExpr(new StringLiteral(var.getVar().getValue()));
            strListForCall.add(l);
            Expression con = new CallExpr(sig, strListForCall);
            LetClause let = new LetClause(var, con);
            lets.add(let);
        }
        return lets;
    }

    /**
     *
     * Creates a query to test usability of function by compilation
     *
     * @param cfs
     *            The statement that is trying to create a function
     * @return Query
     */
    public static Query createQueryToTestFunction(CreateFunctionStatement cfs) {
        List<FromTerm> froms = new ArrayList<>();
        FromTerm ft = new FromTerm(cfs.getFunctionBodyExpression(), new VariableExpr(new VarIdentifier()), null, null);
        froms.add(ft);
        FromClause from = new FromClause(froms);
        Projection p = new Projection(null, null, true, false);
        List<Projection> ps = new ArrayList<>();
        ps.add(p);
        SelectClause selectClause = new SelectClause(null, new SelectRegular(ps), false);
        SelectBlock block = new SelectBlock(selectClause, from, createLetsToTestFunction(cfs), null, null, null, null);
        SelectSetOperation ssop = new SelectSetOperation(new SetOperationInput(block, null), null);
        SelectExpression se = new SelectExpression(null, ssop, null, null, true);
        Query q = new Query(false);
        q.setBody(se);
        return q;
    }

}
