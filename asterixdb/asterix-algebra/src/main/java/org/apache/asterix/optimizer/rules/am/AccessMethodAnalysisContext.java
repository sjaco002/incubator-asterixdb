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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;

/**
 * Context for analyzing the applicability of a single access method.
 */
public class AccessMethodAnalysisContext {

    private List<IOptimizableFuncExpr> matchedFuncExprs = new ArrayList<IOptimizableFuncExpr>();

    // Contains candidate indexes and a list of (integer,integer) tuples that index into matchedFuncExprs and
    // matched variable inside this expr. We are mapping from candidate indexes to a list of function expressions
    // that match one of the index's expressions.
    private Map<Index, List<Pair<Integer, Integer>>> indexExprsAndVars =
            new TreeMap<Index, List<Pair<Integer, Integer>>>();

    // Maps from index to the dataset it is indexing.
    private Map<Index, Dataset> indexDatasetMap = new TreeMap<Index, Dataset>();

    // Maps from an index to the number of matched fields in the query plan (for performing prefix search)
    private Map<Index, Integer> indexNumMatchedKeys = new TreeMap<Index, Integer>();

    // variables for resetting null placeholder for left-outer-join
    private Mutable<ILogicalOperator> lojGroupbyOpRef = null;
    private ScalarFunctionCallExpression lojIsNullFuncInGroupBy = null;

    public void addIndexExpr(Dataset dataset, Index index, Integer exprIndex, Integer varIndex) {
        List<Pair<Integer, Integer>> exprs = getIndexExprsFromIndexExprsAndVars(index);
        if (exprs == null) {
            exprs = new ArrayList<Pair<Integer, Integer>>();
            putIndexExprToIndexExprsAndVars(index, exprs);
        }
        exprs.add(new Pair<Integer, Integer>(exprIndex, varIndex));
        putDatasetIntoIndexDatasetMap(index, dataset);
    }

    public List<IOptimizableFuncExpr> getMatchedFuncExprs() {
        return matchedFuncExprs;
    }

    public IOptimizableFuncExpr getMatchedFuncExpr(int index) {
        return matchedFuncExprs.get(index);
    }

    public void addMatchedFuncExpr(IOptimizableFuncExpr optFuncExpr) {
        matchedFuncExprs.add(optFuncExpr);
    }

    public Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> getIteratorForIndexExprsAndVars() {
        return indexExprsAndVars.entrySet().iterator();
    }

    public boolean isIndexExprsAndVarsEmpty() {
        return indexExprsAndVars.isEmpty();
    }

    public List<Pair<Integer, Integer>> getIndexExprsFromIndexExprsAndVars(Index index) {
        return indexExprsAndVars.get(index);
    }

    public void putIndexExprToIndexExprsAndVars(Index index, List<Pair<Integer, Integer>> exprs) {
        indexExprsAndVars.put(index, exprs);
    }

    public Integer getNumberOfMatchedKeys(Index index) {
        return indexNumMatchedKeys.get(index);
    }

    public void putNumberOfMatchedKeys(Index index, Integer numMatchedKeys) {
        indexNumMatchedKeys.put(index, numMatchedKeys);
    }

    public void setLOJGroupbyOpRef(Mutable<ILogicalOperator> opRef) {
        lojGroupbyOpRef = opRef;
    }

    public Mutable<ILogicalOperator> getLOJGroupbyOpRef() {
        return lojGroupbyOpRef;
    }

    public void setLOJIsNullFuncInGroupBy(ScalarFunctionCallExpression isNullFunc) {
        lojIsNullFuncInGroupBy = isNullFunc;
    }

    public ScalarFunctionCallExpression getLOJIsNullFuncInGroupBy() {
        return lojIsNullFuncInGroupBy;
    }

    public Dataset getDatasetFromIndexDatasetMap(Index idx) {
        return indexDatasetMap.get(idx);
    }

    public void putDatasetIntoIndexDatasetMap(Index idx, Dataset ds) {
        indexDatasetMap.put(idx, ds);
    }

}
