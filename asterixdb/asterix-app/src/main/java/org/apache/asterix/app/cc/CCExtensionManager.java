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
package org.apache.asterix.app.cc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.algebra.base.ILangExtension.Language;
import org.apache.asterix.algebra.extension.IAlgebraExtensionManager;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.common.api.IExtension;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.utils.ExtensionUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * AsterixDB's implementation of {@code IAlgebraExtensionManager} which takes care of
 * initializing extensions for App and Compilation purposes
 */
public class CCExtensionManager implements IAlgebraExtensionManager {

    private final Map<ExtensionId, IExtension> extensions = new HashMap<>();

    private final IStatementExecutorExtension statementExecutorExtension;
    private final ILangCompilationProvider aqlCompilationProvider;
    private final ILangCompilationProvider sqlppCompilationProvider;
    private transient IStatementExecutorFactory statementExecutorFactory;

    /**
     * Initialize {@code CompilerExtensionManager} from configuration
     *
     * @param list
     *            a list of extensions
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws HyracksDataException
     */
    public CCExtensionManager(List<AsterixExtension> list)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, HyracksDataException {
        Pair<ExtensionId, ILangCompilationProvider> aqlcp = null;
        Pair<ExtensionId, ILangCompilationProvider> sqlppcp = null;
        IStatementExecutorExtension see = null;
        if (list != null) {
            for (AsterixExtension extensionConf : list) {
                IExtension extension = (IExtension) Class.forName(extensionConf.getClassName()).newInstance();
                extension.configure(extensionConf.getArgs());
                if (extensions.containsKey(extension.getId())) {
                    throw new RuntimeDataException(ErrorCode.EXTENSION_ID_CONFLICT, extension.getId());
                }
                extensions.put(extension.getId(), extension);
                switch (extension.getExtensionKind()) {
                    case STATEMENT_EXECUTOR:
                        see = ExtensionUtil.extendStatementExecutor(see, (IStatementExecutorExtension) extension);
                        break;
                    case LANG:
                        ILangExtension le = (ILangExtension) extension;
                        aqlcp = ExtensionUtil.extendLangCompilationProvider(Language.AQL, aqlcp, le);
                        sqlppcp = ExtensionUtil.extendLangCompilationProvider(Language.SQLPP, sqlppcp, le);
                        break;
                    default:
                        break;
                }
            }
        }
        this.statementExecutorExtension = see;
        this.aqlCompilationProvider = aqlcp == null ? new AqlCompilationProvider() : aqlcp.second;
        this.sqlppCompilationProvider = sqlppcp == null ? new SqlppCompilationProvider() : sqlppcp.second;
    }

    /** @deprecated use getStatementExecutorFactory instead */
    @Deprecated
    public IStatementExecutorFactory getQueryTranslatorFactory() {
        return getStatementExecutorFactory(null);
    }

    public IStatementExecutorFactory getStatementExecutorFactory(ExecutorService executorService) {
        if (statementExecutorFactory == null) {
            statementExecutorFactory = statementExecutorExtension == null
                    ? new DefaultStatementExecutorFactory(executorService)
                    : statementExecutorExtension.getStatementExecutorFactory(executorService);
        }
        return statementExecutorFactory;
    }

    public ILangCompilationProvider getAqlCompilationProvider() {
        return aqlCompilationProvider;
    }

    public ILangCompilationProvider getSqlppCompilationProvider() {
        return sqlppCompilationProvider;
    }
}
