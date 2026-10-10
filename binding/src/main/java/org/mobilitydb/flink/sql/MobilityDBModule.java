/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.flink.sql;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.module.Module;

/**
 * The MobilityDB SQL surface as a Flink module, loaded into any SQL session, the SQL Client,
 * the SQL Gateway or a table environment, by
 *
 * <pre>
 *   LOAD MODULE mobilitydb;
 * </pre>
 *
 * which Flink answers through {@link MobilityDBModuleFactory}, found by the service loader.
 *
 * The module holds exactly the functions {@link MobilityFlinkSql#registerAll} registers, under
 * the same names and as the same classes: it runs that method once on a table environment that
 * records each createTemporaryFunction(name, class) call it receives, so the generated
 * registration is the one list of the surface.
 *
 * A module function is a system function, which Flink resolves after the temporary system
 * functions and before the catalog functions, module by module in the order of
 * {@code USE MODULES}. A name the core module also holds (lower, round, abs, ...) thus answers
 * with Flink's built-in while the core module comes first, the order LOAD MODULE leaves.
 */
public final class MobilityDBModule implements Module {

    public static final MobilityDBModule INSTANCE = new MobilityDBModule();

    /** The class of each function, keyed by its name in lower case, as Flink normalizes it. */
    private final Map<String, Class<? extends UserDefinedFunction>> normalizedFunctions;

    /** The name of each function as registerAll writes it. */
    private final Set<String> functionNames;

    private MobilityDBModule() {
        Map<String, Class<? extends UserDefinedFunction>> functions = new LinkedHashMap<>();
        MobilityFlinkSql.registerAll(recorder(functions));
        Map<String, Class<? extends UserDefinedFunction>> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Class<? extends UserDefinedFunction>> e : functions.entrySet()) {
            if (normalized.put(e.getKey().toLowerCase(Locale.ROOT), e.getValue()) != null) {
                throw new IllegalStateException(
                        "two MobilityDB functions share the name " + e.getKey());
            }
        }
        this.normalizedFunctions = Collections.unmodifiableMap(normalized);
        this.functionNames = Collections.unmodifiableSet(functions.keySet());
    }

    /**
     * A table environment that records each function registerAll registers and refuses every
     * other call, so a registration of another kind fails here rather than going missing.
     */
    private static TableEnvironment recorder(
            Map<String, Class<? extends UserDefinedFunction>> functions) {
        return (TableEnvironment) Proxy.newProxyInstance(
                TableEnvironment.class.getClassLoader(),
                new Class<?>[] {TableEnvironment.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("createTemporaryFunction")
                            && args != null && args.length == 2
                            && args[0] instanceof String && args[1] instanceof Class) {
                        functions.put((String) args[0],
                                ((Class<?>) args[1]).asSubclass(UserDefinedFunction.class));
                        return null;
                    }
                    throw new UnsupportedOperationException("MobilityDBModule reads only "
                            + "createTemporaryFunction(String, Class) from registerAll, not "
                            + method);
                });
    }

    @Override
    public Set<String> listFunctions() {
        return functionNames;
    }

    @Override
    public Set<String> listFunctions(boolean includeHiddenFunctions) {
        return functionNames;
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        Class<? extends UserDefinedFunction> cls =
                normalizedFunctions.get(name.toLowerCase(Locale.ROOT));
        if (cls == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(cls.getDeclaredConstructor().newInstance());
        } catch (ReflectiveOperationException e) {
            throw new ValidationException("Cannot instantiate " + cls.getName(), e);
        }
    }
}
