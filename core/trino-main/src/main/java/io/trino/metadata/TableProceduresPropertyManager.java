/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.ParameterRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

// TODO maybe refactor AbstractPropertyManager and use as a base so there is less code copied
public class TableProceduresPropertyManager
{
    private final ConcurrentMap<Key, Map<String, PropertyMetadata<?>>> connectorProperties = new ConcurrentHashMap<>();

    public final void addProperties(CatalogName catalogName, String procedureName, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(procedureName, "procedureName is null");
        requireNonNull(properties, "properties is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);

        checkState(connectorProperties.putIfAbsent(new Key(catalogName, procedureName), propertiesByName) == null, "Properties for procedure '%s.%s' are already registered", catalogName, procedureName);
    }

    public final void removeProperties(CatalogName catalogName)
    {
        Set<Key> keysToRemove = connectorProperties.keySet().stream()
                .filter(key -> catalogName.equals(key.getCatalogName()))
                .collect(toImmutableSet());
        for (Key key : keysToRemove) {
            connectorProperties.remove(key);
        }
    }

    public final Map<String, Object> getProperties(
            CatalogName catalogName,
            String procedureName,
            String catalog, // only use this for error messages
            Map<String, Expression> sqlPropertyValues,
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = connectorProperties.get(new Key(catalogName, procedureName));
        if (supportedProperties == null) {
            throw new TrinoException(NOT_FOUND, format("Procedure %s.%s not found", catalog, procedureName));
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        // Fill in user-specified properties
        for (Map.Entry<String, Expression> sqlProperty : sqlPropertyValues.entrySet()) {
            String propertyName = sqlProperty.getKey().toLowerCase(ENGLISH);
            PropertyMetadata<?> property = supportedProperties.get(propertyName);
            if (property == null) {
                throw new TrinoException(
                        INVALID_PROCEDURE_ARGUMENT,
                        format("Procedure '%s.%s' does not support property '%s'",
                                catalog,
                                procedureName,
                                propertyName));
            }

            Object sqlObjectValue;
            try {
                sqlObjectValue = evaluatePropertyValue(sqlProperty.getValue(), property.getSqlType(), session, metadata, accessControl, parameters);
            }
            catch (TrinoException e) {
                throw new TrinoException(
                        INVALID_PROCEDURE_ARGUMENT,
                        format("Invalid value for procedure property '%s': Cannot convert [%s] to %s",
                                property.getName(),
                                sqlProperty.getValue(),
                                property.getSqlType()),
                        e);
            }

            Object value;
            try {
                value = property.decode(sqlObjectValue);
            }
            catch (Exception e) {
                throw new TrinoException(
                        INVALID_PROCEDURE_ARGUMENT,
                        format(
                                "Unable to set procedure property '%s' to [%s]: %s",
                                property.getName(),
                                sqlProperty.getValue(),
                                e.getMessage()),
                        e);
            }

            properties.put(property.getName(), value);
        }
        Map<String, Object> userSpecifiedProperties = properties.build();

        // Fill in the remaining properties with non-null defaults
        for (PropertyMetadata<?> propertyMetadata : supportedProperties.values()) {
            if (!userSpecifiedProperties.containsKey(propertyMetadata.getName())) {
                Object value = propertyMetadata.getDefaultValue();
                if (value != null) {
                    properties.put(propertyMetadata.getName(), value);
                }
            }
        }
        return properties.build();
    }

//    public Map<CatalogName, Map<String, PropertyMetadata<?>>> getAllProperties()
//    {
//        return ImmutableMap.copyOf(connectorProperties);
//    }

    private Object evaluatePropertyValue(
            Expression expression,
            Type expectedType,
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
        Object value = evaluateConstantExpression(rewritten, expectedType, metadata, session, accessControl, parameters);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(null, 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);

        if (objectValue == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, format("Invalid null value for procedure property"));
        }
        return objectValue;
    }

    private class Key
    {
        private final CatalogName catalogName;
        private final String procedureName;

        public Key(CatalogName catalogName, String procedureName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.procedureName = requireNonNull(procedureName, "procedureName is null");
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public String getProcedureName()
        {
            return procedureName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(catalogName, key.catalogName)
                    && Objects.equals(procedureName, key.procedureName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalogName, procedureName);
        }
    }
}
