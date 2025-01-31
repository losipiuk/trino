package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.faker.ColumnInfo.ALLOWED_VALUES_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;

public class PropertyValues
{
    private PropertyValues() {}

    public static Object propertyValue(ColumnMetadata column, String property)
    {
        if (ALLOWED_VALUES_PROPERTY.equals(property)) {
            if (column.getProperties().containsKey(ALLOWED_VALUES_PROPERTY)) {
                ImmutableList.Builder<Object> builder = ImmutableList.builder();
                for (String value : strings((List<?>) column.getProperties().get(ALLOWED_VALUES_PROPERTY))) {
                    try {
                        builder.add(Literal.parse(value, column.getType()));
                    }
                    catch (IllegalArgumentException | ClassCastException e) {
                        throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must only contain valid %s literals, failed to parse `%s`".formatted(ALLOWED_VALUES_PROPERTY, column.getType().getDisplayName(), value), e);
                    }
                }
                return builder.build();
            }
            else {
                return null;
            }
        }

        try {
            return Literal.parse((String) column.getProperties().get(property), column.getType());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must be a valid %s literal".formatted(property, column.getType().getDisplayName()), e);
        }
    }

    private static List<String> strings(Collection<?> values)
    {
        return values.stream()
                .map(String.class::cast)
                .collect(toImmutableList());
    }
}
