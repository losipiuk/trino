package io.trino.plugin.faker;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;

import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;

public class PropertyValues
{
    private PropertyValues() {}

    public static Object propertyValue(ColumnMetadata column, String property)
    {
        try {
            return Literal.parse((String) column.getProperties().get(property), column.getType());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must be a valid %s literal".formatted(property, column.getType().getDisplayName()), e);
        }
    }
}
