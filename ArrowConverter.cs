using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

public static class ArrowConverter
{
    public static RecordBatch ConvertToRecordBatch<T>(List<T> records)
    {
        if (records == null || records.Count == 0)
            throw new ArgumentException("The records list is empty.");

        // Get properties dynamically
        var properties = typeof(T).GetProperties();

        // Create schema dynamically based on property types
        var fields = new List<Field>();
        var arrayBuilders = new Dictionary<string, IArrowArrayBuilder>();

        foreach (var prop in properties)
        {
            var fieldType = GetArrowType(prop.PropertyType);
            if (fieldType == null)
                throw new NotSupportedException($"Type {prop.PropertyType} is not supported.");

            fields.Add(new Field(prop.Name, fieldType, nullable: true));

            // Create the corresponding Arrow array builder
            arrayBuilders[prop.Name] = CreateArrayBuilder(fieldType);
        }

        // Populate the builders with data
        foreach (var record in records)
        {
            foreach (var prop in properties)
            {
                var value = prop.GetValue(record);
                AppendToArrayBuilder(arrayBuilders[prop.Name], value);
            }
        }

        // Build final Arrow arrays
        var arrowArrays = fields.Select(f => arrayBuilders[f.Name].Build()).ToList();

        // Create schema
        var schema = new Schema(fields);

        // Create and return the RecordBatch
        return new RecordBatch(schema, arrowArrays, records.Count);
    }

    private static IArrowArrayBuilder CreateArrayBuilder(IArrowType type)
    {
        return type switch
        {
            StringType => new StringArray.Builder(),
            Int32Type => new Int32Array.Builder(),
            Int64Type => new Int64Array.Builder(),
            DoubleType => new DoubleArray.Builder(),
            FloatType => new FloatArray.Builder(),
            BooleanType => new BooleanArray.Builder(),
            _ => throw new NotSupportedException($"Unsupported Arrow type: {type}")
        };
    }

    private static void AppendToArrayBuilder(IArrowArrayBuilder builder, object value)
    {
        switch (builder)
        {
            case StringArray.Builder stringBuilder:
                stringBuilder.Append(value as string);
                break;
            case Int32Array.Builder intBuilder:
                intBuilder.Append(value != null ? Convert.ToInt32(value) : (int?)null);
                break;
            case Int64Array.Builder longBuilder:
                longBuilder.Append(value != null ? Convert.ToInt64(value) : (long?)null);
                break;
            case DoubleArray.Builder doubleBuilder:
                doubleBuilder.Append(value != null ? Convert.ToDouble(value) : (double?)null);
                break;
            case FloatArray.Builder floatBuilder:
                floatBuilder.Append(value != null ? Convert.ToSingle(value) : (float?)null);
                break;
            case BooleanArray.Builder boolBuilder:
                boolBuilder.Append(value != null ? Convert.ToBoolean(value) : (bool?)null);
                break;
            default:
                throw new NotSupportedException($"Unsupported builder type: {builder.GetType()}");
        }
    }

    private static IArrowType GetArrowType(Type type)
    {
        if (type == typeof(string)) return new StringType();
        if (type == typeof(int) || type == typeof(int?)) return new Int32Type();
        if (type == typeof(long) || type == typeof(long?)) return new Int64Type();
        if (type == typeof(double) || type == typeof(double?)) return new DoubleType();
        if (type == typeof(float) || type == typeof(float?)) return new FloatType();
        if (type == typeof(bool) || type == typeof(bool?)) return new BooleanType();
        return null; // Unsupported type
    }
}
