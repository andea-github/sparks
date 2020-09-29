package finestjson;

import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.ExtraProcessor;
import com.alibaba.fastjson.parser.deserializer.ExtraTypeProvider;
import com.alibaba.fastjson.parser.deserializer.FieldTypeResolver;
import com.alibaba.fastjson.parser.deserializer.ParseProcess;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializeFilter;
import com.alibaba.fastjson.serializer.SerializeWriter;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.lang.reflect.Type;

/**
 * @author admin 2020-6-29
 */
public class FinestJSON {
    public static int DEFAULT_PARSER_FEATURE;
    static final SerializeFilter[] emptyFilters = new SerializeFilter[0];
    public static int DEFAULT_GENERATE_FEATURE;

    public static <T> T parseObject(String text, Class<T> clazz) {

        return parseObject(text, clazz, null);
    }

    public static <T> T parseObject(String text, Class<T> clazz, Feature... features) {
        ParserConfig global = ParserConfig.global;
        return (T) parseObject(text, clazz, global, DEFAULT_PARSER_FEATURE, features);
    }

    public static <T> T parseObject(String input, Type clazz, ParserConfig config, int featureValues,
                                    Feature... features) {
        return parseObject(input, clazz, config, null, featureValues, features);
    }

    public static <T> T parseObject(String input, Type clazz, ParserConfig config, ParseProcess processor,
                                    int featureValues, Feature... features) {
        if (input == null) {
            return null;
        }

        if (features != null) {
            /*for (Feature feature : features) {
                System.out.println(String.format("---------- %s", feature));
                featureValues = Feature.config(featureValues, feature, true);
            }*/
        }

        DefaultJSONParser parser = new DefaultJSONParser(input, config, featureValues);

        if (processor instanceof ExtraTypeProvider) {
            parser.getExtraTypeProviders().add((ExtraTypeProvider) processor);
        }

        if (processor instanceof ExtraProcessor) {
            parser.getExtraProcessors().add((ExtraProcessor) processor);
        }

        if (processor instanceof FieldTypeResolver) {
            parser.setFieldTypeResolver((FieldTypeResolver) processor);
        }

        T value = parser.parseObject(clazz);

        parser.handleResovleTask(value);

        parser.close();

        return value;
    }

    // ======================
    public static String toJSONString(Object object) {
        return toJSONString(object, emptyFilters);
    }

    public static String toJSONString(Object object, SerializeFilter[] filters, SerializerFeature... features) {
        return toJSONString(object, SerializeConfig.globalInstance, filters, null, DEFAULT_GENERATE_FEATURE, features);
    }

    public static String toJSONString(Object object, //
                                      SerializeConfig config, //
                                      SerializeFilter[] filters, //
                                      String dateFormat, //
                                      int defaultFeatures, //
                                      SerializerFeature... features) {
        SerializeWriter out = new SerializeWriter(null, defaultFeatures, features);

        try {
            JSONSerializer serializer = new JSONSerializer(out, config);
            for (com.alibaba.fastjson.serializer.SerializerFeature feature : features) {
                serializer.config(feature, true);
            }

            if (dateFormat != null && dateFormat.length() != 0) {
                serializer.setDateFormat(dateFormat);
                serializer.config(SerializerFeature.WriteDateUseDateFormat, true);
            }

            if (filters != null) {
                for (SerializeFilter filter : filters) {
                    serializer.addFilter(filter);
                }
            }

            serializer.write(object);

            return out.toString();
        } finally {
            out.close();
        }
    }
}
