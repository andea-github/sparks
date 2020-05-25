package util;

import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * 读取配置文件属性
 *
 * @author admin 2020-6-19
 */
public class ConfigUtil {
    private static Properties properties;
    private String resourceName;

    public ConfigUtil() {
    }

    public ConfigUtil(String resourceName) {
        this.resourceName = resourceName;
    }

    /**
     * 1. get by bundle, using the specified base name
     */
    public String getPropertyByBundle(String key) {
        String resource = this.resourceName.contains(".") ? this.resourceName.substring(0, this.resourceName.indexOf(".")) : this.resourceName;
        ResourceBundle bundle = ResourceBundle.getBundle(resource);
        return bundle.getString(key);
    }


    /**
     * 2. get by java 反射, using the full name
     */
    public String getProperty(String key) {
        if (properties == null)
            properties = new Properties();
        InputStream inputStream = ConfigUtil.class.getClassLoader().getResourceAsStream(this.resourceName);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties.getProperty(key);
    }

    /**
     * 3. get by spring, Recommended use the @Configuration + @Value
     */
    public String getPropertyBySpring(String key) {
        try {
            properties = PropertiesLoaderUtils.loadAllProperties(this.resourceName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties.getProperty(key);
    }
}
