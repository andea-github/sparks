package util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

/**
 * @author admin 2020-6-29
 */
public class FinestJSON {
    private static final String PATH = "file://C:\\Users\\admin\\.m2\\repository\\com\\alibaba\\fastjson\\1.2.9\\fastjson-1.2.9.jar";
    private static String objPath = "com.alibaba.fastjson.JSONObject";
    private static URLClassLoader loader;
    public static Class<?> aClass;
    public static Object obj;
    public static Method method;

    static {
        try {
            URL url = new URL(PATH);
            loader = new URLClassLoader(new URL[]{url});
            if (!isBlank(objPath)) initClassLoader();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public FinestJSON() {
    }

    public FinestJSON(String objPath) {
        if (!isBlank(objPath)) {
            FinestJSON.objPath = objPath;
            initClassLoader();
        }
    }

    private static void initClassLoader() {
        try {
            aClass = loader.loadClass(objPath);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        obj = getInstance();
    }

    private static Object getInstance() {
        try {
            return aClass.newInstance();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * jsonStr 2 javaBean
     *
     * @param text
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T parseObject(String text, Class<T> clazz) {
        if (text == null) {
            return null;
        }
        try {
            Method method = aClass.getMethod("parseObject", String.class, Class.class);
            // Make the function  valid
            method.setAccessible(true);
            // Invoke method
            return (T) method.invoke(obj, text, clazz);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * jsonArray 2 List
     *
     * @param text
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> List<T> parseArray(String text, Class<T> clazz) {
        List<T> list = null;
        if (text == null) {
            list = null;
        }
        try {
            Method method = aClass.getMethod("parseArray", String.class, Class.class);
            method.setAccessible(true);
            list = (List<T>) method.invoke(obj, text, clazz);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * javaBean 2 jsonStr
     *
     * @param javaBean
     * @return
     */
    public static String toJSONString(Object javaBean) {
        try {
            Method method = aClass.getMethod("toJSONString", Object.class);
            method.setAccessible(true);
            return method.invoke(obj, javaBean).toString();
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }

    @Deprecated
    private Object getInstance(Class<?> aClass) {
        try {
            return aClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }


}

