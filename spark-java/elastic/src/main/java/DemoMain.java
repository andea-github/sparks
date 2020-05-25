import es.EsConfig;

import java.util.Properties;

/**
 * @author admin 2020-6-19
 */
public class DemoMain {

    public static void main(String[] args) {
        EsConfig esConfig = new EsConfig();
        System.out.println(esConfig.getNodes());
        Properties properties = new Properties();
    }
}
