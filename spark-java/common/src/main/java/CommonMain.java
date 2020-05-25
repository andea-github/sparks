import util.ConfigUtil;

/**
 * @author admin 2020-6-19
 */
public class CommonMain {
    public static void main(String[] args) {
        ConfigUtil configUtil = new ConfigUtil("es-config.properties");
//        System.out.println(configUtil.getPropertyBySpring("es.host"));
//        System.out.println(configUtil.getProperty("es.cluster"));
        System.out.println(configUtil.getPropertyByBundle("es.cluster"));
    }
}
