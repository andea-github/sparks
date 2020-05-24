import java.util.Arrays;

public class MainArgs {
    public static void main(String[] args) {
        for (String arg : args)
            System.out.println(String.format("========== [%d] ", Arrays.asList(args).indexOf(arg)) + arg);
    }
}
