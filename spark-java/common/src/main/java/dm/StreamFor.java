package dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author admin 2020-8-10
 */
public class StreamFor {

    public static void main(String[] args) {
        int[] arr = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
        List<Integer> list = Arrays.stream(arr).boxed().collect(Collectors.toList());
        System.out.println(list.size());
        List<Integer> res = new ArrayList<>();
        Stream<Integer> stream = list.parallelStream();
        stream.anyMatch(new Predicate<Integer>() {
            @Override
            public boolean test(Integer integer) {
                res.add(integer);
                return integer == 5;
            }
        });
//        Collections.sort(res);
//        Collections.shuffle(list);
        list.subList(0, 1).forEach(integer -> System.out.println(integer));
//        Optional<Integer> first = filterStream.findFirst();
//        System.out.println(first.equals(Optional.empty()));

//        System.out.println(anyMatch);
        Arrays.stream(arr).anyMatch(value -> {
//            System.out.println(value);
            return value >= 5;
        });
//                .forEach(value -> System.out.println(value));
    }
}
