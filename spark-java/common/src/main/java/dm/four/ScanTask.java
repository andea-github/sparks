package dm.four;

import lombok.Data;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author admin 2020-8-13
 */
public class ScanTask extends RecursiveTask<Object> {
    static final int THRESHOLD = 1000;
    private List<Integer> list;
    private int start;
    private int end;
    private Integer random;
    private TaskManager manager;

    private int num = 0;

    public ScanTask(List<Integer> list, int start, int end, Integer random, TaskManager manager) {
        this.list = list;
        this.start = start;
        this.end = end;
        this.random = random;
        this.manager = manager;
    }

    public ScanTask(List list) {
        this.list = list;
    }

    public ScanTask(List<Integer> list, Integer random) {
        this.list = list;
        this.random = random;
    }

    public ScanTask(List<Integer> list, Integer random, TaskManager manager) {
        this.list = list;
        this.random = random;
        this.manager = manager;
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AtomicLong count = new AtomicLong();
        int max = 100 * 100 - 1;
        int size = max + 1;
        List<Integer> list = IntStream.rangeClosed(0, max).boxed().collect(Collectors.toList());
        System.out.println("========== size " + list.size());
        IntStream intStream = IntStream.rangeClosed(0, 9);
//        intStream.forEach(x -> System.out.println(x));

        ForkJoinPool pool = new ForkJoinPool();
        Integer random = Integer.valueOf((int) (Math.random() * size));
        System.out.println(random);
        ScanTask task = new ScanTask(list, 0, size, random, new TaskManager());
        ForkJoinTask<Object> submit = pool.submit(task);
        System.out.println("========== submit " + submit.join());
        pool.shutdown();
        /*try {
            TimeUnit days = TimeUnit.DAYS;
            System.out.println("=========  days " + days);
            pool.awaitTermination(1, days);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
//        System.out.println("========== submit " + submit.get());
    }

    @Override
    protected Object compute() {
        String res = "";
        User user = null;
        int result = 0;

//        int size = list.size();
        if ((end - start) <= THRESHOLD) {
            for (int i = start; i < end; i++) {
                if (i == random) {
                    System.out.println("----------- index = " + i);
                    manager.cancelTasks(this);
                    user = new User(i);
                    result = i;
                } else {
                    result = -1;
                }
            }
        } else {
            int middle = (start + end) / 2;
            ScanTask task0 = new ScanTask(list, start, middle, random, manager);
            ScanTask task1 = new ScanTask(list, middle, end, random, manager);
            manager.addTask(task0);
            manager.addTask(task1);
            task0.fork();
            task1.fork();
            User r0 = (User) task0.join();
            User r1 = (User) task1.join();
//            int age0 = r0.getAge();
//            int age1 = r1.getAge();
            user = (r0 != null) ? r0 : r1;
//            int j0 = (int) task0.join();
//            int j1 = (int) task1.join();
//            result = (j0 > j1) ? j0 : j1;
        }
//        System.out.println("------ num = " + num);
//        return Integer.valueOf(result);
        return user;
    }

    public void writeCancelMessage() {
        System.out.printf("Task: Cancelled task size = %d \n", list.size());
    }
}

@Data
class User {
    private String name = "Jack";
    private int age;

    public User(int age) {
        this.age = age;
    }
}