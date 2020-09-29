package dm.four;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;

/**
 * @author admin 2020-8-13
 */
public class TaskManager {
    private List<ForkJoinTask> tasks;

    /**
     * Constructor of the class. Initializes the list of tasks
     */
    public TaskManager() {
        tasks = new ArrayList<>();
    }

    /**
     * Method to add a new Task in the list
     *
     * @param task The new task
     */
    public void addTask(ForkJoinTask task) {
        tasks.add(task);
    }

    /**
     * Method that cancel all the tasks in the list
     *
     * @param cancelTask
     */
    public void cancelTasks(ForkJoinTask cancelTask) {
        int size = 0;
        for (ForkJoinTask task : tasks) {
            if (!task.isCancelled())
                task.cancel(!task.isCancelled());
//                ((ScanTask) task).writeCancelMessage();
        }
    }
}
