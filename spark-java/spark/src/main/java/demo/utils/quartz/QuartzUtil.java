package demo.utils.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.text.ParseException;
import java.util.Date;

/**
 * 任务定时调度
 * 格式：[*    *   *   *   *   *   *]
 * 格式：[秒  分   时  日  月  周  年]
 * 取值范围及其用法：
 * 秒分：0-59      , - * /
 * 时：0-23       , - * /
 * 日：1-31       , - * ? / L W
 * 月：1-12       , - * /
 * 周：1-7        , - * ? / L #
 * 年：可以不传   , - * /
 * 通配符：
 *
 * @* 表示所有值.
 * @? 表示不指定值
 * @/ 用于递增触发；如在秒上面设置"5/15" 表示从5秒开始，每增15秒触发(5,20,35,50)
 * @, 表示指定多个值；例如在周字段上设置 "MON,WED,FRI" 表示周一，周三和周五触发
 * @- 表示区间；例如 在小时上设置 "10-12",表示 10,11,12点都会触发。
 * @# 序号(表示每月的第几个周几)
 * @W 表示离指定日期的最近那个工作日(周一至周五)
 * @L 表示最后的意思
 */
public class QuartzUtil implements Job {

    public static void main(String[] args) {
        StdSchedulerFactory factory = new StdSchedulerFactory();
        run();
        try {
            Scheduler scheduler = factory.getScheduler();
            checkStarted(scheduler);
            JobDetailImpl job = new JobDetailImpl();
            job.setName("task-01");
            job.setJobClass(QuartzUtil.class);
            CronTriggerImpl trigger = new CronTriggerImpl();
            trigger.setName("执行间隔：5s");
            trigger.setCronExpression("*/5 * * * * ?");
            scheduler.scheduleJob(job, trigger);
            if (!scheduler.isStarted())
                scheduler.start();
            checkStarted(scheduler);
        } catch (SchedulerException | ParseException e) {
            e.printStackTrace();
        }
    }

    private static void checkStarted(Scheduler scheduler) throws SchedulerException {
        boolean shutdown = scheduler.isShutdown();
        boolean started = scheduler.isStarted();
        System.out.println("shutdown: " + shutdown + ", started: " + started);
    }

    /**
     * 定时执行内容
     */
    private static void run() {
        System.out.println(new Date().toLocaleString() + " test scheduler ...");
    }

    @Override
    public void execute(JobExecutionContext context) {
        QuartzUtil.run();
    }
}
