package top.mangod.flinkblog.demo002;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 **/
public class SourceSourceDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据源
        RandomStudentSource randomStudentSource = new RandomStudentSource();
        DataStreamSource<Student> dataStreamSource = env
                .addSource(randomStudentSource);

        // 3. 数据转换
        DataStream<Student> transformedStream = dataStreamSource
                .keyBy(student -> student.name)
                .reduce((a, b) -> new Student(a.name, a.score + b.score));


        transformedStream.print("result =======").setParallelism(1);

        // 5. 启动任务
        env.execute(SourceSourceDemo.class.getSimpleName());
    }

    private static class RandomStudentSource implements SourceFunction<Student> {

        private Random rnd = new Random();
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (isRunning) {
                Student student = new Student();
                student.setName("name-" + rnd.nextInt(5));
                student.setScore(rnd.nextInt(20));
                ctx.collect(student);
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class Student {
        private String name;
        private Integer score;

        public Student() {
        }

        public Student(String name, Integer score) {
            this.name = name;
            this.score = score;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getScore() {
            return score;
        }

        public void setScore(Integer score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", score=" + score +
                    '}';
        }
    }
}
