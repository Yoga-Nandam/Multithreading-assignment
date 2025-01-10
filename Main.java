package org.example.assignment;

import java.util.UUID;
import java.util.concurrent.*;

public class Main {
    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @param <T>  Task computation result type.
         * @return Future for the task's asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param <T> Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     */
    public record TaskGroup(UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("groupUUID must not be null");
            }
        }
    }

    public  static class ThreadPoolTaskExecutorTest implements TaskExecutor{
        private final ConcurrentHashMap<TaskGroup, Semaphore> groupLocks;
        private final ExecutorService executorService;

        public ThreadPoolTaskExecutorTest( int poolSize ) {
            this.groupLocks = new ConcurrentHashMap<>();
            this.executorService = Executors.newFixedThreadPool(poolSize);
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            if (task == null) {
                throw new IllegalArgumentException("Task must not be null");
            }
            return executorService.submit(() -> {
                Semaphore semaphore = groupLocks.computeIfAbsent(task.taskGroup, group -> new Semaphore(1));
                try {
                    semaphore.acquire();
                    Thread.sleep(1000);
                    return task.taskAction().call();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }finally {
                    semaphore.release();
                }

            });
        }

        public void shutdown() {
            executorService.shutdown();
        }
    }

    public static void main(String[] args) {
        ThreadPoolTaskExecutorTest executor = new ThreadPoolTaskExecutorTest(4);

        // Create a TaskGroup
        TaskGroup taskGroup = new TaskGroup(UUID.randomUUID());

        // Create and submit tasks
        Task<String> task1 = new Task<>(
                UUID.randomUUID(),
                new TaskGroup(UUID.randomUUID()), // concurrent execution due to different group
                TaskType.READ,
                () -> "Task 1 completed"
        );

        Task<Integer> task2 = new Task<>(
                UUID.randomUUID(),
                new TaskGroup(UUID.randomUUID()),  // concurrent execution due to different group
                TaskType.WRITE,
                () -> {
                   // Thread.sleep(1000); // Simulate some computation
                    return 42;
                }
        );

        Task<String> task3 = new Task<>(
                UUID.randomUUID(),
                taskGroup,  
                TaskType.READ,
                () -> "Task 3 completed"
        );
        Task<String> task4 = new Task<>(
                UUID.randomUUID(),   // non-concurrent execution due to same group
                taskGroup,
                TaskType.READ,
                () -> "Task 4 completed"
        );
        Task<String> task5 = new Task<>(
                UUID.randomUUID(),
                taskGroup,            // non-concurrent execution due to same group
                TaskType.READ,
                () -> "Task 5 completed"
        );



        try {
            Future<String> result1 = executor.submitTask(task1);
            Future<Integer> result2 = executor.submitTask(task2);
            Future<String> result3 = executor.submitTask(task3);
            Future<String> result4 = executor.submitTask(task3);
            Future<String> result5 = executor.submitTask(task3);
            System.out.println("Result of Task 1: " + result1.get());
            System.out.println("Result of Task 2: " + result2.get());
            System.out.println("Result of Task 3: " + result3.get());
            System.out.println("Result of Task 4: " + result4.get());
            System.out.println("Result of Task 5: " + result5.get());


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }finally {
            executor.shutdown();
        }

    }
}
