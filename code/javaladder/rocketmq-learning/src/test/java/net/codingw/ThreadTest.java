package net.codingw;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadTest {



    class Task implements Runnable {
        private LinkedBlockingQueue taskQueue = new LinkedBlockingQueue();
        private AtomicBoolean running = new AtomicBoolean(true);

        public void submitTask(Object task) throws InterruptedException {
            taskQueue.put(task);
        }

        @Override
        public void run() {
            Thread t;
            while(running.get()) {
                try {
                    Object task = taskQueue.take(); // 如果没有任务，会使线程阻塞，一旦有任务，会被唤醒
                    doSomething(task);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        public void shutdown() {
            if(running.compareAndSet(true, false)) {
                System.out.println(Thread.currentThread() + " is stoped");
            }
        }

        private void doSomething(Object task) {
        }
    }






    class Task1 implements Runnable {
        @Override
        public void run() {
            while(true) {
                if( shouldRun() ) {// 符合业务规则就运行
                    doSomething();
                } else {
                    try {
                        //休眠1s,继续去判断是否可运行
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
        private void doSomething() {
        }
        private boolean shouldRun() {
            return false;
        }
    }
}
