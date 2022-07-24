package net.codingw.jk02;

import java.util.concurrent.locks.ReentrantLock;

public class ReentratLockTest {

    public static void main(String[] args) {

        ReentrantLock lock = new ReentrantLock();

        for(int i =0; i  <3; i ++) {
            (new Thread(new Task(lock))).start();
        }


    }


    static class Task implements Runnable {

        ReentrantLock lock;

        public Task(ReentrantLock lock) {
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                lock.lock();
                System.out.println(Thread.currentThread().getId() + "执行业务逻辑");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } finally {
                lock.unlock();
            }
        }
    }

}
