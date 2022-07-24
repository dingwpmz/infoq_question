package net.codingw.jk02;

import java.util.ArrayList;
import java.util.List;

public class ProduceConsumerModeTest {


    // 面包仓库
    static class Bakery {
        private List<Bread> breads = new ArrayList<>();
        private final int maxCapacity;
        private Bakery(int maxCapacity) {
            this.maxCapacity = maxCapacity;
        }
        public void put(Bread bread) throws InterruptedException {
            synchronized (breads) {//锁定资源
                while (breads.size() == this.maxCapacity) {
                    // 没有可存储的空间，阻塞生产者，等待有存储空间后再继续
                    breads.wait();
                    log();
                }
                breads.add(bread);
                breads.notifyAll();
            }
        }
        public Bread get() throws InterruptedException{
            Bread bread = null;
            synchronized (breads) {
                while (breads.isEmpty()) {
                    breads.wait();
                }
                bread = breads.remove(breads.size() - 1);
                breads.notifyAll();
             }
             return bread;
        }

        private void log() {
            System.out.println("log");
        }

    }




    static class Bread {
        private int breadId;
        public Bread(int breadId) {
            this.breadId = breadId;
        }
        public int getId() {
            return this.breadId;
        }
    }
}
