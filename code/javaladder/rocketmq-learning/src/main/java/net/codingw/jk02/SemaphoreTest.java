package net.codingw.jk02;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SemaphoreTest {

    public static void main(String[] args) throws Exception{

        Semaphore semaphore = new Semaphore(5);

        for(int i = 0; i < 5; i ++) {
            System.out.println(semaphore.tryAcquire(3000, TimeUnit.MILLISECONDS));
        }

    }
}
