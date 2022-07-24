package net.codingw.jk13;

import java.util.concurrent.ThreadLocalRandom;

public class LoadBalaceTest {

//    private static String nextColor(String[] colorArr, int[] weights) {
//
//        int length = colorArr.length;
//
//        int n = 2;
//
//        System.out.println(n >> 1);
//
//    }


    public static String selectColor(String[] colorArr, int[] weightArr) {
        int length = colorArr.length;
        boolean sameWeight = true;
        // The sum of weights
        int totalWeight = 0;
        for (int i = 0; i < length; i++) {
            int weight = weightArr[i];
            // Sum
            totalWeight += weight;
            // save for later use
            if (sameWeight && totalWeight != weight * (i + 1)) {
                sameWeight = false;
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            System.out.println("offset:" + offset);
            // Return a invoker based on the random value.
            for (int i = 0; i < length; i++) {
                if (offset < weightArr[i]) {
                    return colorArr[i];
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return colorArr[ThreadLocalRandom.current().nextInt(length)];
    }

    public static void main(String[] args) {

        String[] colorArr = new String[]{"GREEN","BLUE"};
        int[] weightArr = new int[] {100,50};

        for(int i = 0; i < 20; i ++) {
            System.out.println(selectColor(colorArr, weightArr));
        }

        try {

        } finally {

        }

    }

}
