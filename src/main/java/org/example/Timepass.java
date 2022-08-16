package org.example;


import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

class Tom implements Callable<String> {
    @Override
    public String call() throws Exception {

        Thread.sleep(5000);

        return "yo";
    }
}

public class Timepass {

    public static void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }


    public static void main(String[] args) throws Exception {

        print("a");

        FutureTask<String> ft = new FutureTask<>(new Tom());

        Thread t = new Thread(ft);

        t.start();

        print(ft.get());


        ExecutorService es = Executors.newFixedThreadPool(2);

        for(int i=0; i<10; i++) {

            int temp = i;

            es.submit(() -> {
                int val = (int)(Math.random() * 10000);
                try {
                    Thread.sleep(val);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return temp + "|" + val ;
            });
        }



    }


}
