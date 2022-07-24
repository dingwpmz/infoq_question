package net.codingw.jk02;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FutureTask {


    static class Rpc2UserCenterTask implements Callable<Object> {
        @Override
        public Object call() throws Exception {
            return sendRpcToUserCenter();
        }
        private Object sendRpcToUserCenter() {
            // 具体业务逻辑省略
            return new Object();
        }
    }

    static class Rpc2OrgCenterTask implements Callable<Object> {
        @Override
        public Object call() throws Exception {
            return sendRpcToOrgCenter();
        }
        private Object sendRpcToOrgCenter() {
            // 具体业务逻辑省略
            return new Object();
        }
    }

    public static void main(String[] args) throws Exception {
        // 生产环境建议使用 new ThreadPoolExecutor方式创建线程池
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        // 发起
        Future<Object> userRpcResultFuture = executorService.submit(new Rpc2UserCenterTask()); //异步执行
        Future<Object> orgRpcResultFuture = executorService.submit(new Rpc2OrgCenterTask());   // 异步执行
        Object userRpcResult = userRpcResultFuture.get(); // 如果任务未执行完成，则该方法会被阻塞，直到处理完成
        Object orgRpcResult = orgRpcResultFuture.get();   // 如果任务未执行完成，则该方法会被阻塞，直到处理完成
        doTask(userRpcResult, orgRpcResult);
    }

    private static void doTask(Object userRpcResult, Object orgRpcResult) {
        // doSomeThing
    }


}
