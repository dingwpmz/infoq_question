package net.codingw.jk12;

import org.apache.dubbo.config.*;
import org.apache.dubbo.rpc.service.GenericService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DubboConsumerTest {

    public static void main(String[] args) {

        Map<String, Object> a = new ConcurrentHashMap<>();

    }


    public static GenericService getInvoker(String serviceInterface, String version, List<String> methods, int retry, String registryAddr ) {

        ReferenceConfig referenceConfig = new ReferenceConfig();

        // 关于消费者通用参数，可以从配置文件中获取，本示例取消
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setTimeout(3000);
        consumerConfig.setRetries(2);


        referenceConfig.setConsumer(consumerConfig);

        //应用程序名称
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName("GateWay");
        referenceConfig.setApplication(applicationConfig);

        // 注册中心
        RegistryConfig registry = new RegistryConfig();
        registry.setAddress(registryAddr);
        registry.setProtocol("zookeeper");
        referenceConfig.setRegistry(registry);


        // 设置服务接口名称
        referenceConfig.setInterface(serviceInterface);
        // 设置服务版本
        referenceConfig.setVersion(version);

        referenceConfig.setMethods(new ArrayList<MethodConfig>());
        for(String method : methods) {
            MethodConfig methodConfig = new MethodConfig();
            methodConfig.setName(method);
            referenceConfig.getMethods().add(methodConfig);
        }

        referenceConfig.setGeneric("true");// 开启dubbo的泛化调用

        return (GenericService) referenceConfig.get();
    }


}
