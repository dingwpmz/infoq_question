package net.codingw.jk13;

import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.router.tag.TagRouter;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.rpc.Constants.ACCESS_LOG_KEY;

@Activate(group = CONSUMER, value = "tagConsumerFilter")
public class TagConsumerContextFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(TagConsumerContextFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            String tag = ThreadLocalContext.getTag();
            if(StringUtils.isNotEmpty(tag)) {
                RpcContext.getContext().setAttachment(TagRouter.NAME, tag );
            }
        } catch (Throwable t) {
            logger.warn("Exception in TagConsumerContextFilter of service(" + invoker + " -> " + invocation + ")", t);
        }

        // 调用链传递
        return invoker.invoke(invocation);
    }
}
