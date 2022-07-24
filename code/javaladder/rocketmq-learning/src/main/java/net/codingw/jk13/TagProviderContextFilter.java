package net.codingw.jk13;

import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.router.tag.TagRouter;

import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;

@Activate(group = PROVIDER, value = "tagProviderFilter")
public class TagProviderContextFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(TagProviderContextFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            String tag = invocation.getAttachment(TagRouter.NAME);
            if(StringUtils.isNotEmpty(tag)) {
                ThreadLocalContext.setTag(tag);
            }
        } catch (Throwable t) {
            logger.warn("Exception in TagProviderContextFilter of service(" + invoker + " -> " + invocation + ")", t);
        }
        // 调用链传递
        return invoker.invoke(invocation);
    }
}
