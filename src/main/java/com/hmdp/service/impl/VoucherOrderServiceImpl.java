package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {  // 将读取脚本工作设置在静态代码块，加载类时加载lua脚本，在使用时直接调用，提升效率
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct //当前类初始化之后执行
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    // 获取队列中订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                }

            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 获取用户ID
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁对象
        boolean isLock = lock.tryLock();// 该方法的tryLock有无参、双参、三参，无参表示失败直接返回，不等待重试
        //加锁失败
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {

            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 1.执行Lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(), // 空集合
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        // 保存下单信息到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单ID
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 用户ID
        voucherOrder.setUserId(userId);
        // 代金券ID
        voucherOrder.setVoucherId(voucherId);
        // 放入阻塞队列
        orderTasks.add(voucherOrder);
        // 第一部分抢单已完成，剩余操作为异步下单
        // 获取代理对象
        // 获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 返回订单id
        return Result.ok(orderId);
    }



    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        // 判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过一次!");
            return ;
        }
        // 5.减扣库存
        boolean success = seckillVoucherService.update().
                setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足!");
            return ;
        }
        save(voucherOrder);
    }
}
