package top.mangod.flinkblog.demo005;

import lombok.Data;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/5/10 19:47
 **/
@Data
public class OrderBO {
    /**
     * ID
     */
    private Integer id;
    /***
     * 订单标题
     */
    private String title;
    /**
     * 订单金额
     */
    private Integer amount;
    /**
     * 订单状态：1-已支付，2-已退款
     */
    private Integer state;
    /**
     * 用户ID
     */
    private String userId;
}
