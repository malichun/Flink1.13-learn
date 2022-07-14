package bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * 支付表
 * @author malichun
 * @create 2022/07/14 0014 22:05
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
