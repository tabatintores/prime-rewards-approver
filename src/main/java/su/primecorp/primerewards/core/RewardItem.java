package su.primecorp.primerewards.core;

import java.util.Map;
import java.util.Objects;

public final class RewardItem {
    // Обязательные поля для исполнителя
    public final long id;               // PK в источнике (orders.id)
    public final String orderId;        // UUID или строка
    public final String nickname;       // целевой ник
    public final String tier;           // ключ тарифа
    public final double amount;         // числовая сумма (может пригодиться)
    public final String currency;       // валюта (опционально)

    // Дополнительные атрибуты для плейсхолдеров и логов
    public final Map<String, Object> attrs;

    public RewardItem(long id, String orderId, String nickname, String tier, double amount, String currency, Map<String, Object> attrs) {
        this.id = id;
        this.orderId = orderId;
        this.nickname = nickname;
        this.tier = tier;
        this.amount = amount;
        this.currency = currency;
        this.attrs = attrs;
    }

    public String getAttrAsString(String key) {
        Object v = attrs != null ? attrs.get(key) : null;
        return v == null ? "" : Objects.toString(v);
    }
}
