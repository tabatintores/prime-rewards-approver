package su.primecorp.primerewards.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Простейший шаблонизатор под ${key} */
public final class TemplateEngine {
    private static final Pattern P = Pattern.compile("\\$\\{([a-zA-Z0-9_\\-]+)}");

    public static String applyStrict(String tpl, Map<String, String> ctx) {
        Matcher matcher = P.matcher(tpl);
        while (matcher.find()) {
            if (!ctx.containsKey(matcher.group(1))) {
                throw new IllegalArgumentException("Неизвестный плейсхолдер команды: " + matcher.group(1));
            }
            if (ctx.get(matcher.group(1)) == null) {
                throw new IllegalArgumentException("В заказе не сохранено значение плейсхолдера: " + matcher.group(1));
            }
        }
        String result = apply(tpl, ctx);
        if (result.contains("$" + "{")) throw new IllegalArgumentException("Некорректный плейсхолдер команды.");
        return result;
    }

    public static String apply(String tpl, Map<String, String> ctx) {
        if (tpl == null || ctx == null) return tpl;
        Matcher m = P.matcher(tpl);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            String val = ctx.getOrDefault(key, "");
            // экранирование потенциальных «опасных» символов в никнейме и др. под команды (минимум)
            val = val.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
            m.appendReplacement(sb, Matcher.quoteReplacement(val));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
