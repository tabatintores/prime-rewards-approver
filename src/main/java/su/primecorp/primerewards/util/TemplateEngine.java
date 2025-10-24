package su.primecorp.primerewards.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Простейший шаблонизатор под ${key} */
public final class TemplateEngine {
    private static final Pattern P = Pattern.compile("\\$\\{([a-zA-Z0-9_\\-]+)}");

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
