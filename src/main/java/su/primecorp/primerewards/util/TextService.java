package su.primecorp.primerewards.util;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import org.bukkit.command.CommandSender;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TextService {
    private static final MiniMessage MINI = MiniMessage.miniMessage();
    private static final Pattern LEGACY = Pattern.compile("(?i)[&§]([0-9a-fk-or])");
    private static final String[] COLORS = {"black", "dark_blue", "dark_green", "dark_aqua", "dark_red",
            "dark_purple", "gold", "gray", "dark_gray", "blue", "green", "aqua", "red", "light_purple", "yellow", "white"};
    private static final Map<Character, String> DECORATIONS = Map.of(
            'k', "obfuscated", 'l', "bold", 'm', "strikethrough", 'n', "underlined", 'o', "italic", 'r', "reset");
    private final Map<String, String> messages;

    public TextService(Map<String, String> messages) { this.messages = Map.copyOf(messages); }

    public void send(CommandSender sender, String key, String fallback) {
        sender.sendMessage(parse(messages.getOrDefault(key, fallback)));
    }

    public static Component parse(String text) {
        Matcher matcher = LEGACY.matcher(text);
        StringBuilder output = new StringBuilder();
        while (matcher.find()) {
            char code = Character.toLowerCase(matcher.group(1).charAt(0));
            int color = Character.digit(code, 16);
            String replacement = color >= 0 ? "<reset><" + COLORS[color] + ">" : "<" + DECORATIONS.get(code) + ">";
            matcher.appendReplacement(output, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(output);
        return MINI.deserialize(output.toString());
    }
}
