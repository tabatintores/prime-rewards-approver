package su.primecorp.primerewards.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/** Файловая часть загрузки не обращается к Bukkit и выполняется только async. */
public record ConfigFiles(String main, String telegram, String votes) {
    public static ConfigFiles read(Path directory) throws IOException {
        Files.createDirectories(directory);
        return new ConfigFiles(readOne(directory, "config.yml"), readOne(directory, "tg_rewards.yml"),
                readOne(directory, "votes_rewards.yml"));
    }

    private static String readOne(Path directory, String name) throws IOException {
        Path file = directory.resolve(name);
        if (!Files.exists(file)) {
            Path temporary = Files.createTempFile(directory, name, ".tmp");
            try {
                try (InputStream resource = ConfigFiles.class.getResourceAsStream("/" + name)) {
                    if (resource == null) throw new IOException("В JAR отсутствует конфигурация " + name);
                    Files.copy(resource, temporary, StandardCopyOption.REPLACE_EXISTING);
                }
                try {
                    Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException unsupported) {
                    Files.move(temporary, file);
                } catch (FileAlreadyExistsException exists) {
                    // Уже созданный администратором конфиг оставляем без изменений.
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        }
        if (Files.size(file) > 262144L) {
            throw new IOException("Конфигурация " + name + " превышает допустимые 256 КиБ.");
        }
        return Files.readString(file, StandardCharsets.UTF_8);
    }
}
