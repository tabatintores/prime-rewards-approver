package su.primecorp.primerewards.util;

import org.bukkit.configuration.file.FileConfiguration;

import java.util.List;

public final class SafeConfig {
    private final FileConfiguration cfg;
    public SafeConfig(FileConfiguration cfg) { this.cfg = cfg; }
    public FileConfiguration getConfig() { return cfg; }

    public String getString(String path) { return cfg.getString(path); }
    public String getString(String path, String def) { return cfg.getString(path, def); }
    public int getInt(String path, int def) { return cfg.getInt(path, def); }
    public long getLong(String path, long def) { return cfg.getLong(path, def); }
    public double getDouble(String path, double def) { return cfg.getDouble(path, def); }
    public List<String> getStringList(String path) { return cfg.getStringList(path); }
}
