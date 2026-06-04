# AGENTS.md — PRIMECORP Minecraft Plugin Development Rules

This file contains persistent development instructions for Codex when working on PRIMECORP Minecraft plugins.

These rules are mandatory unless the user explicitly overrides them for a specific task.

---

## 1. Project context

All projects are Minecraft server plugins for the PRIMECORP ecosystem.

Default target stack:

- Minecraft server: Paper/Purpur 1.21.11
- Java: 21
- Build system: Gradle Java
- Gradle DSL: Kotlin (`build.gradle.kts`)
- IDE used by the developer: IntelliJ IDEA
- Project/developer identity: `primecorp.su`
- Typical integrations:
  - Paper API
  - Purpur server behavior
  - Velocity proxy
  - PlaceholderAPI
  - PacketEvents
  - CMI
  - LuckPerms
  - Vault
  - mcMMO
  - Custom PRIMECORP plugins

Do not assume every project uses every integration. Inspect `build.gradle.kts`, `plugin.yml`, source code, and configs before adding integration code.

---

## 2. Primary goals

When making changes, prioritize:

1. Server safety and TPS stability.
2. Clean, maintainable, production-ready architecture.
3. Protection from abuse, bypasses, dupes, race conditions, and permission mistakes.
4. Small focused patches instead of large rewrites.
5. Compatibility with Paper/Purpur 1.21.11 and Java 21.
6. Clear configuration and safe defaults.
7. Easy review through small diffs.

---

## 3. Hard non-negotiable rules

### 3.1 Main thread rules

The Minecraft main thread must only perform fast Bukkit/Paper API logic.

Never perform the following on the main thread:

- File I/O:
  - `YamlConfiguration.save`
  - `FileConfiguration.save`
  - `Files.write`
  - `Files.readAllBytes`
  - `Files.readString`
  - NBT/playerdata file access
  - large config serialization
- Database operations.
- Network, HTTP, REST API, webhook or external API calls.
- Large YAML/JSON serialization/deserialization.
- Long regex processing.
- Heavy loops over:
  - players
  - entities
  - blocks
  - chunks
  - inventories
  - worlds
  - large maps/lists
- Any operation likely to take more than 5-10 ms.

If an operation might be slow, architect it as:

- Main thread:
  - use Bukkit/Paper API;
  - validate state;
  - capture a small immutable snapshot;
  - set dirty flags;
  - schedule sync follow-up if needed.
- Async thread:
  - file I/O;
  - serialization;
  - DB work;
  - network work;
  - large CPU-only processing.

### 3.2 Async thread rules

Never call Bukkit/Paper API from async threads unless the API explicitly documents that it is safe.

Do not access live Bukkit objects async:

- `Player`
- `World`
- `Location` if it may be tied to live world operations
- `Entity`
- `Block`
- `Inventory`
- `ItemStack` mutation
- `PluginManager`
- scheduler registration that touches Bukkit state
- permissions through Bukkit objects unless known safe and already captured sync

If async code needs player/world data, capture a primitive/immutable snapshot on the main thread first:

- `UUID`
- player name string
- world name string
- block coordinates
- serialized item data prepared safely
- plain DTOs/records

Then process that snapshot async.

### 3.3 Periodic work

Anything that runs periodically once per minute or more often must be async unless it uses only very small Bukkit main-thread operations.

For repeating tasks:

- Avoid scanning all online players every tick.
- Avoid scanning chunks/entities repeatedly.
- Prefer event-driven updates.
- Cache results where possible.
- Use incremental processing if a full scan is unavoidable.
- Add configurable intervals.
- Make expensive tasks cancellable on disable/reload.

### 3.4 Build execution

Do not run Gradle build, Gradle test, Maven build, or other heavy project builds unless the user explicitly asks for it.

The developer normally builds locally in IntelliJ IDEA.

If a task needs verification, explain what should be checked locally instead of running a build.

---

## 4. Storage and persistence

### 4.1 Runtime data

Do not use Bukkit YAML for frequently saved runtime data.

Bad for frequent saves:

- `FileConfiguration.save`
- `YamlConfiguration.save`
- frequent large `config.yml` writes

Preferred runtime formats:

- JSON with a dedicated serializer;
- SQLite/MySQL for structured data;
- custom compact storage where appropriate.

### 4.2 Debounce save pattern

For mutable runtime data, use a debounce save design.

Required concepts:

- `dirty`
- `saveInProgress`
- schedule save after a delay instead of immediate repeated saves
- if data changes while saving, run another save after the current one completes
- avoid multiple simultaneous save tasks for the same data file/table

Correct pattern:

1. Main thread mutates state.
2. Main thread marks `dirty = true`.
3. Main thread schedules or requests a save.
4. Main thread captures immutable snapshot.
5. Async thread serializes and writes snapshot.
6. Async thread completes.
7. Main thread or thread-safe state decides if another save is required.

### 4.3 Atomic file writes

All important file writes must be atomic where possible.

Use this pattern:

1. Serialize to memory or stream to a temporary file:
   - `filename.tmp`
2. Move into final location:
   - `Files.move(tmp, target, ATOMIC_MOVE, REPLACE_EXISTING)`
3. If `ATOMIC_MOVE` is not supported by the filesystem, gracefully fallback to `REPLACE_EXISTING` and log a warning if appropriate.

Do not leave corrupted partial data on crash.

### 4.4 Shutdown saves

On plugin disable:

- cancel repeating tasks;
- stop accepting new async save requests;
- flush pending data safely;
- avoid Bukkit API calls from async callbacks;
- log failures clearly;
- do not block the main thread for a long time unless absolutely necessary and clearly explained.

For large data, prefer graceful bounded waiting with timeout and warnings.

---

## 5. Configuration

### 5.1 Config design

Configs must be readable and safe.

Use:

- clear section names;
- comments where useful;
- safe defaults;
- explicit permissions;
- explicit time units in key names:
  - `delay_ticks`
  - `interval_seconds`
  - `timeout_ms`
- lowercase IDs for internal keys;
- stable IDs for GUI items, actions, rewards, tasks, and modes.

Avoid:

- magic numbers hidden in code;
- hardcoded messages;
- hardcoded permissions unless they are core command permissions;
- ambiguous units like `delay: 5`.

### 5.2 Config loading

Config loading may use Bukkit YAML during plugin startup/reload for normal configuration files.

However:

- do not frequently save config YAML at runtime;
- do not parse huge config files on the main thread during gameplay;
- validate config values;
- clamp unsafe values;
- log warnings for invalid sections;
- never crash the server for a minor config mistake if safe fallback is possible.

### 5.3 Reload behavior

Reload commands must be safe:

- unregister or cancel outdated tasks if needed;
- close or refresh GUI state if needed;
- do not leave duplicate listeners, tasks, packet entities, holograms, bossbars, scoreboards, or cached states;
- avoid memory leaks;
- keep old config if new config fails validation, when practical.

---

## 6. Commands and permissions

### 6.1 Command structure

Prefer a clean command architecture:

- command root class;
- subcommand registry;
- separate subcommand classes for complex commands;
- clear permission checks;
- clear usage messages;
- tab completion where useful.

### 6.2 Permissions

Every administrative or potentially abusable command must have a permission.

Default command behavior:

- players without permission receive a configurable message;
- console-only commands must reject players cleanly;
- player-only commands must reject console cleanly;
- dangerous commands should require confirmation if appropriate.

### 6.3 Velocity/server commands

When dispatching commands from console:

- use `Bukkit.dispatchCommand(Bukkit.getConsoleSender(), "command without slash")`;
- never include `/` in `dispatchCommand`;
- if command belongs to another plugin, add `softdepend` when useful;
- delay dispatch by at least 1 tick after startup when needed so dependencies can finish enabling;
- never dispatch user-provided command strings without validation.

---

## 7. Text, colors, MiniMessage, and legacy codes

All chat, title, actionbar, bossbar, GUI names, and lore should support:

- MiniMessage;
- legacy `&` color codes.

Use a centralized `TextService` or equivalent utility.

### 7.1 Text requirements

Text system should:

- parse MiniMessage;
- support `&` legacy color codes;
- avoid unsafe MiniMessage tags if user input is inserted;
- provide methods for:
  - chat messages;
  - plain text if needed;
  - item display names;
  - lore lines;
  - titles/actionbar.

### 7.2 User input in messages

When inserting user-controlled text into MiniMessage:

- escape tags or treat it as plain text;
- do not allow users to inject MiniMessage tags unless explicitly intended.

---

## 8. GUI and inventory safety

Any plugin GUI must be protected from item movement exploits.

### 8.1 Required GUI protections

For managed GUIs:

- cancel clicks in top inventory unless explicitly allowed;
- cancel shift-click transfers into/out of GUI;
- cancel number key hotbar swaps;
- cancel offhand swaps;
- cancel double-click collection abuse;
- cancel drag events that affect GUI slots;
- cancel creative middle-click where relevant;
- validate clicked inventory and raw slot;
- identify GUI by custom holder or stable session object, not only by title;
- never rely only on inventory title for security.

### 8.2 GUI item meta

For all GUI display names and lore:

- remove italic decoration by default;
- use Adventure components;
- support MiniMessage and `&` codes;
- do not mutate shared `ItemStack` instances unsafely;
- clone item templates when placing into inventories.

### 8.3 GUI actions

GUI click actions must be validated server-side:

- check permission at click time;
- check cooldowns;
- check required state;
- check player is still online and in valid world/mode if relevant;
- never trust that an item in a slot is the original item without session validation.

---

## 9. ItemStack, NBT, and custom items

For custom GUI/runtime items:

- use Adventure display names/lore;
- remove italic in item meta;
- avoid expensive NBT operations on the main thread;
- prefer PersistentDataContainer for plugin-owned tags when appropriate;
- never rely on display name/lore alone for identifying important functional items;
- validate material names and head textures from config.

Custom heads:

- if using base64 textures, cache generated head items/templates;
- do not decode/construct heads repeatedly in hot paths;
- handle invalid base64 gracefully.

---

## 10. Events and listeners

### 10.1 Listener design

Listeners should be small and delegate business logic to services.

Avoid:

- large monolithic listener classes;
- heavy logic directly inside event handlers;
- I/O in event handlers;
- network/DB calls in event handlers.

Correct event handler pattern:

1. Quick validation.
2. Permission/state checks.
3. Capture needed data.
4. Delegate to service.
5. Schedule async work only for non-Bukkit logic.

### 10.2 Event priority

Use event priorities intentionally.

- Do not use `MONITOR` to modify event outcome.
- Respect cancellation where appropriate.
- Avoid uncancelling events from other plugins unless explicitly required and documented.

---

## 11. Scheduler rules

Use Bukkit/Paper scheduler safely.

### 11.1 Sync tasks

Sync tasks may use Bukkit API.

Sync tasks must be short.

### 11.2 Async tasks

Async tasks must not use Bukkit API.

Async tasks may:

- serialize data;
- write files;
- query databases;
- perform HTTP requests;
- calculate CPU-only results on snapshots.

### 11.3 Task lifecycle

Every repeating task must be tracked and cancelled on:

- plugin disable;
- reload if recreated;
- feature shutdown.

Never leave old tasks running after reload.

---

## 12. Database rules

If using database storage:

- never access DB on main thread;
- use a bounded connection pool;
- configure timeouts;
- avoid unbounded queue growth;
- handle reconnects gracefully;
- create schema/migrations safely;
- do not block server startup for long migrations unless explicitly required;
- prefer prepared statements;
- never concatenate user input into SQL.

Recommended pool defaults should be conservative:

- small pool size for Minecraft plugin workloads;
- connection timeout;
- validation timeout;
- clear shutdown on disable.

---

## 13. Network and HTTP rules

Network calls must be async.

Requirements:

- timeouts are mandatory;
- no infinite waits;
- handle non-2xx responses;
- handle rate limits if relevant;
- do not expose secrets in logs;
- do not store API tokens in source code;
- make endpoints/configurable when appropriate.

---

## 14. Security and abuse prevention

Always consider abuse scenarios:

- spam clicking;
- command spam;
- GUI item dupes;
- teleport/world change edge cases;
- logout during async operation;
- permission changes during session;
- vanish/spectator edge cases;
- plugin reload while tasks are running;
- server shutdown mid-save;
- proxy/server transfer failures;
- race conditions between async save and sync state mutation.

Use cooldowns and rate limits for expensive or abusable actions.

Never trust client behavior.

---

## 15. Performance guidelines

### 15.1 Avoid hot-path allocations

In frequent events/tasks:

- avoid building complex components repeatedly;
- cache parsed config values;
- cache templates;
- avoid repeated regex;
- avoid repeated MiniMessage parsing in hot loops if static.

### 15.2 Avoid global scans

Avoid scanning all:

- players;
- entities;
- chunks;
- worlds;
- blocks;
- inventories.

If scanning is necessary:

- make it infrequent;
- split across ticks;
- add limits;
- make it configurable;
- document performance impact.

### 15.3 Logging

Do not spam console.

Use debug flags for verbose logs.

Log:

- startup summary;
- config validation warnings;
- integration availability;
- storage errors;
- critical state transitions.

---

## 16. Integrations

### 16.1 PlaceholderAPI

If PlaceholderAPI is optional:

- use `softdepend`;
- check plugin availability before registering placeholders;
- unregister expansions on disable if needed;
- keep placeholder evaluation fast;
- do not do I/O, DB, or network calls inside placeholder methods;
- cache expensive placeholder values.

### 16.2 Velocity

For Velocity/Bungee messaging:

- use proper plugin messaging channel setup;
- register outgoing/incoming channels where needed;
- validate target server names from config;
- handle player disconnects;
- do not assume transfer always succeeds.

### 16.3 PacketEvents

When using PacketEvents:

- track spawned fake entities/objects per viewer;
- cleanup on:
  - player quit;
  - world change;
  - vanish change if detectable;
  - spectator/gamemode changes;
  - reload;
  - disable;
- avoid packet spam;
- batch or rate-limit updates where possible;
- never leave phantom client-side entities.

### 16.4 CMI and external command systems

When integrating through commands:

- use `softdepend` if command availability matters;
- delay startup execution if needed;
- make command strings configurable only for trusted admins;
- avoid executing user-controlled command strings.

### 16.5 LuckPerms/Vault

When checking permissions/groups:

- prefer official APIs if dependency exists;
- avoid repeated expensive lookups;
- cache cautiously and invalidate on relevant events if needed;
- never assume group data is static.

---

## 17. Paper/Purpur compatibility

Use Paper API where beneficial, but avoid unnecessary NMS.

Avoid direct NMS unless explicitly requested.

If NMS or version-specific code is necessary:

- isolate it behind an interface;
- document version assumptions;
- fail gracefully on unsupported versions.

Use `api-version: '1.21'` in `plugin.yml` unless the existing project requires another value.

---

## 18. Gradle and dependencies

### 18.1 Gradle style

Use Kotlin DSL.

Preferred structure:

```kotlin
plugins {
    java
}

group = "su.primecorp"
version = "1.0.0"

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:1.21.11-R0.1-SNAPSHOT")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}
```

Adapt to the existing project structure.

### 18.2 Dependency rules

- Use `compileOnly` for server-provided plugins/APIs.
- Do not shade large libraries unless necessary.
- If shading is required, relocate packages to avoid conflicts.
- Do not add unnecessary dependencies.
- Explain why a new dependency is needed.

---

## 19. plugin.yml rules

Keep `plugin.yml` accurate.

Include when relevant:

- `name`
- `version`
- `main`
- `api-version`
- `author`
- `commands`
- `permissions`
- `depend`
- `softdepend`

Use `softdepend` for optional integrations.

Use `depend` only when the plugin cannot function at all without the dependency.

---

## 20. Architecture preferences

Prefer clear separation:

- `config`
- `command`
- `listener`
- `service`
- `storage`
- `gui`
- `hook`
- `scheduler`
- `util`

Avoid god classes.

The main plugin class should mostly:

- initialize services;
- register commands/listeners/hooks;
- start schedulers;
- perform shutdown cleanup.

Business logic belongs in services.

---

## 21. Error handling

Handle expected failures gracefully:

- invalid config values;
- missing dependency plugin;
- missing world/server;
- player offline;
- database unavailable;
- file write failure;
- invalid material names;
- invalid base64 heads;
- invalid command arguments.

Do not swallow exceptions silently.

Log meaningful messages without leaking secrets.

---

## 22. Testing and verification

Unless explicitly asked, do not run the build.

When finishing a task, provide:

- changed files;
- summary of changes;
- risks/edge cases;
- local verification steps for IntelliJ IDEA/dev server;
- commands the developer can run locally if needed.

Example local verification steps:

```bash
./gradlew clean build
```

or on Windows:

```powershell
.\gradlew.bat clean build
```

Only suggest commands; do not run them unless explicitly requested.

---

## 23. Git workflow

Prefer small commits/patches.

Before editing, inspect current state.

Do not rewrite unrelated files.

Do not reformat the entire project unless explicitly requested.

Do not add binary files.

Do not commit build outputs:

- `build/`
- `.gradle/`
- `out/`
- generated jars
- IDE caches

Respect `.gitignore`.

---

## 24. Codex task behavior

For every task:

1. Read relevant files first.
2. Understand existing architecture.
3. Make the smallest safe change.
4. Keep code style consistent with the project.
5. Avoid unrelated refactors.
6. Preserve public behavior unless asked to change it.
7. Update config/plugin.yml/docs when needed.
8. Mention any assumptions.

If the request is ambiguous, make a reasonable safe assumption and state it in the final response. Ask a question only if proceeding would likely cause wrong or unsafe changes.

---

## 25. Response format after completing a patch

Final response should include:

- Summary.
- Changed files.
- Important implementation details.
- Thread-safety/TPS notes if relevant.
- What to verify locally.
- Any limitations or assumptions.

Example:

```text
Summary:
- Added /example reload command.
- Added TextService with MiniMessage and legacy & support.
- Registered permission in plugin.yml.

Changed files:
- src/main/java/...
- src/main/resources/plugin.yml
- src/main/resources/config.yml

Safety notes:
- No async Bukkit API calls.
- No file I/O added to repeating tasks.

Verify locally:
- Run .\gradlew.bat clean build in IntelliJ terminal.
- Start dev server and test /example reload.
```

---

## 26. Project-specific section template

When creating a new plugin, add a project-specific section below this line.

Use it to document plugin-specific invariants, commands, GUI layouts, storage rules, integrations, and dangerous edge cases.

---

# Project-specific rules

Add project-specific rules here.

Examples:

- This plugin owns GUI title/session IDs.
- This plugin must never modify worlds outside a configured whitelist.
- This plugin depends on Velocity server names from config.
- This plugin stores runtime data in JSON with atomic writes.
- This plugin must cleanup PacketEvents fake entities on quit/world change/reload/disable.

## Language and localization rules

This is a Russian-language Minecraft server project.

All user-facing text must be written in Russian by default.

User-facing text includes:

- `config.yml` comments;
- comments in default YAML configuration files;
- default messages in configs;
- command responses;
- GUI item display names;
- GUI item lore;
- titles;
- subtitles;
- actionbar messages;
- bossbar text;
- kick messages;
- captcha questions;
- captcha answers when they are visible to players;
- error messages;
- warning messages;
- permission denial messages;
- reload/status messages;
- help messages;
- usage messages;
- admin-facing messages;
- console-facing plugin startup summaries when they are intended for server administrators.

Internal technical code may stay in English:

- package names;
- class names;
- method names;
- variable names;
- enum names;
- config keys;
- permission nodes;
- command names;
- database table names;
- database column names;
- internal IDs;
- storage keys;
- placeholder identifiers.

Default language requirements:

- Write all default config comments in Russian.
- Write all default player/admin messages in Russian.
- Write all default GUI names and lore in Russian.
- Write all default command help/usage text in Russian.
- Do not generate English default messages unless the user explicitly asks for English.
- Keep technical IDs stable and preferably English/lowercase.
- Use MiniMessage and legacy `&` color support for all configurable messages.
- For GUI display names and lore, remove italic decoration by default.

Good default message examples:

```yaml
messages:
  no-permission: "<red>Недостаточно прав."
  player-only: "<red>Эта команда доступна только игроку."
  console-only: "<red>Эта команда доступна только из консоли."
  reload-success: "<green>Конфигурация перезагружена."
  reload-failed: "<red>Не удалось перезагрузить конфигурацию. Подробности в консоли."
  unknown-command: "<red>Неизвестная подкоманда."
  usage: "<gray>Использование: <white>/primeplugin reload"