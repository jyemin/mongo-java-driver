# Native Library Packaging Strategy

## Overview

The MongoDB Java driver ships a Rust core library as a native binary (.so/.dylib/.dll). This document describes how we package and load these libraries across platforms.

## Supported Platforms

| Platform | Target Triple | Library Name |
|----------|---------------|--------------|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | `libmongocore.so` |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | `libmongocore.so` |
| macOS x86_64 | `x86_64-apple-darwin` | `libmongocore.dylib` |
| macOS ARM64 | `aarch64-apple-darwin` | `libmongocore.dylib` |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `mongocore.dll` |
| Windows ARM64 | `aarch64-pc-windows-msvc` | `mongocore.dll` |

## Size Estimates

- Per-platform library (release, stripped): ~8 MB
- All platforms combined: ~48 MB
- With JAR compression: ~30-35 MB

## Packaging Strategy: Hybrid Approach

### Default: Fat JAR (all platforms)

Most users get a single dependency that works everywhere:

```xml
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
    <version>5.0.0</version>
</dependency>
```

The driver transitively pulls in `mongodb-driver-native-all` which contains all platform binaries.

### Optional: Platform-Specific JARs

Size-conscious users can exclude the fat native JAR and include only their target platform:

```xml
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
    <version>5.0.0</version>
    <exclusions>
        <exclusion>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-native-all</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-native</artifactId>
    <version>5.0.0</version>
    <classifier>linux-x86_64</classifier>
</dependency>
```

### Precedent: Netty's Native Packaging

Netty uses this exact pattern for their native TLS (tcnative) and transport (epoll/kqueue) libraries. Here's their actual artifact structure:

**netty-tcnative-boringssl-static (TLS/SSL):**

| JAR | Size | Contents |
|-----|------|----------|
| Base (no classifier) | **3 KB** | POM-only, declares deps on all platform JARs |
| `:linux-x86_64` | 1.4 MB | Linux x86_64 native |
| `:linux-aarch_64` | 1.3 MB | Linux ARM64 native |
| `:osx-x86_64` | 1.2 MB | macOS Intel native |
| `:osx-aarch_64` | 1.1 MB | macOS ARM64 native |
| `:windows-x86_64` | 1.1 MB | Windows x86_64 native |
| **Total** | **~6.1 MB** | All platforms combined |

**netty-transport-native-epoll/kqueue (I/O):**

| JAR | Size |
|-----|------|
| `netty-transport-native-epoll:linux-x86_64` | 40 KB |
| `netty-transport-native-kqueue:osx-x86_64` | 25 KB |

**Key insight:** Netty's "uber JAR" (base artifact with no classifier) is **not a fat JAR** - it's a tiny POM-only artifact that declares all platform-specific JARs as transitive dependencies:

```xml
<!-- Inside netty-tcnative-boringssl-static's POM -->
<dependencies>
  <dependency>
    <artifactId>netty-tcnative-boringssl-static</artifactId>
    <classifier>linux-x86_64</classifier>
  </dependency>
  <dependency>
    <artifactId>netty-tcnative-boringssl-static</artifactId>
    <classifier>linux-aarch_64</classifier>
  </dependency>
  <!-- ... etc for all platforms -->
</dependencies>
```

When you add the base dependency, Maven transitively pulls **all platform JARs**. At runtime, Netty detects your platform and loads the right one.

### MongoDB Maven Artifacts

Following Netty's pattern:

| Artifact | Contents | Size |
|----------|----------|------|
| `mongodb-driver-native` | POM-only, deps on all platforms | ~3 KB |
| `mongodb-driver-native:linux-x86_64` | Linux x86_64 only | ~8 MB |
| `mongodb-driver-native:linux-aarch64` | Linux ARM64 only | ~8 MB |
| `mongodb-driver-native:osx-x86_64` | macOS Intel only | ~8 MB |
| `mongodb-driver-native:osx-aarch64` | macOS ARM64 only | ~8 MB |
| `mongodb-driver-native:windows-x86_64` | Windows x86_64 only | ~8 MB |
| `mongodb-driver-native:windows-aarch64` | Windows ARM64 only | ~8 MB |
| **Total (all platforms)** | | **~48 MB** |

**Usage:**
```xml
<!-- Simple: pulls all platforms (~48 MB total) -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-native</artifactId>
    <version>5.0.0</version>
</dependency>

<!-- Optimized: single platform (~8 MB) -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-native</artifactId>
    <version>5.0.0</version>
    <classifier>linux-x86_64</classifier>
</dependency>
```

## JAR Structure

Native libraries are packaged at:

```
META-INF/native/
  linux-x86_64/
    libmongocore.so
  linux-aarch64/
    libmongocore.so
  osx-x86_64/
    libmongocore.dylib
  osx-aarch64/
    libmongocore.dylib
  windows-x86_64/
    mongocore.dll
  windows-aarch64/
    mongocore.dll
```

## Runtime Loading

### Platform Detection

```java
public class NativePlatform {
    public static String getOsName() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("linux")) return "linux";
        if (os.contains("mac") || os.contains("darwin")) return "osx";
        if (os.contains("windows")) return "windows";
        throw new UnsupportedOperationException("Unsupported OS: " + os);
    }
    
    public static String getArchName() {
        String arch = System.getProperty("os.arch").toLowerCase();
        if (arch.equals("amd64") || arch.equals("x86_64")) return "x86_64";
        if (arch.equals("aarch64") || arch.equals("arm64")) return "aarch64";
        throw new UnsupportedOperationException("Unsupported arch: " + arch);
    }
    
    public static String getPlatformDir() {
        return getOsName() + "-" + getArchName();
    }
    
    public static String getLibraryName() {
        String os = getOsName();
        if (os.equals("windows")) return "mongocore.dll";
        if (os.equals("osx")) return "libmongocore.dylib";
        return "libmongocore.so";
    }
}
```

### Extraction and Loading

```java
public class NativeLibraryLoader {
    private static final String RESOURCE_PREFIX = "META-INF/native/";
    private static volatile boolean loaded = false;
    
    public static synchronized void load() {
        if (loaded) return;

        String platform = NativePlatform.getPlatformDir();
        String libName = NativePlatform.getLibraryName();
        String resourcePath = RESOURCE_PREFIX + platform + "/" + libName;

        // Try system library path first
        try {
            System.loadLibrary("mongocore");
            loaded = true;
            return;
        } catch (UnsatisfiedLinkError e) {
            // Fall through to extraction
        }

        // Extract from JAR
        URL resource = NativeLibraryLoader.class.getClassLoader().getResource(resourcePath);
        if (resource == null) {
            throw new UnsatisfiedLinkError(
                "Native library not found for platform: " + platform +
                ". Add mongodb-driver-native:" + platform + " to your dependencies.");
        }

        try {
            Path tempDir = Files.createTempDirectory("mongodb-native");
            Path tempLib = tempDir.resolve(libName);

            try (InputStream in = resource.openStream()) {
                Files.copy(in, tempLib, StandardCopyOption.REPLACE_EXISTING);
            }

            System.load(tempLib.toAbsolutePath().toString());

            // Clean up on JVM exit
            tempLib.toFile().deleteOnExit();
            tempDir.toFile().deleteOnExit();

            loaded = true;
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("Failed to extract native library: " + e.getMessage());
        }
    }
}
```

## libmongocrypt Consideration

### Background

The Rust MongoDB driver depends on `libmongocrypt` for Client-Side Field Level Encryption (CSFLE) and Queryable Encryption. Currently:

- **Java side**: The `mongodb-crypt` module bundles `libmongocrypt.so/dylib/dll` and uses JNA to call it
- **Rust side**: The `mongocrypt-sys` crate dynamically links to `libmongocrypt` at runtime

With the Rust-based driver, the `mongodb-crypt` Java module becomes dead code, but the Rust core still needs `libmongocrypt`.

### The Trade-off

**Static linking** (libmongocrypt baked into libmongocore):
- ✅ Single binary, simple deployment
- ✅ No runtime dependency resolution
- ❌ Everyone pays the size cost (~2-3 MB extra)
- ❌ CSFLE is an enterprise feature most users don't need

### Options

#### Option A: Static Linking (Simple, Larger)

Statically link libmongocrypt into libmongocore. Single binary per platform.

```
libmongocore.so (~10-12 MB) = Rust driver + libmongocrypt
```

**Build changes:**
1. Build libmongocrypt as static library (`.a`)
2. Modify mongocrypt-sys to link statically: `cargo:rustc-link-lib=static=mongocrypt`
3. Set `MONGOCRYPT_LIB_DIR` during build

**User experience:**
```xml
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
</dependency>
<!-- Just works, CSFLE included -->
```

#### Option B: Two Native Binaries (Flexible, Complex)

Ship two variants of the native library per platform.

```
libmongocore.so (~8 MB)       = Rust driver only
libmongocore-csfle.so (~11 MB) = Rust driver + libmongocrypt
```

**User experience:**
```xml
<!-- Without CSFLE (default, smaller) -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-native</artifactId>
    <classifier>linux-x86_64</classifier>
</dependency>

<!-- With CSFLE -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-native</artifactId>
    <classifier>linux-x86_64-csfle</classifier>
</dependency>
```

#### Option C: Feature-Gated Top-Level Artifacts (Clean UX)

Two top-level driver artifacts that transitively pull the right native library.

```xml
<!-- Without CSFLE -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
</dependency>

<!-- With CSFLE -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync-csfle</artifactId>
</dependency>
```

#### Option D: Runtime dlopen (Cleanest, Most Work)

Ship libmongocore without libmongocrypt linked. Load libmongocrypt dynamically at runtime only when CSFLE is initialized.

```
libmongocore.so (~8 MB)    - always loaded
libmongocrypt.so (~3 MB)   - loaded on demand if CSFLE used
```

**Requires:**
- Changes to Rust driver to use `dlopen`/`dlsym` instead of link-time binding
- Upstream changes to `mongocrypt-sys` crate

**User experience:**
```xml
<!-- Base driver -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
</dependency>

<!-- Add only if using CSFLE -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-csfle</artifactId>
</dependency>
```

#### Option E: Keep Existing mongodb-crypt (Incremental)

Don't include CSFLE in the Rust core initially. Keep the existing `mongodb-crypt` JNA module working for CSFLE while shipping the Rust core for all other operations.

- Rust core handles: CRUD, aggregation, transactions, etc.
- JNA path handles: CSFLE/Queryable Encryption (existing code)

**Pros:** Incremental migration, less risk
**Cons:** Two native loading mechanisms, eventually need to unify

### Comparison

| Option | Binary Size | User Complexity | Build Complexity | CSFLE Cost |
|--------|-------------|-----------------|------------------|------------|
| A: Static | ~11 MB | Lowest | Medium | Everyone pays |
| B: Two binaries | ~8 MB or ~11 MB | Medium | High | Only CSFLE users |
| C: Feature JARs | ~8 MB or ~11 MB | Low | High | Only CSFLE users |
| D: Runtime dlopen | ~8 MB + ~3 MB | Low | Highest | Only CSFLE users |
| E: Keep JNA | ~8 MB + existing | Low | Low | Only CSFLE users |

### Recommendation

**TBD** - Decision depends on:
- How important is binary size optimization?
- How much complexity is acceptable in the build pipeline?
- Is upstream mongocrypt-sys modification feasible?
- Timeline constraints?
```

