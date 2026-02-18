# Dubbo-Go 与 Java 对齐改动说明

本文档描述工作区中 **java-align** 相关修改，旨在使 dubbo-go 在元数据、服务发现、Directory 缓存及端口等行为上与 **Dubbo Java 3.x** 保持一致，并增强 Go 与 Java 混用场景下的可观测性。

---

## 一、概述

| 目标     | 说明 |
|----------|------|
| 行为对齐 | URL 缓存 key、元数据命名空间、Nacos 读写格式、Directory 复用、端口选取、首次订阅与 metadata 回退等与 Java 一致 |
| 兼容场景 | Go 消费者调用 Java 提供方；使用 Nacos 作为注册中心与元数据中心 |
| 可观测性 | 增加 `[metadata-diag]`、`[port-diag]`、`[directory-cache]`、`[service-discovery]` 等诊断日志，便于排查 |

涉及模块：`common`、`config`、`metadata`、`metadata/report/nacos`、`registry`、`registry/servicediscovery`。

---

## 二、按模块说明

### 2.1 common/url.go — 缓存 Key 与 Java 一致

**函数**：`GetCacheInvokerMapKey()`

**改动要点**：

- 构建 Directory 的 `cacheInvokerMap` key 时**不再包含 `timestamp`**。
- 与 Java `dubbo-common/url/component/URLParam.equals/hashCode` 行为一致（排除 `TIMESTAMP_KEY`）。
- 效果：同一端点不会因 timestamp 变化被识别为不同 invoker，避免重复通知产生重复 invoker。

**实现**：key 由 `(protocol, ip, port, interface, group, version, meshClusterID)` 组成，不再使用 `urlNew.GetParam(constant.TimestampKey, "")`。

---

### 2.2 config/metadata_config.go — 元数据配置与 Java 对齐

**改动要点**：

1. **命名空间**  
   当使用注册中心作为元数据中心时，元数据上报使用的命名空间固定为 **`"public"`**，不再使用 `rc.Namespace`。与 Java 默认 metadata namespace 一致，确保 Go 消费者能从 Java 提供方写入的同一命名空间读取元数据。

2. **UseAsMetaReport 空值语义**  
   - 当 `reg.UseAsMetaReport` 为**空字符串**时，视为 **`true`**（使用注册中心作为元数据中心）。  
   - 与 Java 行为一致；原先空字符串会导致 `strconv.ParseBool` 报错。

3. **诊断日志**  
   - 创建 metadata report 时输出：registry id、address、registry namespace、以及说明将使用 `namespace=public`。  
   - 创建完成后输出 report 已创建日志。  
   - 日志前缀：`[metadata-diag] Config:`。

---

### 2.3 metadata/options.go — 编程式元数据选项

**改动要点**：

- 与 `config/metadata_config.go` 一致：当从注册中心构建 ReportOptions 时，**命名空间固定为 `"public"`**。
- 注释中说明对齐 Java 及 Nacos 默认 namespace 的用法。

---

### 2.4 metadata/client.go — 元数据获取与空指针防护

**改动要点**：

1. **GetMetadataFromMetadataReport**  
   - 增加 `[metadata-diag]` 日志：report 为 nil 告警；请求的 app、revision；成功/失败。  
   - 对 `GetAppMetadata` 的错误正确返回，不再忽略 `err`。

2. **GetMetadataFromRpc**  
   - 对 `url == nil` 做检查并返回明确错误（实例可能缺少 `dubbo.metadata-service.url-params`）。  
   - 增加 `[metadata-diag]` 日志：连接实例、revision 等。

---

### 2.5 metadata/report/nacos/report.go — Nacos 元数据与 Java 对齐

**GetAppMetadata**：

| 步骤 | 说明 |
|------|------|
| try1 | 与 Java 一致：`dataId=application`，`group=revision`。 |
| try2 | 兼容 dubbo-go 3.1.x：`dataId=application+"::"+revision`，`group=reportGroup`。 |
| try3（新增） | 若仍无数据，尝试 **Java 按接口维度元数据**：通过 HTTP 调用 Nacos 开放 API，按 `dataId={interface}::{group}:provider:{application}`、`group=dubbo` 等格式拉取，解析 Java FullServiceDefinition，组装为 `MetadataInfo`。依赖环境变量 `DUBBO_JAVA_METADATA_INTERFACES`（及可选的 `DUBBO_METADATA_GROUP`）。 |

**PublishAppMetadata**：

- 仅写入 **`(dataId=application, group=revision)`**，与 Java `NacosMetadataReport.publishAppMetadata` 一致。
- **移除** 对 3.1.x 的双写（不再写入 `application+"::"+revision` + reportGroup）。

**结构变更**：

- `nacosMetadataReport` 增加字段 **`reportURL *common.URL`**，用于 Java 按接口回退时的 HTTP 请求（需能解析出 Nacos 地址）。
- 新增方法：`getAppMetadataFromJavaPerInterface`、`nacosConfigGetHTTP`、`parseExportedURLsFromJavaFullServiceDefinition`（从 Java FullServiceDefinition JSON 的 `parameters` 等构建 Dubbo URL）。

**诊断**：关键步骤带 `[metadata-diag]` 日志（dataId、group、成功/失败、回退路径等）。

---

### 2.6 registry/protocol/protocol.go — Directory 复用（一接口一 Directory）

**改动要点**：

1. **directoryCache**  
   - 新增 **`directoryCache *sync.Map`**，按「同一注册中心 + 同一接口」复用同一个 Directory，与 Java “one Reference one RegistryDirectory” 一致。

2. **缓存 Key**  
   - **getRegistryCacheKey**：原有逻辑抽成方法，用于 registry 实例缓存。  
   - **getDirectoryCacheRegistryKey**：对 `registry://` 与 `service-discovery-registry://` 做**归一化**（host:port + namespace），使同一 Nacos 的两种 URL 共用同一 Directory，提高缓存命中率。

3. **Refer 流程**  
   - 先计算 `dirCacheKey = getDirectoryCacheRegistryKey(registryUrl) + "#dir#" + serviceUrl.ServiceKey()`。  
   - 若 `directoryCache` 命中：对已存在的 Directory 执行 `Subscribe(registryUrl.SubURL)`，再 `cluster.Join(dic)` 返回。  
   - 若未命中：新建 Directory 并 **Store** 到 `directoryCache`。  
   - 日志前缀：`[directory-cache]`（hit/store、key、directory 指针、interface）。

---

### 2.7 registry/service_instance.go — 端口与元数据安全

**ToURLs 改动**：

1. **Endpoints 解析**  
   - 仅在 **`d.Metadata != nil`** 且 **`Metadata[ServiceInstanceEndpoints]` 非空**时再 Unmarshal，避免 metadata 为空时逻辑异常。

2. **端口选取（与 Java 对齐）**  
   - 优先使用 **ServiceInfo 的端口**：`service.Port`；若为 0 则使用 `service.URL.Port`。  
   - 否则使用实例端口 **`d.Port`**（Nacos 的 instance.port 可能为应用 HTTP 端口，不适合 Dubbo RPC）。  
   - 日志前缀：`[port-diag]`，输出 interface、instance_port、serviceInfo_port、url_port、chosen_port、source（instance / serviceInfo.Port / serviceInfo.URL.Port）。

---

### 2.8 registry/servicediscovery/service_discovery_registry.go — 首次订阅与实例拉取

**新增**：

- **firstServiceName(services)**：从 services 集合取**第一个**提供方应用名，用于 **GetAppMetadata(dataId=app, group=revision)**，避免误用 consumer 的 application。

**Subscribe**：

- 在首次 **OnEvent(ServiceMappingChangedEvent)** 之后，**显式调用一次 `SubscribeURL(url, notify, services)`**。  
- 原因：与 Java 对齐——当使用 `provided-by` 且 `oldServiceNames==newServiceNames` 时，OnEvent 可能不会触发 SubscribeURL，导致 Directory 从未收到实例；此处补一次首次订阅。

**SubscribeURL**：

- 创建新 listener 时，使用 **providerApp = firstServiceName(services)** 作为 metadata 查找的应用名；若为空才回退到 URL 的 application。  
- 若 **GetInstances** 首次返回 0（如 Nacos 未就绪），进行最多 **5 次、间隔 300ms** 的重试，减少 “No provider available”。  
- 日志前缀：`[service-discovery]`（SubscribeURL 入口、listener 是否 nil、GetInstances 首次/重试、实例数量等）。

---

### 2.9 registry/servicediscovery/service_instances_changed_listener_impl.go — 元数据获取策略与诊断

**GetMetadataInfo 策略**：

- **metadataStorageType == remote**：仅走 **GetMetadataFromMetadataReport**。  
- **非 remote**：  
  - 先 **GetMetadataFromRpc**；  
  - 若失败且已配置 metadata-report，则 **回退到 GetMetadataFromMetadataReport**（与 Java 行为一致）。  
  - 若 report 为 nil 则返回 RPC 的 err。

**诊断日志**：

- **`[metadata-diag]`**：实例 host、app、revision、cache 命中、storageType、GetMetadataFromRpc/GetMetadataFromMetadataReport 成功/失败、fallback、ServiceInfo 的 port/url、listener 构建的 URL 等。  
- **`[port-diag]`**：listener 构建的 URL 中首个 URL 的 serviceKey、数量、首 URL 的 ip/port。  

便于在 Go 与 Java 混用场景下排查元数据与端口问题。

---

## 三、诊断日志前缀速查

| 前缀                  | 用途 |
|-----------------------|------|
| `[metadata-diag]`     | 元数据配置、获取、Nacos 读写、cache、storageType、RPC/report 成功失败、fallback |
| `[port-diag]`         | 端口选取（instance vs ServiceInfo）、listener 构建的 URL |
| `[directory-cache]`   | Directory 缓存命中/存储、key、interface |
| `[service-discovery]` | SubscribeURL 入口、GetInstances 首次/重试、实例数量 |

---

## 四、环境变量（Java 按接口元数据回退）

在 Nacos 元数据 report 使用 **Java 按接口回退** 时，可选：

- **DUBBO_JAVA_METADATA_INTERFACES**：逗号分隔的接口名列表，用于拼 dataId。  
- **DUBBO_METADATA_GROUP**：服务分组，默认 `dubbo`。

---

## 五、修改文件列表

```
common/url.go
config/metadata_config.go
metadata/client.go
metadata/options.go
metadata/report/nacos/report.go
registry/protocol/protocol.go
registry/service_instance.go
registry/servicediscovery/service_discovery_registry.go
registry/servicediscovery/service_instances_changed_listener_impl.go
```

---

## 六、总结

本系列修改在以下方面与 Dubbo Java 3.x 对齐，并增强可观测性：

- **URL/缓存**：cacheInvokerMap key 排除 timestamp。  
- **元数据配置**：命名空间 `"public"`；UseAsMetaReport 空为 true。  
- **Nacos 元数据**：读写格式与 Java 一致；支持 Java 按接口元数据 HTTP 回退。  
- **Directory**：按 registry+interface 复用；归一化 registry key。  
- **端口**：优先 ServiceInfo.Port/URL.Port。  
- **服务发现**：首次订阅补 SubscribeURL；GetInstances 重试；使用 provider 应用名做 metadata 查找。  
- **元数据获取**：RPC 失败时按 Java 行为回退到 metadata report。  
- **诊断**：全链路 metadata/port/directory/service-discovery 日志，便于 Go/Java 混用排查。
