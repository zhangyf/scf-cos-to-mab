# COS 普通桶 → 元数据加速桶 搬运函数

腾讯云国际站 · 东京地域（ap-tokyo）的 SCF 云函数。监听普通 COS 桶 `prod-logs/`
前缀下的上传事件，把对象搬运到同地域元数据加速桶的 `prod-raw-logs/` 下。

```
srcbucket-<appid>/prod-logs/2026/08/20/xxxx
                  └────────┘└──────────────┘
                   剥掉        层级原样保留
dstmab-<appid>/prod-raw-logs/2026/08/20/xxxx
```

## 先读这一段：为什么不是 CopyObject

原始需求是"触发 copyObject"，但这条路走不通。

腾讯云官方文档明确写了：**启用元数据加速能力的存储桶暂不支持
`PUT Object - Copy`、`Upload Part - Copy`、`DELETE Object`**
（见 https://cloud.tencent.com/document/product/436/73685 「注意事项」）。

所以本函数改为 **GetObject 取字节流 → PutObject 写入**。这不只是换个 API：
函数从"发一个 API 调用"变成了"实际搬运字节"，耗时、内存、流量成本完全不同，
大文件还必须走分块。设计上的很多取舍都源于此。

其它相关限制（https://cloud.tencent.com/document/product/436/56971）：

| 限制 | 对本方案的影响 |
|---|---|
| 不支持 `x-cos-meta-*` 自定义头 | 自定义元数据会丢。已确认可接受 |
| 存储桶复制 / COS Batch 不适用 | "用跨桶复制替代"也堵死，只能自己搬 |
| **不支持 DELETE Object** | 写坏的对象删不掉，只能重新 Put 覆盖 |
| list 的 prefix 仅支持目录且不递归 | 本函数不做任何 list，不受影响 |
| MAB 为公测功能 | 需申请白名单（已具备） |

## 文件说明

```
scf-cos-to-mab/
├── mab-mover.zip                ★ 上传这个到 SCF 控制台（540KB）
├── src/index.py                 函数源码
├── pkg39/                       打包用的依赖（Python 3.9 兼容版本）
├── deploy/
│   ├── cam-policy.json          若改用运行角色，这里是最小权限策略
│   ├── env.example.json         环境变量模板（含逐项说明）
│   ├── verify.py                上线前 MAB 行为实测（建议跑）
│   └── test_logic.py            纯逻辑单测，不需要凭证
└── README.md
```

### zip 里包含什么

**SCF 的 Python 运行时不内置 cos-python-sdk-v5**（实测报
`ModuleNotFoundError: No module named 'qcloud_cos'`），所以依赖必须打进包。

zip 内容（109 个文件，压缩后 540KB）：

```
index.py               ← 函数入口，必须在根目录
qcloud_cos/            1.9.44   COS SDK
requests/              2.31.0
urllib3/               1.26.20  ← 版本关键，见下
charset_normalizer/    2.1.1    ← 版本关键，见下
crcmod/                1.7      纯 Python 实现
idna/ certifi/ six.py xmltodict.py
```

### ⚠️ 依赖版本被钉死，不要用 latest

两次部署失败都源于版本问题，所有版本号都是实测确定的：

**1. urllib3 必须 < 2.0**

`urllib3 2.x` 声明支持 `>=3.9`，但代码里用了 Python 3.10+ 的 PEP604 语法：

```python
# urllib3/_base_connection.py
bytes, typing.IO[typing.Any], typing.Iterable[bytes | str], str
                                                    ^^^^^ 3.9 不支持
```

在 3.9 上报 `TypeError: unsupported operand type(s) for |: 'type' and 'type'`。
**版本声明与实际不符，不能信任 pip 的自动解析**，必须钉 `urllib3==1.26.20`。

**2. charset_normalizer 必须是 2.x**

`requests 2.31.0` 把 chardet/charset_normalizer 当**硬依赖**（`requests/compat.py`
里直接 import，两个都缺就 `ModuleNotFoundError`），不像新版是可选的。
而 `charset_normalizer 3.x` 用 mypyc 编译、带平台 `.pyd`，`chardet 7.x` 同样如此。
只有 `charset_normalizer 2.1.1` 是纯 Python 且能满足 requests 2.31.0。

**3. 包内零平台二进制**

`pycryptodome`（Crypto）被刻意排除——它只被 `qcloud_cos/crypto.py` 用（客户端加密场景），
本函数调用路径用不到，而它带 C 扩展。

最终成果：**109 个文件全是纯 Python，无 .pyd / .so / .dll**，
不存在平台兼容问题。

### 已验证

用真实 Python 3.9.25 实测（不是靠版本声明推断）：

```
zip 解压 → import index → 构造 CosS3Client → 解析 event → 映射路径 → 调 handler
结果: PASS | py=3.9.25 | client=CosS3Client
      src=prod-logs/2026/08/20/nginx/access.log.gz
      dst=prod-raw-logs/2026/08/20/nginx/access.log.gz
```

### 重新打包（改了 src/index.py 之后）

```bash
cd scf-cos-to-mab
python - <<'EOF'
import zipfile, os
OUT, SRC = 'mab-mover.zip', 'pkg39'
if os.path.exists(OUT): os.remove(OUT)
EXCLUDE = {'__pycache__', 'tests', 'test', 'bin'}
SKIP = ('.pyc', '.pyo', '.pyd', '.dll', '.exe', '.chm', '.txt')

def add(z, path, arc):
    i = zipfile.ZipInfo(arc)
    i.date_time = (2026, 8, 20, 0, 0, 0)
    i.compress_type = zipfile.ZIP_DEFLATED
    i.external_attr = 0o644 << 16        # Unix 权限位，Linux 侧才可读
    z.writestr(i, open(path, 'rb').read())

with zipfile.ZipFile(OUT, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
    add(z, 'src/index.py', 'index.py')   # 入口必须在根目录
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs
                   if d not in EXCLUDE and not d.endswith(('.dist-info', '.egg-info'))]
        rel = os.path.relpath(root, SRC)
        for f in sorted(files):
            if f.endswith(SKIP): continue
            arc = f if rel == '.' else os.path.join(rel, f).replace(os.sep, '/')
            if arc != 'index.py':
                add(z, os.path.join(root, f), arc)
print('done:', os.path.getsize(OUT), 'bytes')
EOF
```

若 `pkg39/` 丢了，按下面重建。**版本号必须照抄，不要用 latest**：

```bash
pip install --no-compile --no-deps --target pkg39 \
    "cos-python-sdk-v5==1.9.44" \
    "requests==2.31.0" \
    "urllib3==1.26.20" \
    "charset-normalizer==2.1.1" \
    "idna==3.7" \
    "certifi==2024.8.30" \
    "six==1.16.0" \
    "xmltodict==0.13.0"
# crcmod 只发 sdist，必须 --no-binary 才能拿到纯 Python 实现
pip install --no-compile --no-deps --no-binary=:all: --target pkg39 "crcmod==1.7"
```



## 上线步骤

### 0. 先跑单测和实测

```bash
# 纯逻辑单测，不需要凭证和网络（27 项）
python deploy/test_logic.py

# MAB 行为实测，需要凭证。结论直接决定环境变量怎么配
export COS_SECRET_ID=xxx
export COS_SECRET_KEY=xxx
export SRC_BUCKET=srcbucket-1250000000
export DST_BUCKET=dstmab-1250000000
export COS_REGION=ap-tokyo
python deploy/verify.py
```

`verify.py` 逐项验证 7 个文档没保证的行为，输出会直接告诉你该怎么配
（例如"设 VERIFY_CRC=false"）。有 FAIL 项就别上线。

> 注意：`verify.py` 会往 MAB 桶写测试对象，而 **MAB 不支持 DELETE**，
> 脚本结束时会打印残留清单，需手工清理或用生命周期规则处理 `_verify_tmp/` 前缀。

### 1. 控制台创建函数

在 SCF 控制台（**东京地域**）新建函数，从头开始：

| 配置项 | 值 |
|---|---|
| 运行环境 | Python 3.9 |
| 提交方式 | 本地上传 zip 包 → 选 `mab-mover.zip` |
| 执行方法 | `index.main_handler` |
| 内存 | **1024 MB**（不要用默认的 128MB，见下） |
| 超时时间 | 300 秒 |

> **内存不能给 128MB**。代码默认 `SMALL_MB=100`，即 ≤100MB 的对象会整个读进内存再 Put，
> 128MB 的配额装不下（还要扣掉运行时和 SDK 本身的占用），会直接 OOM 被杀。
> 两种配法二选一：
> - 内存给 **1024MB**（推荐，留足余量）
> - 或内存保持小，把 `SMALL_MB` 调到内存的 1/4 以下（如 256MB 内存配 `SMALL_MB=48`），
>   超过阈值的对象会自动走分块上传，内存占用恒定为 `PART_MB`

### 2. 环境变量（含密钥）

函数配置 → 环境变量，最少填这 7 条：

| Key | Value 示例 |
|---|---|
| `SRC_BUCKET` | `srcbucket-1250000000` |
| `DST_BUCKET` | `dstmab-1250000000` |
| `COS_REGION` | `ap-tokyo` |
| `SRC_PREFIX` | `prod-logs/` |
| `DST_PREFIX` | `prod-raw-logs/` |
| `COS_SECRET_ID` | `AKIDxxxxxxxx` |
| `COS_SECRET_KEY` | `xxxxxxxx` |

其余变量见 `deploy/env.example.json`，都有默认值，可先不填。

> **密钥变量名为什么是 `COS_` 开头**：SCF 保留了 `TENCENTCLOUD_` / `QCLOUD_` / `SCF_`
> 这三个前缀，自定义环境变量不允许使用，所以改用 `COS_SECRET_ID` / `COS_SECRET_KEY`。
> 若改用临时密钥，额外加 `COS_SESSION_TOKEN` 即可，代码会自动识别。

如果偏好用运行角色而非长期密钥，`deploy/cam-policy.json` 里给了最小权限策略
（替换 `<uin>` / `<appid>` / 桶名占位符），把角色注入的密钥值填到上面三个
`COS_*` 变量即可。

### 3. 触发器

新建 COS 触发器：

| 配置项 | 值 |
|---|---|
| 存储桶 | 源普通桶（必须与函数同地域） |
| 事件类型 | 全部创建（`cos:ObjectCreated:*`） |
| 前缀过滤 | `prod-logs/` ← **不带开头的斜杠**，见下节 |
| 后缀过滤 | 留空 |

### 4. 异步重试配置

函数配置 → 异步执行 / 重试配置：重试次数 **2**，消息保留时长建议 **2 小时**
（默认 6 小时，脏事件会堆太久）。

### 5. 配置 CLS 告警（不能跳过）

这是本方案唯一的兜底手段。到 CLS 控制台：

- 检索语句：`result:FAILED`
- 建议阈值：5 分钟内出现 > 10 条即告警

## ⚠️ 最容易踩的坑：前缀不能带开头的斜杠

COS 对象键本身**不以 `/` 开头**。所以：

| 位置 | 正确 | 错误 |
|---|---|---|
| 触发器前缀过滤 | `prod-logs/` | ~~`/prod-logs/`~~ |
| `SRC_PREFIX` | `prod-logs/` | ~~`/prod-logs/`~~ |
| `DST_PREFIX` | `prod-raw-logs/` | ~~`/prod-raw-logs/`~~ |

写成 `/prod-logs/` 的后果是：触发器匹配不到任何对象，函数一次都不会被调用，
**没有任何报错、没有一条日志**，极难排查。

代码里的 `_norm_prefix()` 会自动纠正环境变量（去开头斜杠、补尾斜杠），
但**触发器的前缀过滤是在 COS 侧配的，代码管不到**，必须自己填对。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `SRC_BUCKET` | 必填 | 源普通桶，带 `-APPID` |
| `DST_BUCKET` | 必填 | 目标元数据加速桶，带 `-APPID` |
| `COS_REGION` | `ap-tokyo` | 函数与两桶必须同地域 |
| `SRC_PREFIX` | `prod-logs/` | 待剥离前缀 |
| `DST_PREFIX` | `prod-raw-logs/` | 目标前缀 |
| `SMALL_MB` | `100` | ≤ 该值走内存直传，> 该值走分块 |
| `PART_MB` | `16` | 分块大小 |
| `MAX_OBJECT_GB` | `48` | 硬上限，超过判 FAILED |
| `IDEMPOTENT` | `true` | 搬运前 Head 目标做幂等校验 |
| `VERIFY_CRC` | `true` | Put 后比对 crc64 |
| `DECODE_PLUS` | `true` | **文件名含 `+` 号时必须设 false**，见下 |
| `USE_INTERNAL_DOMAIN` | `false` | 源桶内网域名 |
| `DST_USE_INTERNAL_DOMAIN` | `false` | MAB 内网域名，实测通过后再开 |

### 关于 `DECODE_PLUS`

COS 事件里的 key 是 URL 编码的，但官方文档没说用哪种解码方式：

- `true`（默认）：用 `unquote_plus`，会把 `+` 解成空格
- `false`：用 `unquote`，保留 `+` 原样

如果你的日志文件名里含 `+`（如 `access+01.log`），默认配置会把它搬成
`access 01.log`，文件名对不上。**这种情况必须设 `DECODE_PLUS=false`。**

## 工作流程

```
COS 上传事件（异步调用）
   ↓
1. 解析 event：剥掉 /appid/bucketname/ 前缀，URL 解码
2. 映射 dst_key = DST_PREFIX + src_key[len(SRC_PREFIX):]
3. Head 源对象拿 size 和 crc64（不信任 event 里的 size）
4. 幂等：Head 目标，size 一致 → SKIP_EXISTS
5. 搬运
     ≤ SMALL_MB : get_object → read() 全量 bytes → put_object
     > SMALL_MB : Range-Get 逐块 + create/upload_part/complete
6. 校验 Put 响应的 crc64 与源一致
7. 打单行 JSON 日志
```

**为什么小文件走"全量入内存"而不是流式转发**：流式传 file-like 对象时，
SDK 不会自动设 `Content-Length`，requests 会退化成 `Transfer-Encoding: chunked`。
MAB 对 chunked 的支持没有文档保证。全量读进内存后 Put bytes，
`Content-Length` 由 requests 精确推断，这是唯一确定安全的路径。日志小文件为主，
`SMALL_MB=100` 已覆盖绝大多数对象。

**为什么大文件用 `upload_part` 而不是 `upload_part_copy`**：后者 MAB 明确不支持。

## 日志与排查

所有结果打单行 JSON，`result` 只有五个取值：

| result | 含义 | 是否需要关注 |
|---|---|---|
| `OK` | 搬运成功 | — |
| `SKIP_EXISTS` | 目标已存在且一致（幂等命中） | — |
| `SKIP_DIR` | 目录占位对象，无需搬运 | — |
| `RETRY` | 可重试错误，已抛给平台重试 | 同一 key 出现 3 次 = 重试耗尽 |
| `FAILED` | 不可重试，**需人工介入** | 配告警 |

失败日志样例：

```json
{"level":"ERROR","result":"FAILED","requestId":"xxx",
 "srcBucket":"srcbucket-1250000000","srcKey":"prod-logs/2026/08/20/a.log.gz",
 "dstBucket":"dstmab-1250000000","dstKey":"prod-raw-logs/2026/08/20/a.log.gz",
 "size":10485760,"cosCode":"AccessDenied","cosStatus":403,
 "retryable":false,"ts":1787000000}
```

### CLS 检索

```
result:FAILED                                    # 全部失败
result:FAILED AND srcKey:"prod-logs/2026/08/20*" # 某天的失败
result:RETRY                                     # 重试过程
```

### 人工重放

从日志捞出 `srcKey` 列表，直接 invoke（不用手工拼完整的 COS event 结构）：

```bash
tccli scf Invoke --region ap-tokyo --FunctionName mab-mover \
  --ClientContext '{"keys":["prod-logs/2026/08/20/a.log.gz","prod-logs/2026/08/20/b.log.gz"]}'
```

## 已知盲区

**如果 COS 侧根本没投递事件，函数不会被调用，也就没有任何日志——这种丢失无法发现。**

只有 list 对比源桶和目标桶（即定时对账）才能查出来，而对账已按需求砍掉。

实际概率不高（COS 事件通知自身有重试机制），但它客观存在。
如果后续发现两个桶的数据量对不上，把对账函数加回来即可，
主搬运逻辑一行都不用改——`map_key` 和 `move_*` 可以直接复用。

## 上线前风险清单

`verify.py` 会逐项实测：

| # | 风险 | 兜底 |
|---|---|---|
| 1 | MAB 是否返回 `x-cos-hash-crc64ecma` | 拿不到则设 `VERIFY_CRC=false`，退化为只比 size |
| 2 | MAB 内网域名是否可达 | 默认关，走公网域名 |
| 3 | MAB 的 `upload_part` 是否可用 | 不可用则调大 `SMALL_MB`，超限对象判 FAILED |
| 4 | 并发写同一深层目录是否冲突 | 5xx 走平台重试；409 类需降并发 |
| 5 | 含中文 / `+` / 空格的 key | 见 `DECODE_PLUS` 说明 |
| 6 | 目录与同名文件冲突 | 归入不可重试 → FAILED 日志 |
| 7 | 端到端搬运一致性 | FAIL 则不要上线 |

## 运维注意

- **MAB 不支持 DELETE**：CRC 校验失败时代码不尝试删除，只打日志，靠重新 Put 覆盖。
- **SCF 并发配额**：单日十万对象规模下需观察调用曲线和 429 率，必要时提配额。
- **MAB 元数据热点**：同一深层目录单日写入十万对象是否形成热点，是最大未知项。
  建议先用一天真实量灰度验证再全量切。
- **触发器约束**：同地域桶；单函数 ≤10 个 COS 触发器，单桶 ≤10 个；
  同一桶的相同事件 + 前后缀组合只能绑一个函数，前后缀不可重叠。

## 参考文档

- [元数据加速桶操作限制](https://cloud.tencent.com/document/product/436/73685)（不支持 CopyObject 的依据）
- [元数据加速功能概述](https://cloud.tencent.com/document/product/436/56971)（完整限制表）
- [SCF COS 触发器](https://cloud.tencent.com/document/product/583/9707)（event 结构）
- [SCF 异步重试策略](https://cloud.tencent.com/document/product/583/41138)
- [SCF 运行角色与临时密钥](https://intl.cloud.tencent.com/document/product/583/38176?lang=zh)
