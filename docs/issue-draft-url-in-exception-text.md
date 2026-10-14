# 后续 issue 草案：URL 经异常对象/结构化属性进输出面的残口（`#158+#159` r2 未修部分）

> **已提报**：草案 A → **plaita#160**、草案 B → **plaita#161**（均 `OPEN`，P2）。
> 下面每节正文前的 blockquote 就是**提报时 issue 正文的第一段**（`docs/ISSUE_FILING_GUIDE.md`
> 要求的 `优先级 **Px**` + 字面量 `depends-on: #N`），r2 收尾时两条都没带，r3 补齐并
> 指向已落地的单号。

> 本文件是 **plaita#158+#159 r2** 的交付之一。r2 已关闭的：
> ① 策略异常消息在**构造处**掩码（`plaita/node/http.py::_mask_policy_url`）；
> ② 第三方 HTTP 客户端异常文本在 `NodeException` 消息构造处的清洗
> （`_scrub_url_text`，覆盖 requests 的 `url: /path?query` 与 aiohttp 的
> `url='…'`）；③ 守卫第二腿（异常消息/notes）与定验 MISS 族
> （inline getLogger / `*url`·`*uri` / `getattr`·`.get` / 赋值别名 /
> `record.__dict__`）。
> 下面两条是**刻意不在本轮修**的残口。
>
> **r3 增补（2026-10-15，同一合并轮）**：`_scrub_url_text` 的两处 totality 残口在 r3 关闭
> —— ① 匹配器漏掉**方括号 authority**（`http://[webhook.internal]:8443/…`）导致
> `aiohttp` 的 `InvalidUrlClientError`（`str(e)` 即裸 URL）整段穿过；② 孤立代理字符
> 输入原样穿过、结果不可 UTF-8 编码。见 `plaita/node/http.py` 的
> `_render_absolute_url_ref` / `_mask_bracketed_absolute_url` 与
> `tests/unit/test_issue159_policy_url_exception_sinks.py` 的第 ⑦/⑧ 节。下面两条残口
> 与 r3 无关，状态不变。
>
> **r4 更正（2026-10-15，同一合并轮）**：上面 r3 那句「方括号 authority 整段穿过已关闭」
> **不成立**，是本文件与 `_mask_bracketed_absolute_url` docstring 的**事实错误**，r4 撤回：
> r3 的 `_ABSOLUTE_URL_RE` 把 `, ; ( ) " ' < > { }` 排除在字符类外，匹配在这些分隔符处
> **截断**，而 `re.sub` 只替换匹配片段 ⇒ 尾巴（含 `access_token=`）原样留下；截断恰好落在
> `[` 之后时 `_mask_bracketed_absolute_url("http://[web")` 返回**输入本身** ⇒ 替换是**恒等**
> ⇒ 整条带凭据的 URL 逐字穿过（无策略约束的节点上实测 6 面 × 2 路全泄）。r4 的正面修法：
> token 边界 = **只认 ASCII 空白**、分隔符留在 token 内、渲染结果必须是**规范形态**否则
> 退化为常量、并补一趟「凭据键=值」规则。另：r3 称 `redis://` / `ftp://` 与 r2「逐字一致」
> 也是错的——r2 的字符类不含方括号，这些串**不匹配**因而**原文穿过**；r4 收进方括号后它们
> 才走到 scheme 判据、退化为常量（刻意的 fail-closed，代价是丢掉端点诊断）。
> 细节与矩阵见 `tests/unit/test_issue159_policy_url_exception_sinks.py` 第 ⑨–⑮ 节。

## 草案 A（跨单项）：`exc.details` / `HttpNodeErrorInfo.request` 持原始 URL

> 优先级 **P2**
>
> depends-on: #88

**已提报**：plaita#160（`security(server): NodeException.details.request.url 持请求原文 —— 任何序列化/repr 的消费者即泄露（#158+#159 同族第三条向量）`，`OPEN`）。

### 标题

`#158+#159` 跨单项：`NodeException.details.request.url` 持请求原文，任何序列化/`repr`
它的消费者即泄露

### 复现

```python
from plaita.node.http import HTTP
node = HTTP(id="h", name="h",
            url="http://host/robot/send?access_token=Ks9Qm4Zt", method="GET")
try:
    node.execute({})          # 任意请求失败路径都会挂 details
except Exception as exc:
    detail = exc.details
    assert "Ks9Qm4Zt" in str(detail.request.url)   # 实测：True
    # ⚠️ `HttpNodeErrorInfo` **没有** `__repr__`：`print(repr(detail))` 只会打印
    # `<plaita.node.http.HttpNodeErrorInfo object at 0x…>`，所谓「结构化全文」是错的。
    # 要看原文得逐个取字段：
    print(detail.code)
    print(detail.message)
    print(detail.request.method, detail.request.url, detail.request.headers)
    print(detail.response)    # HttpNodeResponse（status/status_text/headers/data）
```

`wrap_http_node_err`（`plaita/node/http.py`）把 `HttpNodeErrorInfo(message,
request=rsp.raw_request, response=…)` 挂到 `exc.details`；`request.url` 与
`response` 里的头/体是**原文**。仓内消费者状况要说准：**运行时代码 0 处**
（`grep -rn '\.details' plaita/` 只命中 `plaita/node/http.py` 的赋值与 docstring 各一处），
但**测试有读**——`tests/unit/test_clean_c2_http_error_frame.py`（140/141/142/144/151/
152/153/245/246/270/357/358/386/399 行）断言 `exc.details.request.url` 等字段。
所以今天不是活体泄露（没有 logger/序列化面在读）；但事件层/Console/任何
`repr`/`asdict`/JSON 序列化一接上就是新 sink。

### 原则

与 #88/#158/#159/r2 同源：**安全不许依赖「现在没人读」**。结构化异常附件同样是输出面，
要么声明契约、要么给掩码视图。

### 验收标准（建议）

1. `HttpNodeErrorInfo` 增加**掩码视图**（例如 `redacted()` / `request.url_masked`），
   或明确把 `details` 声明为「非日志面」并在 `__repr__` 里掩码；
2. `repr(exc)` / `traceback.format_exception(exc)` / `exc.details` 的常见序列化路径
   都不含密钥本体与任何 ≥3 字符前缀/后缀（复用
   `tests/unit/_url_sink_support.leak_needles`）；
3. 传输路径仍拿原文（`raw_request.url` 逐字节），只有**记录/序列化**面掩码；
4. 至少 1 条变异（还原掩码视图）由普通 `FAILED` 行杀死、`old` 唯一。

---

## 草案 B：其它第三方异常文本 sink（r2 只关了 `node/http.py` 这一条）

> 优先级 **P2**
>
> depends-on: #158

**已提报**：plaita#161（`security(server): alerts/http_callback_service 的 logger.*(…, exc) 仍是第三方异常文本的出口（#158+#159 同族残口）`，`OPEN`）。

r2 修的是 `plaita/node/http.py` 的消息构造（定验的族表即验收清单）。以下位置仍会把
第三方异常文本（requests/aiohttp/urllib）整段交给 `logger.*`，需要在**各自的构造/边界**
处清洗，而不是在 logger 调用点打补丁：

### 标题

`#158+#159` 残口：`alerts.py` / `http_callback_service.py` 等的 `logger.*(…, exc)`
仍是第三方异常文本的出口

### 复现（本地，无外网）

```python
# alerts.py:144 —— 默认 _post_urllib 实测文本不带 URL（urllib 的 URLError 只有 errno），
# 但 PostFn 是可注入的；注入 requests 型 post 时 exc 文本带 `url: /path?query`：
from plaita.server.alerts import WebhookAlerter
import requests
url = "https://oapi.dingtalk.com/robot/send?access_token=Ks9Qm4Zt"
def post(u, body, headers, timeout):
    requests.post(u, data=body, headers=headers, timeout=timeout)  # 连接失败
alerter = WebhookAlerter(url, post=post)
alerter.send({"event": "dead_letter"}); alerter.flush(2.0)
# WARNING 记录的 exc 文本含 access_token=Ks9Qm4Zt（实测 requests ConnectionError 形态）
```

### 原则

同 r2：**构造/边界处清洗**，不在 logger 调用点。`alerts.py` 已掩码 `self.url`，
但 `exc` 是另一条向量；守卫的名字判据同样看不见它。

### 验收标准

1. `alerts.py` 的投递失败 WARNING、`server/nodes/http_callback_node.py` 的
   `logger.warning("…: %s", e)`、`services/http_callback_service.py` 的
   `logger.error(…, e)` 等，`e` 的文本经「自由文本 URL 清洗」后再记录
   （r2 的 `plaita/node/http.py::_scrub_url_text` 可抽成公共渲染器复用）；
2. 动态测试：注入一个带 token 的 requests/aiohttp 异常，断言
   `record.msg`·`args`·`getMessage`·`__dict__`·格式化 `exc_text` 全无 needle；
3. 不回归：这些位置的非 URL 文本（状态码/errno/主机端口）保留；
4. ≥2 条变异（绕过清洗、回退记录面）由普通 `FAILED` 行杀死、`old` 唯一。

---

## r2 的族表结论（供上面两单复用）

| site | URL 怎么进 | sink | r2 状态 |
|------|-----------|------|---------|
| `node/http.py` `_host_allowed`(:384/:387) / `_validate_host_target`(4 处) | 源码 f-string 插 `{url}` | URLPolicyError → NodeException → NodeExecutionError → runner/worker 日志 | **已修**（`_mask_policy_url`） |
| `node/http.py` `handle_http_node_err` → `wrap_http_node_err` 的 `str(err)` | requests `url: /path?query`、aiohttp `url='…'` | 同上 | **已修**（`_scrub_url_text` 在消息构造处） |
| `node/http.py` `wrap_http_node_err` 的 `_ABSOLUTE_URL_RE`（**方括号 authority**） | aiohttp `InvalidUrlClientError`：`str(e)` 就是**裸 URL**（`http://[webhook.internal]:8443/…?access_token=…`） | 同 `str(err)` 链 | **r3 已修**（匹配器接受 `[`/`]`；`_render_absolute_url_ref` 端点兜底保 `host:port`） |
| `node/http.py` `parse_url/query/body/header` 的 `{str(e)}` | `evaluate` 错误文本可能回显 URL 字面量 | 同上 | **已修**（同清洗器） |
| `node/http.py` `exc.details = HttpNodeErrorInfo(request=raw_request)` | 结构化属性持原文 | 默认不打印；序列化即漏 | **草案 A** |
| `alerts.py:144` `logger.warning(…, exc)` | 默认 urllib 文本**实测无 URL**；注入 requests 型 post 时有 | WARNING | **草案 B** |
| `http_callback_node.py:114` INFO `回调URL=%s` | 源码直接插 `config[...]['full_url']` | INFO | **已修**（`_mask_webhook_url_for_log`；守卫 `*url` 族点名后暴露） |
| `services/http_callback_service.py` 等 `logger.*(…, e)` | requests/aiohttp 文本 | ERROR/WARNING | **草案 B** |
| `services/__main__.py:68`、`storage/redis.py`、`services/redis_queue_service.py` 等 | redis-py 异常文本 | ERROR/WARNING | **实测安全**（`ValueError`/`ConnectionError`/`AuthenticationError` 均不含 URL/口令）；kafka/DB 未实测，列入草案 B 验证 |
| `factory.py` `_parse_redis_url` → `RedisUrlError` | 明文 URL | logger/调用方 | **已安全**（#88 起经 `_mask_url`） |
| `dsl/codeflow/*`、`dsl/ir_validate.py` 的 `…Error(…, target)` | `target` 是**节点 id**，不是 URL | 编译期错误 | **不适用**（守卫判据刻意不把 `target` 当 URL） |

补充（r3）：`_scrub_url_text` 的 **totality** 也在 r3 收紧 —— 孤立代理字符
（`surrogateescape` 型输入）不再原样穿过；输出保证 UTF-8 可编码 + 幂等，判不了就退化
`UNPRINTABLE_URL`（与 `_mask_webhook_url` 同一纪律）。
