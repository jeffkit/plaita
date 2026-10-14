# 后续 issue 草案：URL 经异常对象/结构化属性进输出面的残口（`#158+#159` r2 未修部分）

> 本文件是 **plaita#158+#159 r2** 的交付之一。r2 已关闭的：
> ① 策略异常消息在**构造处**掩码（`plaita/node/http.py::_mask_policy_url`）；
> ② 第三方 HTTP 客户端异常文本在 `NodeException` 消息构造处的清洗
> （`_scrub_url_text`，覆盖 requests 的 `url: /path?query` 与 aiohttp 的
> `url='…'`）；③ 守卫第二腿（异常消息/notes）与定验 MISS 族
> （inline getLogger / `*url`·`*uri` / `getattr`·`.get` / 赋值别名 /
> `record.__dict__`）。
> 下面两条是**刻意不在本轮修**的残口。

## 草案 A（跨单项）：`exc.details` / `HttpNodeErrorInfo.request` 持原始 URL

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
    print(repr(detail))                            # 结构化 request/response 全文
```

`wrap_http_node_err`（`plaita/node/http.py`）把 `HttpNodeErrorInfo(message,
request=rsp.raw_request, response=…)` 挂到 `exc.details`；`request.url` 与
`response` 里的头/体是**原文**。今天仓内无消费者（`grep '\.details'` 只有赋值），
所以不是活体泄露；但事件层/Console/任何 `repr`/`asdict`/JSON 序列化一接上就是新 sink。

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
| `node/http.py` `parse_url/query/body/header` 的 `{str(e)}` | `evaluate` 错误文本可能回显 URL 字面量 | 同上 | **已修**（同清洗器） |
| `node/http.py` `exc.details = HttpNodeErrorInfo(request=raw_request)` | 结构化属性持原文 | 默认不打印；序列化即漏 | **草案 A** |
| `alerts.py:144` `logger.warning(…, exc)` | 默认 urllib 文本**实测无 URL**；注入 requests 型 post 时有 | WARNING | **草案 B** |
| `http_callback_node.py:114` INFO `回调URL=%s` | 源码直接插 `config[...]['full_url']` | INFO | **已修**（`_mask_webhook_url_for_log`；守卫 `*url` 族点名后暴露） |
| `services/http_callback_service.py` 等 `logger.*(…, e)` | requests/aiohttp 文本 | ERROR/WARNING | **草案 B** |
| `services/__main__.py:68`、`storage/redis.py`、`services/redis_queue_service.py` 等 | redis-py 异常文本 | ERROR/WARNING | **实测安全**（`ValueError`/`ConnectionError`/`AuthenticationError` 均不含 URL/口令）；kafka/DB 未实测，列入草案 B 验证 |
| `factory.py` `_parse_redis_url` → `RedisUrlError` | 明文 URL | logger/调用方 | **已安全**（#88 起经 `_mask_url`） |
| `dsl/codeflow/*`、`dsl/ir_validate.py` 的 `…Error(…, target)` | `target` 是**节点 id**，不是 URL | 编译期错误 | **不适用**（守卫判据刻意不把 `target` 当 URL） |
