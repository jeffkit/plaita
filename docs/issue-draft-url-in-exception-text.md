# 后续 issue 草案：第三方异常文本里的请求 URL 会经异常对象进日志 sink

> 本文件是 **plaita#158+#159 r2**（`757f04e9` 之后的本地修订）刻意**未在本轮修**的
> 同族出口的 issue 草案。r2 已把「配置 URL 在**构造消息时**被拼进策略异常」这条
> 路径结构性关闭（`plaita/node/http.py::_mask_policy_url` + 守卫第二腿 +
> 动态五面测试）；下面这类 URL **不是 plaita 拼进去的**，而是第三方 HTTP 客户端
> 把请求 URL 放进了自己的异常文本，plaita 只是 `str(err)` 转手。

## 标题

`#158+#159` 残口：`requests`/`aiohttp` 异常文本带请求 URL，`str(err)` 转手后经
`NodeException`/`NodeExecutionError` 进 runner/worker 日志

## 复现（本地，无外网）

```python
from plaita.node.http import HttpExecutor
SECRET = "Ks9Qm4Zt"
url = f"http://127.0.0.1:<无监听端口>/robot/send?access_token={SECRET}"
ex = HttpExecutor(url=url, method="GET", query=None, body=None,
                  headers=None, addressing=None, delegate=None)
rsp, err = ex.handle_request({})
assert SECRET in str(err)          # 实测：True
# err = requests.exceptions.ConnectionError
# "HTTPConnectionPool(host='127.0.0.1', port=…): Max retries exceeded with
#  url: /robot/send?access_token=Ks9Qm4Zt (Caused by NewConnectionError(...))"
node_err = ex_node.handle_http_node_err(err, rsp)   # NodeException(code, str(err))
assert SECRET in str(node_err)     # 实测：True —— 明文已进异常对象
# 之后 core.runner._handle_node_error(..., CONTINUE) 的 logger.warning(…, e)、
# 默认 abort 的 NodeExecutionError、flow_worker 的 logger.warning(…, exc)
# 与 logger.error(…, exc_info=True) 全部带明文。
```

异步侧（aiohttp，实测同样带明文）：

```python
aiohttp.ClientResponseError(request_info=RequestInfo(
    URL(f"http://h/robot/send?access_token={SECRET}"), URL("http://h"), ()),
    history=(), status=500, message="boom")
# str(err) == "500, message='boom', url='http://h/robot/send?access_token=Ks9Qm4Zt'"
```

## 原则（与 #88 / #158 / #159 / r2 同源）

**凭据不得进任何输出面**，且安全不许依赖输入是否合法。r2 证明「在构造消息时
掩码」能关闭 plaita 自己拼 URL 的那一半；本单要处理的是剩下的一半：URL 由
**第三方库的异常文本**携带，plaita 在 `str(err)` 处转手。修法不能退化成
「只在 logger 调用点掩码」——那正是 r1 守卫的盲区（异常对象里的明文，
「logger 实参名」判据结构性看不见）。

## 枚举（r2 扫描；site → URL 怎么进 → 哪个 sink 打印 → 状态）

| # | 位置 | URL 怎么进 | sink | r2 状态 |
|---|------|-----------|------|---------|
| 1 | `plaita/node/http.py` `_host_allowed`(:384/:387) / `_validate_host_target`(4 处 raise) | 源码 f-string 插 `{url}` | `URLPolicyError` → `NodeException` → `NodeExecutionError` → `runner.py:149` / `flow_worker.py:4205,4212` / `exc_info=True` / `traceback` | **已修**（`_mask_policy_url`） |
| 2 | `plaita/node/http.py` `handle_http_node_err` → `wrap_http_node_err` 的 `str(err)` | `requests` `ConnectionError` 文本 `url: /path?query`；`aiohttp` `ClientResponseError` 文本 `url='…'` | 同 #1 的 runner/worker sink | **本单（待修）** |
| 3 | `plaita/node/http.py` `parse_url`/`parse_query`/`parse_body`/`parse_header` 的 `raise Exception(f"…: {str(e)}")` | `evaluate(self.url, ctx)` 的错误文本可能回显 URL 字面量 | 同 #1 | **本单（待修）** |
| 4 | `plaita/node/http.py` `NodeException.details = HttpNodeErrorInfo(request=raw_request)` | URL 作为**结构化属性**挂在异常上（`exc.details.request.url` 是全文） | 默认不打印；任何 `repr(exc.details)` / 序列化它的调用方会漏 | **本单（待定：约定 details 为「非日志面」或提供掩码视图）** |
| 5 | `plaita/server/alerts.py:144` `logger.warning("… url=%s: %s", _mask_webhook_url_for_log(self.url), exc)` | `exc` 来自 `self._post`；**默认 `_post_urllib` 实测文本不带 URL**（`<urlopen error [Errno 61] Connection refused>`），注入 requests 型 `post` 时带 `url: /path?query` | WARNING | **本单（待修：对 `exc` 文本做 URL 掩码，或约束 `PostFn` 契约）** |
| 6 | `plaita/server/nodes/http_callback_node.py` / `services/http_callback_service.py` 的 `logger.*(…, e)` | 同上，requests/aiohttp 文本 | ERROR/WARNING | **本单（待修，同 #2 的文本掩码）** |
| 7 | `plaita/server/services/__main__.py:68`、`storage/redis.py`、`services/redis_queue_service.py` 等的 `logger.*(…, e)` | redis-py 异常文本 | ERROR/WARNING | **实测安全**（`ValueError: Port could not be cast…`、`ConnectionError: Error 8 connecting to <host>:<port>`、`AuthenticationError` 均不含 URL/口令）；kafka/DB 客户端的文本未实测，列入本单验证 |
| 8 | `plaita/server/factory.py` `_parse_redis_url` → `RedisUrlError` | 明文 URL | `logger.*` / 调用方 | **已安全**（#88 起消息经 `_mask_url` 渲染） |
| 9 | `plaita/dsl/codeflow/_common.py`、`_stmt.py`、`dsl/ir_validate.py` 的 `…Error(…, target)` | `target` 是**节点 id**，不是 URL | 编译期错误 | **不适用**（r2 扫描的假阳类；守卫判据已刻意不把 `target` 当 URL） |

## 验收标准（代码级）

1. 上面 #2、#3、#5、#6 的每一条，都能给出「用真实第三方异常对象 + 真实
   runner/worker 路径」的动态测试：`str` / `repr` / `traceback.format_exception` /
   捕获记录的 `msg`·`args`·`getMessage`·格式化 `exc_text` / `NodeExecutionError`
   链里，**不含**密钥本体与任何 ≥3 字符前缀/后缀（复用
   `tests/unit/_url_sink_support.leak_needles`）。
2. 修法必须**在异常对象构造处**掩码或清洗，不得只在 logger 调用点做——否则
   r1 的守卫盲区原形复发。
3. 守卫第二腿（`_url_sink_support.exception_message_url_sites`）扩展为可识别
   「第三方异常文本经 `str(err)` 转手」的形态：至少对 `handle_http_node_err`
   这类 `NodeException(code, str(err))` 转手点，要求 `str(err)` 的输出经掩码/
   清洗渲染器（新增的渲染器名进 `_MASKER_NAMES`），并有由真实源文件改一行生成
   的红→绿探针。
4. 不回归：POST/传输路径仍拿到配置的原始 URL（逐字节）；`_mask_url_for_log` /
   `_mask_webhook_url_for_log` 的 totality / idempotence 性质不变；
   `plaita/server/factory.py` 零 diff（若必须改，单列在验收里）。
5. 至少 3 条变异（`158-*`/`159-*`）分别覆盖：还原 `str(err)` 明文、绕过清洗
   渲染器、回退守卫第二腿；每条由普通 `FAILED` 行杀死、`old` 唯一。
