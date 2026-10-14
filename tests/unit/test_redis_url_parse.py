"""plaita#88 —— ``_parse_redis_url`` 的形态契约。

缺陷：手写正则只认 ``redis://[:password@]host:port[/db]``，``match is None`` 时
**静默**返回 ``localhost:6379/0``。于是 ``rediss://``、``user:pass@``、省略端口、
IPv6 字面量等形态被改写成 localhost，而同一进程的队列/事件总线仍用原始 URL
（``Redis.from_url``）——存储与队列连到两个 Redis。

本文件把「URL 形态 × 入口」的每一格钉成三态之一：

* **解析**（合法 URL → ``RedisTarget``，且与队列 ``Redis.from_url`` 同端点）；
* **具名拒绝**（畸形/不支持 → ``RedisUrlError``，带原因 + **掩码** URL）；
* **显式默认**（未配置：``None``/空串/纯空白 → ``DEFAULT_REDIS_URL``，非错误）。

没有第四态：任何形态都不得静默变成 localhost。

r2（P0）：拒绝消息里的 URL 一律经 ``factory._mask_url`` 渲染（密码 ``***``）——
它会被 ``flow_worker`` / ``event_filter`` 原样写进生产日志。

r3（P0）：r2 的 ``_mask_url`` 是正则（口令字符类排除 ``/`` 与空白、只掩第一个
``X:Y@``），口令含 ``@``/空格的形态照样原样落盘。本文件改用**结构化**掩码
（authority 里最后一个 ``@`` 分 userinfo、userinfo 第一个 ``:`` 分用户/口令），
并以 ``redis.connection.parse_url`` **接地**：只要 redis-py 认出口令，就断言
原文与 parse_url 口令的**长度 ≥3 前缀/后缀**都不出现在 ``str``/``repr``/
traceback 任何一处。

r4（P0）：r3 仍有三处「依赖输入形态」的漏点，本文件把它们钉死：

* 结构层只在**字面 ``://``** 之后找 userinfo ⇒ 单斜杠 ``redis:/user:pw@host``、
  大写 ``REDIS:/…``、凭据在 scheme 前 ``user:pw@redis://host`` 认不出凭据，
  **原样返回**（r2 反而掩住了，是 r3 的回归）。现在锚点是**全串最后一个 ``@``**，
  与 ``://`` 无关；
* 口令重复写在 userinfo 之外（``?socket_timeout=PwX``、``/PwX``）时活下来。
  现在识别到的口令以**原文 / percent-encoded / decoded** 三种形态在 userinfo 段
  与 path/query/fragment 上再洗一遍（scheme/host 是结构，不参与替换）；
* ``raise … from exc`` 的 ``__cause__`` 是 ``traceback`` **独立渲染的对象**，
  只洗消息文案不够。现在换成洗过的**同类型替身**。

r5（P0）：r4 的架构（结构化、单遍、无 ``re``、兜底重洗、cause 替身）保留，补四处：

* **R1**：三个结构档都要求凭据形状的 ``@`` 落在**同一个 ``/`` 段**内——口令里含
  ``/`` 时（``redis://user:p/w@host``）段先被截断，三档全部落空，
  ``_mask_url`` 走 ``if credentials is None: return value`` **原样返回**；
* **R2**：``_passwords`` / ``_scrub`` 依赖 ``_locate_credentials``，没有 userinfo 时
  整层重洗空转——``redis://host:6379/0?db=Pr0d:x@y``（r2 掩了、r3/r4 漏了）；
* **R3**：``raise … from _sanitized_cause(...)`` 只置 ``__cause__``，
  ``__context__`` 仍是**原始**异常，走 ``__context__`` 的消费者拿到明文；
* **R4**：本文件的 needle 只用 ``redis.connection.parse_url`` 接地，而它在端口/db
  cast 失败时产不出 password——r1 的两族因此**按构造**看不见（见
  ``LITERAL_GROUNDED`` 的第二类「字面接地」行）。

r5 追加（``#158`` 验收交回）：``_mask_url`` 必须是 **total**（任意 ``str`` 不抛，
含 ``os.environ`` surrogateescape 的孤立代理字符）；``@`` 前没有凭据冒号时也要
fail-closed（``:`` 被写成 ``/``/``?``/``#``）；没有 ``@`` 的凭据样 token 同样要洗。

r7（P0）：**验收判据换向**——从「自洽」改成「**完备**」，且 needle 由**独立
oracle** 选。r6 的性质测试拿**注入的文本**当 needle，而它的 ``user==pw`` 槽用
``safe_token(allow_at=False, allow_colon=False)`` 把 `@`/`:` 从字母表里删掉了——
6000 行语料里那 545 个形状**没有一个含 `@`**，于是语料自己选出来的 needle 里
不含 ``Ccc`` 这类后缀，洞一直看不见：

* ``redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0`` 的三个 oracle（``parse_url``
  / ``urlsplit`` / ``_parse_redis_url``）都报 ``zB0@@cB9cB@@Ccc``；r6 输出
  ``redis://***@@***@@Ccc:***@host:6379/0``（后缀 ``Ccc`` 活下来）。根因是
  ``_VALUE_DELIMITERS`` 含 ``@``，``_at_colon_spans`` 的 ``start`` 退到用户段
  最后一个 ``@`` 之后，那个 token 没有任何跨度覆盖；r7 改成「authority 里候选
  冒号**之前还有 `@`** ⇒ 用户/口令分界**不可证** ⇒ 整段 userinfo fail-closed」
  （见 ``_userinfo_split_is_provable``），不是给 ``user==pw`` 加特例。
* 验收（主）：oracle 报出的口令，任何 ≥3 **前缀/后缀**（``test_oracle_grounded_
  prefix_suffix_never_leaks``）与所有 ≥3 **子串**（``test_oracle_grounded_no_
  substring_survives``）都不在五个渲染面上。**自洽不蕴含完备**——r6 的
  ``_passwords`` 与 ``_mask_url`` 完全自洽，仍然泄露后缀。
* 不变量**合格化**：``_passwords(s) != () ⟹ _mask_url(s)`` 不含其中任何形态
  ——**除了**与有意保留的结构 token（scheme / 可证用户名 / host / port / db /
  非 authority 跨度的用户 token）偶然重合的那些（验证者的 432 个反例全是这一类，
  如 ``redis://user:Q7w:9:6379@host:6379/0`` 的 ``6379`` 与 host 端口）。
* 语料**逐槽全字母表**：13 个候选槽（userinfo user / password、``user==pw``、
  query 值 / 名、无 `@` 的 `?u=X:`、path、fragment、no-scheme、single-slash、
  empty-user、multi-span、slash-in-password）**每一个**都用同一个全字母表口令；
  ``test_corpus_per_slot_counts_and_alphabet`` 把逐槽样本数与字母表覆盖钉住，
  任何「为让语料变绿而收窄字母表」都会红。
* 三处**默认拒绝**（同单，不是新工作）：``?db=<非法值>``、``?<未知名>=1``、
  fragment / 非法 query 取值的回显都被抹成 ``***``（保留参数名与固定措辞）。

四个渲染面（``str`` / ``repr`` / ``traceback.format_exception`` / 真实
``logger.error(..., exc_info=True)``）都有断言；``__cause__`` 替身另算一面。
"""
from __future__ import annotations

import logging
import random
import traceback
import unittest
from urllib.parse import unquote, urlsplit

from redis import Redis
from redis.connection import parse_url

from plaita.server import factory
from plaita.server.factory import (
    DEFAULT_REDIS_URL,
    RedisTarget,
    RedisUrlError,
    _REDACTED,
    _mask_url,
    _parse_redis_url,
    _passwords,
    create_storage_component,
)

#: 合法形态（解析；端口/db 可省，凭据可带用户名）
ACCEPTED = [
    "redis://host:6379/0",
    "redis://host",
    "redis://host/3",
    "redis://host:6379",
    "redis://host:6379/",
    "redis://:pw@host:6379/2",
    "redis://user:pw@host:6379/0",
    "rediss://user:pw@host:6380/1",
    "rediss://host:6380",
    "redis://user%40d:p%40ss%3Aword@host:6379/0",
    "redis://[::1]:6379/0",
    "rediss://[2001:db8::1]:6380/2",
    "redis://host:6379/0?db=2",
    # r2：redis-py 认的 query 参数必须能进来（此前一律硬拒绝 ⇒ 生产启动崩）
    "redis://host:6379/0?socket_timeout=5",
    "redis://host:6379/0?socket_timeout=5&health_check_interval=30",
    "rediss://host:6380/0?ssl_min_version=771&health_check_interval=30",
    # r2：redis-py 不认识的 query 名字也不再判死（见 _parse_query_options）
    "redis://host:6379/0?foo=bar",
    "unix:///tmp/plaita-redis.sock",
    "unix:///tmp/plaita-redis.sock?db=1",
]

#: 畸形/不支持形态（具名拒绝）——文件里的每一种都曾被静默改写成 localhost
REFUSED = [
    ("empty target", "redis://"),
    ("host missing with db", "redis:///3"),
    ("missing scheme", "host:6379"),
    ("scheme-relative", "//host:6379"),
    ("wrong scheme http", "http://host:6379"),
    ("wrong scheme rediss-less typo", "redis2://host:6379/0"),
    ("uppercase scheme", "REDIS://host:6379/0"),
    ("port non-numeric", "redis://host:notaport"),
    ("port out of range", "redis://host:99999"),
    ("port zero", "redis://host:0"),
    ("invalid IPv6 literal", "redis://[::1:6379/0"),
    ("bad db path", "redis://host/foo"),
    ("negative db query", "redis://host:6379/0?db=-1"),
    ("non-int db query", "redis://host:6379/0?db=x"),
    ("invalid query value", "redis://host:6379/0?socket_timeout=abc"),
    # r2：parse_url 认 timeout，但 AbstractConnection 没有该形参——传下去会连接期炸
    ("timeout query param", "redis://host:6379/0?timeout=3"),
    ("fragment", "redis://:p#w@host:6379/0"),
    ("fragment after db", "redis://host:6379/0#frag"),
    ("unix without socket path", "unix://"),
]

#: 带凭据的拒绝形态（plaita#88 r2 P0 → r3 结构化掩码）：消息会进生产日志，
#: 口令一个字都不能出现。
#:
#: 每条 = ``(形态名, URL, 原文里出现的口令字面量)``。测试用
#: ``redis.connection.parse_url`` **接地**（复核 redis-py 真正会拿去连的口令），
#: 再断言「原文口令」与「parse_url 口令」的**长度 ≥3 前缀/后缀**都不出现在
#: ``str``/``repr``/traceback 里——r2 的正则只掩住普通中段口令，本表覆盖
#: 口令含 ``@``（前/后/多个）/空格/tab/nbsp/``:``/``%``/unicode、空用户、空口令、
#: authority 多个 ``@``、裸 ``#``、bytes、缺 scheme、畸形 authority 等全部形态。
SECRET = "sup3rsecret"
LEAK_SHAPES = [
    # r2 正则的四个漏网形态（r3 P0 反例，逐字保留）
    ("at leading password", "redis://user:@Pr0d2024x@redis.internal:0", "@Pr0d2024x"),
    ("space in password", "redis://user:Super Secret@redis.internal:0", "Super Secret"),
    ("at inside password", "redis://user:p@ssword@redis.internal:0", "p@ssword"),
    ("nbsp in password", "redis://user:Super\xa0Secret@redis.internal:0", "Super\xa0Secret"),
    # 口令里 '@' 的其余位置 / 多个 '@'
    ("at trailing password", "redis://user:Pr0d2024x@@redis.internal:0", "Pr0d2024x@"),
    ("multiple at in password", "redis://user:Pr0d2024x@c@redis.internal:0", "Pr0d2024x@c"),
    ("multiple at in authority", "redis://a@b:Pr0d2024x@c@redis.internal:0", "Pr0d2024x@c"),
    # 空白 / 结构字符 / percent-escape / unicode
    ("tab in password", "redis://user:Super\tSecret@redis.internal:0", "Super\tSecret"),
    ("colon in password", "redis://user:Su:per@redis.internal:0", "Su:per"),
    ("percent-encoded at", "redis://user:Pr0d%402024x@redis.internal:0", "Pr0d%402024x"),
    ("percent-encoded slash", "redis://user:Pr0d%2F2024x@redis.internal:0", "Pr0d%2F2024x"),
    ("unicode password", "redis://user:p\u00e4ssw\u00f6rd@redis.internal:0", "p\u00e4ssw\u00f6rd"),
    # 空用户 / 空口令 / 口令在裸 '#'
    ("empty user", "redis://:Pr0d2024x@redis.internal:0", "Pr0d2024x"),
    ("empty password", "redis://user:@redis.internal:0", ""),
    ("raw hash in password", "redis://:Pr0d2024x#x@host:6379/0", "Pr0d2024x#x"),
    # 各拒绝分支
    ("fragment", "redis://user:Pr0d2024x@redis.internal:6379/0#frag", "Pr0d2024x"),
    ("bad db query", "redis://user:Pr0d2024x@redis.internal:6379/0?db=x", "Pr0d2024x"),
    ("negative db query", "redis://user:Pr0d2024x@redis.internal:6379/0?db=-1", "Pr0d2024x"),
    ("bad db path", "redis://user:Pr0d2024x@redis.internal:6379/foo", "Pr0d2024x"),
    (
        "invalid query value",
        "redis://user:Pr0d2024x@redis.internal:6379/0?socket_timeout=abc",
        "Pr0d2024x",
    ),
    (
        "timeout query param",
        "redis://user:Pr0d2024x@redis.internal:6379/0?timeout=3",
        "Pr0d2024x",
    ),
    ("port non-numeric", "redis://user:Pr0d2024x@host:notaport", "Pr0d2024x"),
    ("port out of range", "redis://user:Pr0d2024x@host:99999", "Pr0d2024x"),
    ("port zero", "redis://user:Pr0d2024x@host:0", "Pr0d2024x"),
    ("missing host", "redis://user:Pr0d2024x@/3", "Pr0d2024x"),
    ("invalid IPv6 literal", "redis://user:Pr0d2024x@[::1:6379/0", "Pr0d2024x"),
    ("malformed authority", "redis://[::1:user:Pr0d2024x@host/0", "Pr0d2024x"),
    ("unix without socket path", "unix://user:Pr0d2024x@", "Pr0d2024x"),
    ("unsupported scheme", "REDIS://user:Pr0d2024x@host:6379/0", "Pr0d2024x"),
    ("missing scheme", "user:Pr0d2024x@host:6379", "Pr0d2024x"),
    ("wrong scheme", "http://user:Pr0d2024x@host:6379/0", "Pr0d2024x"),
    ("non-str bytes", b"redis://user:Pr0d2024x@host:6379/0", "Pr0d2024x"),
    # r4 Finding 1 逐字复现（验证者的五个形态）：r2 掩住了、r3 原样返回——
    # 结构性回归，因为 r3 只在**字面 '://'** 的 authority 里找 userinfo。
    ("r4 single-slash typo", "redis:/user:pw@host", "pw"),
    ("r4 single-slash typo with port/db", "redis:/user:pw@host:6379/0", "pw"),
    ("r4 single-slash uppercase scheme", "REDIS:/user:pw@host", "pw"),
    ("r4 credentials before scheme", "user:pw@redis://host", "pw"),
    ("r4 credentials before rediss", "user:pw@rediss://host:6380/0", "pw"),
    # r4 Finding 1 兜底层：口令重复出现在 userinfo 之外（query/path）
    ("r4 password repeated in query", "redis://user:PwX@host:6379/0?socket_timeout=PwX", "PwX"),
    (
        "r4 password repeated percent-encoded in query",
        "redis://user:@Pr0d2024x@host:6379/0?socket_timeout=%40Pr0d2024x",
        "@Pr0d2024x",
    ),
]


#: r4 Finding 1：验证者的五个形态 × **≥3 字符口令**（verbatim 那组口令只有 2 字符，
#: 走不到 ≥3 前缀/后缀 needle 方法）。这组必须有 ``://`` 之外的锚定能力。
GLOBAL_FAIL_CLOSED_URLS = [
    ("redis:/user:Pr0d2024x@host", "Pr0d2024x"),
    ("redis:/user:Pr0d2024x@host:6379/0", "Pr0d2024x"),
    ("REDIS:/user:Pr0d2024x@host", "Pr0d2024x"),
    ("rediss:/:Pr0d2024x@host:6380/0", "Pr0d2024x"),
    ("user:Pr0d2024x@redis://host", "Pr0d2024x"),
    ("user:Pr0d2024x@rediss://host:6380/0", "Pr0d2024x"),
    ("user:Pr0d2024x@host:6379", "Pr0d2024x"),
    # 空 authority 多写一个斜杠 / 口令含 '/' 的畸形态（r2 掩了、r3 漏了）
    ("redis:///user:Pr0d2024x@host", "Pr0d2024x"),
    ("redis:/u@u:p/word@host:0/p/word", "p/word"),
    # r6 的 no-scheme 粗档：细档被端口判据吃掉（``user:911/`` 与 ``host:port/db``
    # 局部同形），靠粗档整段 fail-closed。r7 把它作为粗档的杀手留下来。
    ("redis:/user:911/z@host", "911"),
]

#: r4 Finding 1 兜底层：口令在 userinfo 之外又被写了一遍（原文 / 编码 / path）。
RESIDUAL_REPEAT_URLS = [
    "redis://user:Pr0d2024x@host:6379/0?socket_timeout=Pr0d2024x",
    "redis://user:Pr0d2024x@host:6379/0?socket_timeout=%40Pr0d2024x",
    "redis://user:Pr0d2024x@host:6379/Pr0d2024x",
    "redis://user:Pr0d2024x@host:6379/0?db=Pr0d2024x",
]

#: r5 R4：第二类**字面接地**（literal grounding）行——r4 的表「按构造」看不见的两族。
#:
#: 为什么必须另起一类：``LEAK_SHAPES`` 的 needle 由
#: ``redis.connection.parse_url`` **接地**（复核 redis-py 真正会拿去连的口令），
#: 但 ``parse_url`` 在**端口/db cast 失败**时直接抛异常、产不出 password——r5 R1
#: 的两族反例（口令含 ``/`` 被 ``urlsplit`` 截断在 authority、凭据只写在 query 值
#: 里）恰好落在这个盲区。只用 ``parse_url`` 接地 ⇒ needle 集合**按构造**不含这些
#: 口令，表再全也看不见它们（这正是 r4 漏掉两个规则级反例的原因）。
#:
#: 本类的 needle 是**字面量**：写 URL 的人自己知道口令是哪一段，不经过 parse_url。
#: 每条 = ``(形态名, URL, 字面量口令, 接地方式, 备注)``——
#:
#: * ``"literal"``：本类。``why`` 写明这一行为什么只能字面接地；
#: * ``"parse_url+literal"``：与 ``LEAK_SHAPES`` 同款双臂接地（对照用）。
#:
#: **red-then-green**：备注带 ``r4-verbatim`` 的行在 ``2f660c6``（r4）上就是红的
#: ——``_mask_url`` 把口令原样返回（或部分返回），
#: ``test_literal_grounded_never_returns_input_verbatim`` /
#: ``test_literal_grounded_rows_never_leak`` 会失败；修完变绿。
LITERAL_GROUNDED = [
    # --- 端口 cast 失败：口令里的 '/' 先把 authority 截断，urlsplit 只把口令段
    #     当端口值去 int()——parse_url 在这里抛，产不出 password。 ---
    (
        "slash-in-password port cast",
        "redis://user:pa55word/x@host",
        "pa55word",
        "literal",
        "r4-verbatim",
    ),
    (
        "slash-in-password port cast with port/db",
        "redis://user:pa55word/x@host:6379/0",
        "pa55word",
        "literal",
        "r4-verbatim",
    ),
    (
        "slash-in-password unicode port cast",
        "redis://user:p\u00e4ssw\u00f6rd/h@host",
        "p\u00e4ssw\u00f6rd",
        "literal",
        "r4-verbatim",
    ),
    # --- db= cast 失败：凭据只写在 query 值里，authority 里根本没有 userinfo ---
    (
        "query-value credential, db cast",
        "redis://host:6379/0?db=Pr0d:Secret1@y",
        "Secret1",
        "literal",
        "r4-verbatim（r2 掩了、r3/r4 漏了）",
    ),
    (
        "query-value credential, no at-sign, db cast",
        "redis://host:6379/0?db=Pr0d:Secret1",
        "Secret1",
        "literal",
        "r4-verbatim",
    ),
    (
        "query-value credential, no userinfo",
        "redis://host?user:Secret1@x",
        "Secret1",
        "literal",
        "r4-verbatim",
    ),
    (
        "query-value credential, u= position",
        "redis://host:6379/0?u=X:Secret1@x",
        "Secret1",
        "literal",
        "r4-verbatim（验证者的 37 个残留全是这一族）",
    ),
    # --- ':' 被写成 '/'/'?'/'#'：没有任何候选凭据冒号，但 '@' 前是口令 ---
    (
        "colon written as slash",
        "redis://user/Tr0ub4dor@host:6379/0",
        "Tr0ub4dor",
        "literal",
        "r4-verbatim",
    ),
    (
        "colon written as question mark",
        "redis://user?Tr0ub4dor@host:6379/0",
        "Tr0ub4dor",
        "literal",
        "r4-verbatim",
    ),
    (
        "colon written as hash",
        "redis://user#Tr0ub4dor@host:6379/0",
        "Tr0ub4dor",
        "literal",
        "r4-verbatim",
    ),
    (
        "colon written as hash, second at-token",
        "redis://user#Tr0ub4dor@host:6379/0?u=X:Y@x",
        "Tr0ub4dor",
        "literal",
        "r4-verbatim",
    ),
    (
        "question mark before colon",
        "redis://?user:Tr0ub4dor@127.0.0.1:679/0",
        "Tr0ub4dor",
        "literal",
        "r4-verbatim",
    ),
    (
        "hash before colon in authority",
        "redis://se#r:Tr0ub4dor@127.0.0.1:679/0",
        "Tr0ub4dor",
        "literal",
        "r4-verbatim",
    ),
    # --- 完全没有 '@'：redis-py 的 userinfo 位为空，但 'X:Y' 仍是凭据样文本 ---
    (
        "no at-sign, credential token in authority",
        "redis://user:Tr0ub4dor127.0.0.1:679/0",
        "Tr0ub4dor",
        "literal",
        "r4-verbatim",
    ),
    (
        "no at-sign, credential token in query",
        "redis://host:6379/0?u=X:Secret1",
        "Secret1",
        "literal",
        "r4-verbatim",
    ),
    (
        "no at-sign, credential token in path",
        "redis://host/0/X:Secret1",
        "Secret1",
        "literal",
        "r4-verbatim",
    ),
    # --- 两个渲染面同时接地（对照：这一族两版都能接上） ---
    (
        "userinfo + query credential",
        "redis://user:Pr0d2024x@host:6379/0?u=X:Secret1@x",
        "Secret1",
        "parse_url+literal",
        "r4-verbatim（第二个跨度）",
    ),
    # --- r6 R1：口令里含 '://'，r5 的 `rfind("://")` 锚点把口令自己的 `://`
    #     当成 scheme 边界，只掩到尾巴（`redis://user:pa55word://***@host`）。 ---
    (
        "r6 R1 password containing scheme separator",
        "redis://user:pa55word://x@host",
        "pa55word",
        "literal",
        "r5-leak（口令自己的 :// 当成了 scheme 边界）",
    ),
    (
        "r6 R1 password containing scheme separator, with port/db",
        "redis://user:Qz7Wm3Kp9://w@host:6379/0",
        "Qz7Wm3Kp9",
        "literal",
        "r5-leak（同上）",
    ),
    # --- r6 R2：有 userinfo 时**第二个**跨度（query 值）也要取出来；r5 只取第一个
    #     候选冒号，于是消息字段原样回显第二条凭据。 ---
    (
        "r6 R2 second span in query value with userinfo",
        "redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1@x",
        "Secret1",
        "literal",
        "r5-leak（只取第一个候选冒号）",
    ),
    (
        "r6 R2 second span, empty user",
        "redis://:Pr0d2024x@host:6379/0?db=Acct:Secret1@x",
        "Secret1",
        "literal",
        "r5-leak（同上）",
    ),
    (
        "r6 R2 second span in query value, no at-sign",
        "redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1",
        "Secret1",
        "literal",
        "r5-leak（没有 '@' 时只看第一个候选）",
    ),
    # --- r6：没有 scheme 且候选冒号被端口判据吃掉（`user:911/`）时的粗档。
    #     细档全空 ⇒ 把 [scheme 冒号之后, 最后一个 '@') 整段当 userinfo。 ---
    (
        "r6 no scheme, port-shaped credential (coarse tier)",
        "redis:/user:911/z@host",
        "911",
        "literal",
        "r5-leak（细档被端口判据吃掉、粗档被关掉时原样留下 911）",
    ),
    # --- r7：非法取值 / 未知名是攻击者可控文本，消息与日志默认拒绝（不再回显）。
    #     这两行在 r5/r6 上是**红**的（消息/警告原样回显），修完变绿。 ---
    (
        "r7 illegal db query value must not be echoed",
        "redis://host:6379/0?db=Pr0d2024x",
        "Pr0d2024x",
        "message-field",
        "r5/r6-leak（'db 查询参数不是整数' 原样回显取值）",
    ),
    (
        "r7 unknown query name must not be echoed",
        "redis://host:6379/0?Pr0d2024x=1",
        "Pr0d2024x",
        "message-field",
        "r5/r6-leak（未知名 WARNING + 回显的 URL 原样带名字）",
    ),
]

#: r5 A：``_mask_url`` 链必须 **total**——``os.environ`` 用 surrogateescape 解码
#: 非 UTF-8 字节得到孤立代理字符，``quote``/``unquote`` 的 utf-8 编码会抛
#: ``UnicodeEncodeError``。这些串在 #158 的两个新调用点上实测把启动路径打崩。
SURROGATE_URLS = [
    "redis://user:p\udcffw@127.0.0.1:679/0",
    "redis://user:p\udcffw/x@127.0.0.1:679/0",
    "redis://user:\udcff\udcfe@host:6379/0",
    "redis://host:6379/0?db=\udcff\udcfe",
    "redis://host:6379/0?u=X:\udcff\udcfe@x",
    "redis://user:Tr0ub4dor\udcff@host:6379/0",
    "\udcff\udcfe",
]


def _redis_py_password(url) -> object:
    """``redis.connection.parse_url`` 真正会拿去连的口令；它解析不了时 None。

    测试的**接地**来源：不断言「我以为的口令」，只断言 redis-py 自己认的口令。
    """
    if not isinstance(url, str):
        return None
    try:
        return parse_url(url).get("password")
    except Exception:
        return None


def _partial_needles(secret: str) -> set:
    """口令的所有长度 ≥3 前缀/后缀——抓 r2 那种 ``ssword`` 式部分泄露。"""
    return {
        secret[i : i + length]
        for length in range(3, len(secret) + 1)
        for i in range(0, len(secret) - length + 1)
    }


def _exception_messages(traceback_text: str) -> str:
    """traceback 里**异常消息**的部分：剥掉源码路径、源码摘录与 caret 行。

    临时树路径（``.../worktrees/...``）与函数名（``return _parse_redis_url_checked``）
    可能恰好包含口令的 3 字子串（``Secret1`` → ``ret``）；路径/代码不是泄露面，
    真正的泄露面是异常消息与 ``from exc`` 的嵌套 cause 文案。
    """
    out = []
    for line in traceback_text.splitlines():
        if '  File "' in line or line == "Traceback (most recent call last):":
            continue
        if line[:1] in (" ", "\t"):
            continue  # 源码摘录 / caret 行
        out.append(line)
    return "\n".join(out)


def _logged_error_text(exc: BaseException) -> str:
    """真实 ``logger.error(..., exc_info=True)`` 的渲染（``event_filter.py:488`` 同款）。

    ``logging.Formatter`` 会把 ``record.exc_info``（含 ``__cause__`` 链）一并渲染——
    这正是生产日志里落盘的内容，也是第四个渲染面。``File "..."`` 行照旧剥掉。
    """
    records = []

    class _Capture(logging.Handler):
        def emit(self, record):  # pragma: no cover - 由格式化驱动
            records.append(self.format(record))

    log = logging.getLogger("plaita.server.factory")
    handler = _Capture()
    old_level = log.level
    log.addHandler(handler)
    log.setLevel(logging.ERROR)
    try:
        log.error("事件过滤器启动失败: %s", exc, exc_info=True)
    finally:
        log.removeHandler(handler)
        log.setLevel(old_level)
    return _exception_messages("\n".join(records))


def _surfaces(exc: BaseException) -> dict:
    """异常在生产里的**四个**渲染面：``str`` / ``repr`` /
    ``traceback.format_exception`` / 真实 ``logger.error(..., exc_info=True)``。

    前两者的 ``File "..."`` 源码路径行被剥掉（见 ``_exception_messages``），
    ``from exc`` 的嵌套 cause 文案保留——它才是 r4 Finding 2 的泄露面。
    """
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return {
        "str": str(exc),
        "repr": repr(exc),
        "traceback-messages": _exception_messages(tb),
        "logger-exc_info": _logged_error_text(exc),
    }


def _capture_log_lines(fn) -> list:
    """跑 ``fn``，返回 ``plaita.server.factory`` 的全部格式化日志行（含 traceback）。"""
    records = []

    class _Capture(logging.Handler):
        def emit(self, record):  # pragma: no cover - 由格式化驱动
            records.append(self.format(record))

    log = logging.getLogger("plaita.server.factory")
    handler = _Capture()
    old_level = log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        fn()
    finally:
        log.removeHandler(handler)
        log.setLevel(old_level)
    return records


def _refusal_text(url) -> str:
    """``_parse_redis_url(url)`` 的拒绝文案；解析成功时返回 ``""``。"""
    try:
        _parse_redis_url(url)
    except RedisUrlError as exc:
        return str(exc)
    return ""


def _effective(client: Redis) -> dict:
    """客户端 → 生效连接目标（``None`` 与「缺省」等价；端口缺省=6379）。"""
    kwargs = client.connection_pool.connection_kwargs
    return {
        "host": kwargs.get("host"),
        "port": kwargs.get("port") or 6379,
        "db": kwargs.get("db", 0),
        "username": kwargs.get("username"),
        "password": kwargs.get("password"),
        "path": kwargs.get("path"),
        "connection_class": client.connection_pool.connection_class.__name__,
    }


class TestParseAcceptedShapes(unittest.TestCase):
    """合法形态必须完整解析（端口/db/凭据/TLS/IPv6/unix）。"""

    def test_issue_reproduction_targets(self):
        """#88 复现命令里的三条：不得再出现 ('localhost', 6379, 0, None)。"""
        self.assertEqual(
            tuple(_parse_redis_url("redis://plaita-redis"))[:4],
            ("plaita-redis", 6379, 0, None),
        )
        self.assertEqual(
            tuple(_parse_redis_url("rediss://redis.internal:6380/0"))[:4],
            ("redis.internal", 6380, 0, None),
        )
        # username 必须透出，密码不得丢，host 不能再被用户名顶替
        user_pass = _parse_redis_url("redis://user:pass@host:6379/0")
        self.assertEqual(tuple(user_pass)[:4], ("host", 6379, 0, "pass"))
        self.assertEqual(user_pass.username, "user")

    def test_omitted_port_and_db_default(self):
        target = _parse_redis_url("redis://host")
        self.assertIsInstance(target, RedisTarget)
        self.assertEqual((target.host, target.port, target.db, target.password), ("host", 6379, 0, None))

    def test_db_from_path(self):
        self.assertEqual(_parse_redis_url("redis://host/3").db, 3)

    def test_password_only_userinfo(self):
        target = _parse_redis_url("redis://:pw@host:6379/2")
        self.assertEqual((target.host, target.db, target.password), ("host", 2, "pw"))
        self.assertIsNone(target.username)

    def test_username_and_password_userinfo(self):
        target = _parse_redis_url("redis://user:pw@host:6379/0")
        self.assertEqual((target.host, target.username, target.password), ("host", "user", "pw"))

    def test_rediss_keeps_endpoint_and_marks_ssl(self):
        target = _parse_redis_url("rediss://user:pw@host:6380/1")
        self.assertEqual(target.scheme, "rediss")
        self.assertTrue(target.ssl)
        self.assertEqual((target.host, target.port, target.db), ("host", 6380, 1))
        self.assertFalse(_parse_redis_url("redis://host:6379/0").ssl)

    def test_credentials_percent_decoded(self):
        target = _parse_redis_url("redis://user%40d:p%40ss%3Aword@host:6379/0")
        self.assertEqual((target.username, target.password), ("user@d", "p@ss:word"))

    def test_ipv6_literal(self):
        target = _parse_redis_url("redis://[::1]:6379/0")
        self.assertEqual((target.host, target.port, target.db), ("::1", 6379, 0))
        self.assertEqual(_parse_redis_url("rediss://[2001:db8::1]:6380/2").host, "2001:db8::1")

    def test_db_query_beats_path(self):
        self.assertEqual(_parse_redis_url("redis://host:6379/0?db=2").db, 2)

    def test_trailing_slash_is_default_db(self):
        self.assertEqual(_parse_redis_url("redis://host:6379/").db, 0)

    def test_unix_socket_path(self):
        target = _parse_redis_url("unix:///tmp/plaita-redis.sock")
        self.assertEqual(target.scheme, "unix")
        self.assertEqual(target.unix_socket_path, "/tmp/plaita-redis.sock")
        self.assertEqual(_parse_redis_url("unix:///tmp/plaita-redis.sock?db=1").db, 1)

    def test_accepted_shapes_agree_with_queue_parser(self):
        """解析结果必须与队列 ``Redis.from_url`` 同端点（否则又是两个 Redis）。"""
        for url in ACCEPTED:
            with self.subTest(url=url):
                target = _parse_redis_url(url)
                parsed = parse_url(url)  # 队列侧真正用的解析器
                if target.unix_socket_path is not None:
                    self.assertEqual(target.unix_socket_path, parsed["path"])
                else:
                    self.assertEqual(target.host, parsed["host"])
                    self.assertEqual(target.port, parsed.get("port", 6379))
                self.assertEqual(target.db, parsed.get("db", 0))
                self.assertEqual(target.username, parsed.get("username"))
                self.assertEqual(target.password, parsed.get("password"))


class TestAbsentUsesDocumentedDefault(unittest.TestCase):
    """「没配 Redis」必须保持既有行为：文档化默认，不是错误（#99/#95 先例）。"""

    def test_none_empty_and_whitespace(self):
        expected = _parse_redis_url(DEFAULT_REDIS_URL)
        for value in (None, "", "   ", "\t\n"):
            with self.subTest(value=value):
                self.assertEqual(_parse_redis_url(value), expected)

    def test_absent_matches_cli_default(self):
        self.assertEqual(
            _parse_redis_url(None).client_kwargs(),
            {"host": "localhost", "port": 6379, "db": 0},
        )

    def test_factory_without_redis_url_uses_default(self):
        for component_type in ("execution", "flow"):
            with self.subTest(component_type=component_type):
                storage = create_storage_component("redis", component_type, tenant_routing=True)
                pool = storage._current_storage().client.connection_pool.connection_kwargs
                self.assertEqual((pool.get("host"), pool.get("port") or 6379, pool.get("db", 0)),
                                 ("localhost", 6379, 0))


class TestMalformedIsNamedRefusal(unittest.TestCase):
    """畸形/不支持形态必须具名失败，绝不静默回落 localhost。"""

    def test_every_refused_shape_raises_named_error(self):
        for shape, url in REFUSED:
            with self.subTest(shape=shape, url=url):
                with self.assertRaises(RedisUrlError) as ctx:
                    _parse_redis_url(url)
                # 诊断仍要可用：消息里必须能认出 scheme。r1 这里写的是
                # ``assertIn(url, ...)``（把原样回显钉成契约），r2-r6 改成
                # ``_mask_url(url)`` 逐字出现；r7 起非法取值/未知名/fragment 也一并
                # 抹掉（默认拒绝），所以改成断言 scheme 仍可辨认 + 掩码存在。
                if isinstance(url, str):
                    self.assertIn(url.split(":", 1)[0] + ":", str(ctx.exception))
                if isinstance(url, str) and "@" in url:
                    self.assertIn(_REDACTED, str(ctx.exception))

    def test_empty_target_raises(self):
        """#88 验收：``redis://`` 空目标抛异常而非返回默认值。"""
        with self.assertRaises(RedisUrlError):
            _parse_redis_url("redis://")

    def test_never_returns_localhost_for_malformed(self):
        for _shape, url in REFUSED:
            with self.subTest(url=url):
                try:
                    target = _parse_redis_url(url)
                except RedisUrlError:
                    continue
                self.assertNotEqual(
                    (target.host, target.port),
                    ("localhost", 6379),
                    f"畸形 URL {url!r} 静默回落 localhost",
                )

    def test_bytes_input_is_named_refusal(self):
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url(b"redis://host:6379/0")
        self.assertIn("bytes", str(ctx.exception))

    def test_error_is_value_error_subclass(self):
        self.assertTrue(issubclass(RedisUrlError, ValueError))


class TestRefusalMessagesNeverLeakCredentials(unittest.TestCase):
    """plaita#88 r2 P0：拒绝消息会原样进生产日志，口令一个字都不许出现。

    ``flow_worker.py`` `logger.error("流程工作器启动失败: %s", e)`、
    ``event_filter.py`` `logger.error("...: %s", e, exc_info=True)`——后者连
    traceback（含 ``from exc`` 的嵌套 cause）一起落盘，所以三处都要钉。
    """

    def test_no_refusal_message_contains_the_password(self):
        # 不用 subTest：变异重放脚本只认 ``FAILED tests/...`` 行，SUBFAILED-only
        # 的死法它解析不出 killed-by（见 replay.py 的 detail 提取）。
        for shape, url, literal in LEAK_SHAPES:
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            exc = ctx.exception
            traceback_text = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )
            rendered = {
                "str": str(exc),
                "repr": repr(exc),
                "traceback-messages": _exception_messages(traceback_text),
            }
            # 接地：redis-py 真正会用的口令（它自己解析不了的形态不参与）
            secrets = {literal}
            redis_py_password = _redis_py_password(url)
            if isinstance(redis_py_password, str) and redis_py_password:
                secrets.add(redis_py_password)
            secrets.discard("")
            for secret in secrets:
                for kind, text in rendered.items():
                    self.assertNotIn(secret, text, f"{shape}/{kind}: 口令原文泄露")
                    for needle in _partial_needles(secret):
                        self.assertNotIn(
                            needle, text, f"{shape}/{kind}: 口令片段 {needle!r} 泄露"
                        )
                self.assertNotIn(secret, traceback_text, f"{shape}: traceback 泄露口令")
            # 只掩码、不删光：scheme 仍可辨认、掩码存在（r7 起非法取值/未知名/
            # fragment 也被抹掉，不再断言「掩码后的整条 URL 逐字出现」）。
            if isinstance(url, str):
                self.assertIn(
                    url.split(":", 1)[0] + ":", str(exc), f"{shape}: scheme 丢了"
                )
            self.assertIn(_REDACTED, str(exc), f"{shape}: 缺少掩码")

    def test_mask_helper_itself(self):
        self.assertEqual(
            _mask_url("redis://user:sup3rsecret@host:6379/0"),
            "redis://user:***@host:6379/0",
        )
        # 历史畸形：凭据里写了裸 '#'（urlsplit 会把它当 fragment），掩码也不能漏
        self.assertEqual(
            _mask_url("redis://:sup3rsecret#x@host:6379/0"),
            "redis://:***@host:6379/0",
        )
        # 没有凭据 → 原样；没有 scheme 的形态也要能掩码
        self.assertEqual(_mask_url("redis://host:6379/0"), "redis://host:6379/0")
        self.assertEqual(_mask_url("user:sup3rsecret@host:6379"), "user:***@host:6379")
        # r3 P0：authority 里**最后**一个 '@' 才是分隔符，口令整体（含 '@'/空白）抹掉
        self.assertEqual(
            _mask_url("redis://user:@Pr0d2024x@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )
        self.assertEqual(
            _mask_url("redis://user:p@ssword@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )
        self.assertEqual(
            _mask_url("redis://user:Super Secret@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )
        self.assertEqual(
            _mask_url("redis://user:Super\xa0Secret@redis.internal:0"),
            "redis://user:***@redis.internal:0",
        )

    def test_mask_helper_fails_closed(self):
        """r3：认不出 userinfo 也绝不原样返回——r2 正是「失败即返回原文」才漏的。"""
        # urlsplit 直接抛错的畸形 authority（redis-py 也解析不了）
        raw = "redis://[::1:user:Pr0d2024x@host/0"
        masked = _mask_url(raw)
        self.assertNotIn("Pr0d2024x", masked)
        self.assertIn("@host", masked)  # host 仍在，诊断不丢
        # 认不出用户名的形态：userinfo 整体抹成 ***，host 保留
        self.assertEqual(_mask_url("redis://user@host:6379"), "redis://***@host:6379")
        # r6：authority 里 `@` 的 token 全空（`@@@A`）时同样整段 fail-closed——
        # 只掩最后一个 token（`A`）会把 `@@@` 留在消息里。
        self.assertEqual(
            _mask_url("redis://@@@A@host:6379/0"), "redis://***@host:6379/0"
        )
        self.assertNotIn("@@@A", _mask_url("redis://@@@A@host:6379/0"))
        self.assertNotEqual(
            _mask_url("redis://user@host:6379"), "redis://user@host:6379"
        )

    def test_mask_helper_keeps_path_at_out_of_userinfo(self):
        """path/query 里的 '@' 不是 userinfo 分隔符——不得误抹 host。"""
        self.assertEqual(
            _mask_url("redis://host:6379/0@x"), "redis://host:6379/0@x"
        )
        self.assertEqual(
            _mask_url("redis://host:6379/0?ref=a@b"), "redis://host:6379/0?ref=a@b"
        )

    def test_nested_cause_is_scrubbed_too(self):
        """嵌套 cause：底层异常文案里若带裸 URL/口令，也必须先洗掉再进 traceback。"""
        text = "redis://user:sup3rsecret@host:6379/0?db=x"
        embedded = factory._safe_reason(ValueError(f"bad value in {text}"), text)
        self.assertNotIn(SECRET, embedded)
        self.assertIn("redis://user:***@host:6379/0?db=x", embedded)
        # 只带口令字面量（没带整条 URL）也要抹掉
        bare = factory._safe_reason(ValueError(f"bad password {SECRET}"), text)
        self.assertNotIn(SECRET, bare)
        # r3：带 '@' 的完整口令，_passwords 也必须整体取出并洗掉（r2 正则只取到一截）
        at_text = "redis://user:@Pr0d2024x@redis.internal:0"
        scrubbed = factory._safe_reason(ValueError("port needs @Pr0d2024x"), at_text)
        self.assertNotIn("@Pr0d2024x", scrubbed)

    def test_unknown_query_param_warning_is_masked(self):
        """未知 query 只警告不判死——警告本身也是日志，口令与**未知名**都不许出现。

        r7 更正：r2-r6 这里断言 ``"foo" in line``（未知名保留）；按本单的
        default-deny，未知名是攻击者可控文本，连同回显的 URL 一起抹掉，只保留
        「有几个未知名」与参数名 ``db`` 之类**固定**措辞。
        """
        with self.assertLogs("plaita.server.factory", level="WARNING") as logs:
            target = _parse_redis_url("redis://user:sup3rsecret@host:6379/0?foo=bar")
        self.assertEqual((target.host, target.port), ("host", 6379))
        for line in logs.output:
            self.assertNotIn(SECRET, line)
            self.assertNotIn("foo", line, "未知名被回显（r7 默认拒绝）")
            self.assertIn("redis://user:***@host:6379/0", line)
            self.assertIn(_REDACTED, line)
        # 已知参数名仍要可诊断（固定措辞 + 名字，不因 default-deny 一起被抹）
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url("redis://host:6379/0?socket_timeout=abc")
        self.assertIn("socket_timeout", str(ctx.exception))


class TestMaskerCannotLeakByConstruction(unittest.TestCase):
    """plaita#88 r4 P0：掩码不再依赖「输入是合法 URL」。

    r3 只认字面 ``://``、只替换一次、且把 ``__cause__`` 原样留给 traceback；
    这里把三条漏点（全局 fail-closed / 兜底重洗 / cause 替身）都钉在
    ``str`` / ``repr`` / ``traceback.format_exception`` 三个渲染面上。
    """

    def test_global_fail_closed_rule_table(self):
        """锚点是「凭据形状的最后一个 '@'」，不依赖字面 '://'。"""
        expected = {
            "redis:/user:pw@host": "redis:/user:***@host",
            "redis:/user:pw@host:6379/0": "redis:/user:***@host:6379/0",
            "REDIS:/user:pw@host": "REDIS:/user:***@host",
            "user:pw@redis://host": "user:***@redis://host",
            "user:pw@rediss://host:6380/0": "user:***@rediss://host:6380/0",
            # 空 authority 多写一个斜杠 / 口令含 '#'、'?'
            "redis:///user:pw@host": "redis:///user:***@host",
            "redis:///:p#w@host": "redis:///:***@host",
            "redis:///:p?w@host": "redis:///:***@host",
            # 口令本身含 '/' 且用户名段被口令重复污染（r6 枚举全部跨度后重洗整段
            # userinfo：掩码只多不少，`p/w` 与 `p/w@/0` 都不在结果里）
            "redis:/u@u:p/w@/0@x": "redis:/***@***:***@x",
            # keep：path/query 的 '@' 不是 userinfo
            "redis://host:6379/0@x": "redis://host:6379/0@x",
            "redis://host:6379/0?ref=a@b": "redis://host:6379/0?ref=a@b",
            "redis://host/0?x=1:2@y": "redis://host/0?x=1:2@y",
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)

    def test_global_fail_closed_never_returns_input_verbatim(self):
        for url, secret in GLOBAL_FAIL_CLOSED_URLS:
            masked = _mask_url(url)
            self.assertNotEqual(masked, url, url)
            self.assertNotIn(secret, masked, url)
            for needle in _partial_needles(secret):
                self.assertNotIn(needle, masked, f"{url}: {needle!r}")

    def test_global_fail_closed_message_surfaces(self):
        for url, secret in GLOBAL_FAIL_CLOSED_URLS:
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            exc = ctx.exception
            self.assertIn(_mask_url(url), str(exc), url)
            for kind, text in _surfaces(exc).items():
                self.assertNotIn(secret, text, f"{url}/{kind}")
                for needle in _partial_needles(secret):
                    self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")

    def test_residual_repeat_in_query_or_path_is_redacted(self):
        for url in RESIDUAL_REPEAT_URLS:
            masked = _mask_url(url)
            self.assertNotIn("Pr0d2024x", masked, url)
            for needle in _partial_needles("Pr0d2024x"):
                self.assertNotIn(needle, masked, f"{url}: {needle!r}")
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            exc = ctx.exception
            # r7：db 取值非法时 URL 回显里那截也被抹掉，故只断言 scheme 可辨。
            self.assertIn("redis:", str(exc), url)
            for kind, text in _surfaces(exc).items():
                for needle in _partial_needles("Pr0d2024x"):
                    self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")

    def test_cause_object_is_independently_sanitized(self):
        """r4 Finding 2：``from exc`` 的 ``__cause__`` 是 traceback 独立渲染的对象。"""
        url = "redis://user:@Pr0d2024x@host:6379/0?socket_timeout=%40Pr0d2024x"
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url(url)
        exc = ctx.exception
        chain = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        # 链仍在（诊断价值），但底层文案换成了洗过的同类型替身
        self.assertIsNotNone(exc.__cause__, "cause 链不该被丢掉")
        self.assertIn("socket_timeout", chain)
        # r7：非法取值也被抹掉（消息里的 URL 回显不再逐字等于 _mask_url(url)）
        self.assertNotIn("Pr0d2024x", str(exc))
        for text in (chain, str(exc.__cause__), repr(exc.__cause__)):
            for needle in ("@Pr0d2024x", "%40Pr0d2024x", "Pr0d2024x"):
                self.assertNotIn(needle, text, f"cause 链泄露 {needle!r}")
        self.assertIn("float", chain.lower())

    def test_message_fields_that_are_not_urls_are_scrubbed(self):
        """``?db=<口令>`` / ``/<口令>`` / ``?<口令>=1`` 这些字段也是消息面。"""
        for url in (
            "redis://user:Pr0d2024x@host:6379/0?db=Pr0d2024x",
            "redis://user:Pr0d2024x@host:6379/Pr0d2024x",
        ):
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            for kind, text in _surfaces(ctx.exception).items():
                self.assertNotIn("Pr0d2024x", text, f"{url}/{kind}")
                for needle in _partial_needles("Pr0d2024x"):
                    self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")
        # 口令写在 query **名**里：未知参数只警告不判死——warning 同样是日志面
        with self.assertLogs("plaita.server.factory", level="WARNING") as logs:
            target = _parse_redis_url("redis://user:Pr0d2024x@host:6379/0?Pr0d2024x=1")
        self.assertEqual((target.host, target.port), ("host", 6379))
        for line in logs.output:
            self.assertNotIn("Pr0d2024x", line)
            for needle in _partial_needles("Pr0d2024x"):
                self.assertNotIn(needle, line)

    def test_password_substring_of_scheme_or_host_is_not_a_false_red(self):
        """口令恰好是 scheme/host 的子串：朴素 needle 检查给的是**假红**。

        兜底重洗只作用于 userinfo 段与 host 之后的 path/query/fragment（见
        ``_redact_residuals``）——scheme/host 是结构、也是诊断信息，不参与替换，
        否则口令 ``redis`` 会把 scheme 抹成 ``***://``，且没有任何安全收益。
        """
        url = "redis://user:redis@redis.example.com:6379/0"
        masked = _mask_url(url)
        self.assertEqual(masked, "redis://user:***@redis.example.com:6379/0")
        self.assertNotIn(":redis@", masked)
        self.assertIn("@redis.example.com:6379/0", masked)
        # 同一个 needle 在**无凭据**的同类 URL 里也在 ⇒ needle 命中不等于泄露
        self.assertIn("redis", _mask_url("redis://redis.example.com:6379/0"))

    def test_masker_is_linear_on_long_input(self):
        """~70k 字符：口令 70k 且在 query 里重复一遍，掩码必须一趟做完。

        （任何退化成回溯的实现都会在这里挂死；实现只做 O(n) 的 find/rfind/replace。）
        """
        secret = "P" * 69990 + " Secret"
        url = f"redis://user:{secret}@host:6379/0?socket_timeout={secret}"
        self.assertEqual(
            _mask_url(url), "redis://user:***@host:6379/0?socket_timeout=***"
        )
        self.assertNotIn(secret, _mask_url(url))


class TestSlashInPasswordAndResidualSpans(unittest.TestCase):
    """plaita#88 r5 R1/R2：口令含 ``/`` 与「凭据只在 query 值里」两族。

    r4 的三档都要求凭据形状的 ``@`` 落在同一个 ``/`` 段内；口令他含 ``/`` 时先被
    截断 ⇒ ``_mask_url`` 的 ``if credentials is None: return value`` 原样返回。
    r2 的 ``_passwords`` 又依赖 userinfo，没有 userinfo 时整层重洗空转。
    """

    def test_r1_slash_in_password_rule_table(self):
        """R1 反例族的**精确**掩码（r4 上全部 verbatim）。"""
        expected = {
            "redis://user:p/w@host": "redis://user:***@host",
            "redis://user:pa55word/x@host": "redis://user:***@host",
            "redis://user:pw/w@host": "redis://user:***@host",
            "redis://user:p/w@host:6379/0": "redis://user:***@host:6379/0",
            "redis://user:p/w@host/0": "redis://user:***@host/0",
            "redis://user:p\u00e4ssw\u00f6rd/h@host": "redis://user:***@host",
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)
        # R1 与 R5-A 的交界：70k 的口令里含 '/' 也必须一趟掩完（O(n)）
        big = "x" * 70000
        self.assertEqual(_mask_url(f"redis://user:{big}/w@host"), "redis://user:***@host")
        # keep 形态逐字不动（它们的 '@' 是 path/query、没有凭据冒号）
        for keep in (
            "redis://host:6379/0@x",
            "redis://host/0?ref=a@b",
            "redis://host:6379/0?db=1@2",
            "redis://host:6379/0",
            "redis://host/0?x=1:2@y",
        ):
            self.assertEqual(_mask_url(keep), keep)

    def test_r2_residual_is_independent_of_userinfo(self):
        """R2：没有 userinfo 也要洗——精确掩码与 r2 的形态一致。"""
        expected = {
            # r2 掩了、r3/r4 原样返回（验证者的回归集）
            "redis://host:6379/0?db=Pr0d:x@y": "redis://host:6379/0?db=Pr0d:***@y",
            "redis://host?user:pw@x": "redis://host?user:***@x",
            "redis://host:6379/0?u=X:Y@x": "redis://host:6379/0?u=X:***@x",
            "redis://host:6379/0?u=X:Y@x&v=A:B@y": "redis://host:6379/0?u=X:***@y",
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)
        # 真正无凭据的 URL 一个字都不动（不是无差别全局替换）
        for clean in (
            "redis://host:6379/0",
            "redis://host:6379/0?socket_timeout=5",
            "redis://host:6379/0?db=2",
            "redis://[::1]:6379/0",
            "unix:///tmp/plaita-redis.sock?db=1",
        ):
            self.assertEqual(_mask_url(clean), clean)

    def test_r2_message_fields_and_cause_are_masked_without_userinfo(self):
        """R2 的四个渲染面：没有 userinfo 时拒绝消息/cause 里的口令也要洗。

        r4 的 ``_passwords`` 依赖 ``_locate_credentials``，这里返回 ``()`` ⇒
        ``_scrub(db_query, text)`` 原样回显 ``'Pr0d:Pr0d2024x'``。
        """
        # user token 与口令不共享 ≥3 子串：掩码保留 ``Acct`` 是**设计**（r2 同形），
        # 否则 ``Pr0d`` 这种「用户名恰好是口令前缀」会给出假红。
        url = "redis://host:6379/0?db=Acct:Pr0d2024x@y"
        masked = _mask_url(url)
        self.assertEqual(masked, "redis://host:6379/0?db=Acct:***@y")
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url(url)
        exc = ctx.exception
        # r7：未知名/非法取值在消息与回显的 URL 里都抹掉（``db=***``）。
        self.assertIn(_REDACTED, str(exc))
        self.assertNotIn("Pr0d2024x", str(exc))
        for kind, text in _surfaces(exc).items():
            self.assertNotIn("Pr0d2024x", text, f"{kind}: 口令原文泄露")
            for needle in _partial_needles("Pr0d2024x"):
                self.assertNotIn(needle, text, f"{kind}: 口令片段 {needle!r} 泄露")
        # cause 也是独立渲染面，同样洗过
        self.assertIsNotNone(exc.__cause__)
        self.assertNotIn("Pr0d2024x", str(exc.__cause__))

    def test_at_without_candidate_colon_fails_closed(self):
        """r5 B：``:`` 被写成 ``/``/``?``/``#`` 时也要掩，keep 逐字不动。"""
        expected = {
            "redis://user/Tr0ub4dor@host:6379/0": "redis://user/***@host:6379/0",
            "redis://user?Tr0ub4dor@host:6379/0": "redis://user?***@host:6379/0",
            "redis://user#Tr0ub4dor@host:6379/0": "redis://user#***@host:6379/0",
            "redis://user#Tr0ub4dor@host:6379/0?u=X:Y@x": "redis://user#***@host:6379/0?u=X:***@x",
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)
        # keep：空 token / 纯数字 db / '='/':' 之后（r4 已验证）
        for keep in (
            "redis://host:6379/0@x",
            "redis://host/0?ref=a@b",
            "redis://host:6379/0?db=1@2",
            "redis://host/0?x=1:2@y",
            "redis://host/@x",
        ):
            self.assertEqual(_mask_url(keep), keep)

    def test_no_at_sign_credential_token_is_masked(self):
        """r5 C：没有 ``@`` 的凭据样 token（``X:Y``）也要洗，host/port/db 保留。"""
        expected = {
            "redis://user:Tr0ub4dor127.0.0.1:679/0": "redis://user:***:679/0",
            "redis://host:6379/0?u=X:Secret1": "redis://host:6379/0?u=X:***",
            "redis://host/0/X:Secret1": "redis://host/0/X:***",
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)
        # 端口/数字 token 不是凭据（keep）
        for clean in (
            "redis://host:6379/0",
            "redis://host:6379/0?x=1:2",
            "redis://[::1]:6379/0",
            "redis://[2001:db8::1]:6380/2",
        ):
            self.assertEqual(_mask_url(clean), clean)


class TestLiteralGroundedRows(unittest.TestCase):
    """r5 R4：第二类「字面接地」行 + 它必须是**载荷**（不能只靠 parse_url 接地）。"""

    def test_literal_grounded_rows_never_leak(self):
        """每条字面接地行：掩码 + 四个渲染面 + 日志，都不出现口令的任何 ≥3 片段。

        有的行是**具名拒绝**（端口/db cast 失败、fragment），有的是**警告后忽略**
        （未知 query 名）——两条路径都是日志面，都要查。
        """
        for shape, url, literal, grounding, _why in LITERAL_GROUNDED:
            masked = _mask_url(url)
            if grounding != "message-field":
                # ``message-field`` 行不是凭据候选（掩码器按契约原样返回 URL）——
                # 它们的泄露面只有**消息/日志**，见下。
                self.assertNotIn(literal, masked, f"{shape}: _mask_url 泄露")
                for needle in _partial_needles(literal):
                    self.assertNotIn(needle, masked, f"{shape}: {needle!r} 泄露")
                if "://" in url:
                    # scheme 是**结构**、必须保留：r5 的 ``rfind("://")`` 锚点会把
                    # 口令自己的 ``://`` 当 scheme 边界，掩码后连 ``://`` 都没了
                    # （``redis://user:pa55word://x@host`` → ``redis:***@host``）。
                    # 这条断言正是 r6-scheme-anchor-is-last 这个单腿变异的杀手。
                    self.assertIn("://", masked, f"{shape}: scheme 分隔符被掩掉了")

            box: dict = {}

            def _run() -> None:
                try:
                    _parse_redis_url(url)
                except Exception as exc:  # noqa: BLE001 - 具名拒绝或底层异常
                    box["exc"] = exc

            log_lines = _capture_log_lines(_run)
            exc = box.get("exc")
            if isinstance(exc, RedisUrlError):
                # r7：非法取值/未知名/fragment 在回显的 URL 里也被抹掉，所以不再
                # 断言「掩码后的整条 URL 逐字出现」；scheme 可辨 + 掩码存在即可。
                self.assertIn("redis:", str(exc), f"{shape}: scheme 丢了")
                self.assertIn(_REDACTED, str(exc), f"{shape}: 掩码丢了")
                for kind, text in _surfaces(exc).items():
                    self.assertNotIn(literal, text, f"{shape}/{kind}: 口令原文泄露")
                    for needle in _partial_needles(literal):
                        self.assertNotIn(needle, text, f"{shape}/{kind}/{needle}")
                if exc.__cause__ is not None:
                    for kind, text in (
                        ("cause-str", str(exc.__cause__)),
                        ("cause-repr", repr(exc.__cause__)),
                    ):
                        for needle in _partial_needles(literal):
                            self.assertNotIn(needle, text, f"{shape}/{kind}/{needle}")
            else:
                # 没有具名拒绝（未知 query 参数）⇒ 必须至少有一条掩码日志
                self.assertTrue(log_lines, f"{shape}: 既没拒绝也没日志")
            for line in log_lines:
                for needle in _partial_needles(literal):
                    self.assertNotIn(needle, line, f"{shape}/log/{needle}")

    def test_literal_grounded_never_returns_input_verbatim(self):
        """red-then-green：这些行在 r4（``2f660c6``）上 ``_mask_url`` 原样返回。

        ``message-field`` 行除外：它们不是凭据候选，掩码器**按契约**原样返回
        （泄露面是消息/日志，见 ``test_literal_grounded_rows_never_leak``）。
        """
        for shape, url, literal, grounding, why in LITERAL_GROUNDED:
            if grounding == "message-field":
                continue
            masked = _mask_url(url)
            self.assertNotEqual(masked, url, f"{shape}: 原样返回（r4 的红行）")
            self.assertNotIn(literal, masked, f"{shape}: {why}")

    def test_parse_url_grounding_is_blind_to_these_rows(self):
        """为什么必须另起「字面接地」：这些行 parse_url 根本产不出 password。

        如果本断言失败（parse_url 开始能解析这些形态），说明 ``LITERAL_GROUNDED``
        可以并回 ``LEAK_SHAPES``——但**在 r4 上**它们必须是盲区，否则 r4 的表就
        不会漏掉 R1/R2。
        """
        blind = [
            row
            for row in LITERAL_GROUNDED
            if not isinstance(_redis_py_password(row[1]), str)
        ]
        self.assertGreaterEqual(len(blind), 12, "字面接地的载荷行太少")
        # 其中至少一行 r4 会 verbatim 返回（红行）
        self.assertTrue(any(why == "r4-verbatim" for *_rest, why in LITERAL_GROUNDED))

    def test_grounding_tag_is_declared(self):
        """每一行都必须显式声明接地方式（``literal`` / ``parse_url+literal``）。"""
        tags = {row[3] for row in LITERAL_GROUNDED}
        self.assertEqual(tags, {"literal", "parse_url+literal", "message-field"})
        for shape, url, literal, grounding, why in LITERAL_GROUNDED:
            self.assertTrue(
                grounding in {"literal", "parse_url+literal", "message-field"}, shape
            )
            self.assertTrue(literal, f"{shape}: 字面 needle 不能为空")
            self.assertTrue(url and why, shape)


#: r7 生成式语料的字符集：`@ : / ? # %` + 空白 + 普通字符（requirements 明文）。
_FUZZ_ALPHABET = "abzAZ09@:/?#% \t\u00a0"
#: 段长 ≥3 的片段拼接头——保证 `_secret_forms` 拆出的每一截都 ≥3 字符，
#: 这样逐形态断言不会退化成 1 字符碰撞（口令本身仍含 `@ : / ? # %` 与空白）。
_FUZZ_PARTS = "abcABCxyz019"
#: **每个**候选槽都必须在这一组结构性字符上至少被覆盖一次（逐槽断言，见
#: `TestOracleGroundedCompleteness::test_corpus_per_slot_counts_and_alphabet`）。
_FUZZ_REQUIRED_CHARS = ("@", ":", "/", "?", "#", "%", " ", "\t", "\u00a0")


def _fuzz_secret(rnd) -> str:
    """随机口令：1-3 个 ≥3 字符片段 + `_FUZZ_ALPHABET`/`://`/`@@` 等分隔符拼成。

    r7 起**没有** ``allow_at`` / ``allow_colon`` / ``safe_token`` 这类开关——r6 的
    `user==pw` 槽用 ``safe_token(allow_at=False, allow_colon=False)`` 把 `@`/`:`
    从字母表里删掉，于是 6000 行语料里那 545 个形状**没有一个含 `@`**，而 r6 的
    规则级反例恰好只在这个组合里出现。删字母表就是删覆盖：槽位形状由 `_SLOTS`
    决定，口令一律用**同一个全字母表**。
    """
    parts = [
        "".join(rnd.choice(_FUZZ_PARTS) for _ in range(rnd.randrange(3, 8)))
        for _ in range(rnd.randrange(1, 4))
    ]
    separators = [
        s
        for s in (_FUZZ_ALPHABET, _FUZZ_ALPHABET, "://", "::", "@@", "%%", "??")
        if all((ch in _FUZZ_ALPHABET or ch.isalnum()) for ch in s)
    ]
    out = []
    for i, part in enumerate(parts):
        out.append(part)
        if i + 1 < len(parts):
            out.append(rnd.choice(separators))
    return "".join(out)


#: r7 的候选槽：``(名字, 断言种类, builder(secret) -> url)``。
#:
#: 断言种类（**不是**字母表收窄，三者都用全字母表口令）：
#:
#: * ``oracle``——只按 **oracle 报出的口令**断言（用户名位是合法保留面，拿注入
#:   文本当 needle 会假红）；
#: * ``literal``——该槽**构造上**就是凭据候选位置，额外按注入文本的 ≥3 前缀/后缀
#:   断言；
#: * ``suffix``——同 ``literal``，但只查**后缀**：``user==pw`` 槽的注入文本同时
#:   写在「用户名位」，口令里带 `?`/`#`/`/` 时 netloc 在那里截断，注入文本的
#:   **前缀**正好是掩码契约要保留的用户名（``redis://19zab::cbAC0??…@host`` 的
#:   ``19zab``）。前缀因此是假红，后缀才是泄露面——r6 的洞正是后缀 ``Ccc``。
#: * ``message``——该槽不是凭据候选（掩码器按契约原样返回 URL），但按本单的
#:   default-deny 也不许在日志/消息里回显（见 `_parse_query_options`）。
#:
#: ``slash-in-password`` / ``query-value-no-at`` / ``path`` 这类「注入文本会溢出到非凭据位置」的槽在
#: r7 只做 oracle 断言：见 MIGRATION.md 的 r7 已知边界第 6/7 条。
_SLOTS = [
    ("userinfo-user", "oracle", lambda s: f"redis://{s}:pw0@host:6379/0"),
    ("userinfo-password", "literal", lambda s: f"redis://user:{s}@host:6379/0"),
    ("user==pw", "suffix", lambda s: f"redis://{s}:{s}@host:6379/0"),
    ("empty-user", "literal", lambda s: f"redis://:{s}@host"),
    ("query-value", "literal", lambda s: f"redis://host:6379/0?db=Acct:{s}@x"),
    ("query-name", "message", lambda s: f"redis://host:6379/0?{s}=1"),
    ("query-value-no-at", "oracle", lambda s: f"redis://host:6379/0?u=X:{s}"),
    ("path", "literal", lambda s: f"redis://host/0/X:{s}@y"),
    ("fragment", "literal", lambda s: f"redis://user:pwd0@host:6379/0#X:{s}@y"),
    ("no-scheme", "literal", lambda s: f"user:{s}@host:6379"),
    ("single-slash", "literal", lambda s: f"redis:/user:{s}@host"),
    ("slash-in-password", "oracle", lambda s: f"redis://user:{s}/x@host"),
    ("multi-span", "literal", lambda s: f"redis://user:{s}@host:6379/0?db=Acct:{s}@x"),
]


def _slot_corpus(rnd, count: int) -> list:
    """``count`` 个 ``(slot, secret, url)``——每个形状一个新口令、**全字母表**。

    槽位轮转（``i % len(_SLOTS)``）：每个槽拿到 ``count // len(_SLOTS)`` 个样本，
    于是「某个槽偷偷换了字母表」会立刻反映在该槽的字母表覆盖断言上。
    """
    rows = []
    for i in range(count):
        name, kind, build = _SLOTS[i % len(_SLOTS)]
        secret = _fuzz_secret(rnd)
        rows.append((name, kind, secret, build(secret)))
    return rows


def _logged_error_for(url: str) -> str:
    """真实 ``logger.error(..., exc_info=True)`` 渲染（第五个面，剥掉源码行）。"""
    box: dict = {}

    def _run() -> None:
        try:
            _parse_redis_url(url)
        except Exception as exc:  # noqa: BLE001 - 具名拒绝或底层异常
            box["exc"] = exc

    lines = _capture_log_lines(_run)
    if "exc" in box:
        lines.append(_logged_error_text(box["exc"]))
    return _exception_messages("\n".join(lines))


def _all_five_faces(url: str) -> dict:
    """五个渲染面：``_mask_url`` / ``str`` / ``repr`` / ``traceback`` / 真 ``exc_info`` 日志。"""
    box: dict = {}

    def _run() -> None:
        try:
            _parse_redis_url(url)
        except Exception as exc:  # noqa: BLE001
            box["exc"] = exc

    log_lines = _capture_log_lines(_run)
    faces = {"_mask_url": _mask_url(url)}
    exc = box.get("exc")
    if exc is not None:
        faces["str"] = str(exc)
        faces["repr"] = repr(exc)
        faces["traceback-messages"] = _exception_messages(
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        )
    else:
        faces["str"] = faces["repr"] = faces["traceback-messages"] = ""
    faces["logger-exc_info"] = _exception_messages("\n".join(log_lines)) + (
        _logged_error_text(exc) if exc is not None else ""
    )
    return faces


def _fuzz_needles(secret: str, all_substrings: bool = False) -> set:
    """口令的 needle 集：原文 + 所有长度 ≥3 的**前缀/后缀**（r6 的定义）。

    ``all_substrings=True`` 时改取**所有**长度 ≥3 的子串（r7 的完备性方向，
    要求 3）——它严格强于前缀/后缀，r6 的 `@@Ccc` 型泄露两种都抓得到。
    """
    out = {secret}
    for length in range(3, len(secret) + 1):
        if all_substrings:
            for i in range(0, len(secret) - length + 1):
                out.add(secret[i : i + length])
        else:
            out.add(secret[:length])
            out.add(secret[-length:])
    return out


#: 三个**独立 oracle** 的名字：redis-py 的 `parse_url`（队列侧真正拿去连的）、
#: `urlsplit`（WHATWG/标准库）、本仓的 `_parse_redis_url`（存储侧）。
_ORACLE_NAMES = ("redis-py", "urlsplit", "project")


def _oracle_password_of(url, name: str):
    """某个 oracle 报出的口令（没有/解析不了 → ``None``）。

    * ``redis-py``：``redis.connection.parse_url(url)["password"]``——这是**队列侧**
      真正会拿去连的口令，也是本 instrument 的主接地；
    * ``urlsplit``：标准库 netloc 的 userinfo 口令，先 ``unquote`` 成
      redis-py 的同一种写法（否则 ``%40`` 会与 ``@`` 差一个形态）；
    * ``project``：本仓 ``_parse_redis_url(url).password``。
    """
    if name == "redis-py":
        try:
            value = parse_url(url).get("password")
        except Exception:  # noqa: BLE001 - 解析失败 = 这个 oracle 说不出话
            return None
    elif name == "urlsplit":
        try:
            value = unquote(urlsplit(url).password or "")
        except Exception:  # noqa: BLE001
            return None
    else:
        try:
            value = _parse_redis_url(url).password
        except Exception:  # noqa: BLE001
            return None
    return value if isinstance(value, str) and value else None


def _oracle_passwords(url) -> dict:
    """``{oracle 名: 口令}``——只含真的报出了非空口令的 oracle。"""
    found = {}
    for name in _ORACLE_NAMES:
        value = _oracle_password_of(url, name)
        if value is not None:
            found[name] = value
    return found


def _preserved_literals(url: str, authority_span_tokens: bool = False) -> tuple:
    """掩码契约里**有意保留**的结构文本（r7 完备性判据的限定项）。

    掩码**不许**把口令的任何 ≥3 片段留在输出里；但下面这些位置是契约明确保留的
    诊断信息，一段口令子串与它们**偶然重合**不算泄露（与
    ``test_password_substring_of_scheme_or_host_is_not_a_false_red`` 同一类）：

    * scheme（``redis://``）；
    * **可证的**用户名——userinfo 里首个 ``:`` 之前没有 ``@`` 时，``urlsplit``
      的语义是「用户名 = userinfo 里首个 ``:`` 之前那段」，掩码器按契约保留它
      （``redis://user:***@host``）。携带 ``@`` 的 username 位不可证，r7 整段
      fail-closed，因此**不**进这个集合（否则 r6 的 ``@@Ccc`` 会被当成「用户名」
      而放过）；
    * ``host[:port]``（authority 里最后一个 ``@`` 之后、首个 ``/?#`` 之前）；
    * 每个**凭据候选跨度**的用户 token（``_CredentialSpan.start..mask_from``）——
      ``?db=Acct:***@x`` 的 ``Acct:`` 与 ``redis://user:***@host`` 的 ``user:``
      同属「按契约保留」。口令 ``Accba0`` 与它共享 3 字前缀 ``Acc`` 是**偶然
      重合**，不是泄露（r7 实测：没有这一项，完备性判据会产生这类假红）。

    这个集合只在完备性/字面判据里用作「偶然重合」的豁免，且豁免数被逐次计数
    （``test_oracle_grounded_no_substring_survives`` 断言它远小于总 needle 数）。
    """
    out = []
    sep = url.find("://")
    auth = sep + 3 if sep != -1 else 0
    if sep != -1:
        start = sep
        while start > 0 and (url[start - 1].isalnum() or url[start - 1] in "+-."):
            start -= 1
        if start < sep:
            out.append(url[start : sep + 3])
    stop = len(url)
    for ch in "/?#":
        idx = url.find(ch, auth)
        if idx != -1 and idx < stop:
            stop = idx
    at = url.rfind("@", auth, stop)
    userinfo = url[auth:at] if at != -1 else ""
    colon = userinfo.find(":")
    if colon > 0 and "@" not in userinfo[:colon]:
        out.append(userinfo[: colon + 1])  # 可证用户名 + ':'
    if at != -1:
        out.append(url[at + 1 : stop])  # host[:port]
    for span in factory._credential_spans(url):
        # 默认只收**authority 之外**（query/path/fragment）跨度的用户 token：它们是
        # r2 契约的保留面（``?db=Pr0d:***@y`` 的 ``Pr0d``，实测 ``Accba0`` 与
        # ``Acct:`` 共享 ``Acc`` 属偶然重合）。authority 里的跨度默认**不**作为
        # 豁免来源——r7 的 fail-closed 恰恰是「authority 里用户/口令分界不可证时
        # 整段抹掉」，把它的 ``[start, mask_from)`` 当保留面会让 r6 的 ``@@Ccc``
        # 被豁免。``authority_span_tokens=True`` 只给**合格化不变量**用（那里问的
        # 是「掩的 == 洗的」这一自洽性，不是完备性）。
        if span.start < stop and not authority_span_tokens:
            continue
        token = url[span.start : span.mask_from]
        if token:
            out.append(token)
        if authority_span_tokens:
            # authority 跨度保留的是 ``url[auth_start:mask_from]``（``_render_redacted``
            # 只掩 ``[mask_from, stop)``）——合格化不变量把这一整截也算保留面。
            out.append(url[auth : span.mask_from])
    return tuple(token for token in out if token)


def _literal_needles(secret: str, suffix_only: bool = False) -> set:
    """注入文本的字面 needle 集（``suffix_only`` 时只取原文 + ≥3 后缀）。"""
    if not suffix_only:
        return _fuzz_needles(secret)
    return {secret} | {secret[-length:] for length in range(3, len(secret) + 1)}


def _unknown_param_warning(url: str) -> str:
    """``_parse_query_options`` 的「未定义 query 参数」WARNING 行（r7 default-deny 面）。

    这是**未知名**唯一会被回显的通道（要求 7）：未知参数只警告不判死，警告本身
    也是日志。其余把 query 名当口令注入的形状（含 ``#``/``?`` 时 URL 变 fragment，
    由 fragment 拒绝分支回显）不是本单的断言面——见 MIGRATION.md r7 已知边界。
    """
    box: dict = {}

    def _run() -> None:
        try:
            _parse_redis_url(url)
        except Exception as exc:  # noqa: BLE001
            box["exc"] = exc

    lines = [
        line
        for line in _capture_log_lines(_run)
        if "未定义的 query 参数" in line
    ]
    return _exception_messages("\n".join(lines))


def _literal_leaks(secret, faces, preserved, suffix_only=False):
    """注入文本的字面 needle 在渲染面上的存活清单（豁免两类**已知边界**）。

    1. 与 ``preserved`` 里结构文本**偶然重合**的子串（r7 实测：``?db=Acct:***@x``
       的 ``Acct:`` 与口令 ``Accba0`` 共享 ``Acc``）；
    2. **纯数字** needle：``?db=Acct:019@x`` 的 ``:019@`` 与 ``host:port`` 局部
       同形，按 r4 已验证的端口判据是 keep（MIGRATION 已知边界第 3/4 条同族）。
    """
    hits = []
    for needle in _literal_needles(secret, suffix_only=suffix_only):
        if needle[:1].isdigit():
            continue  # 数字打头 = 与 host:port/db 局部同形（MIGRATION 已知边界第 3 条）
        if any(needle in token for token in preserved):
            continue
        for face, text in faces.items():
            if needle in text:
                hits.append((needle, face))
    return hits


def _cause_faces(exc: BaseException) -> dict:
    """``__cause__`` 替身（``from exc`` 的独立渲染面）的 str/repr。"""
    cause = getattr(exc, "__cause__", None)
    if cause is None:
        return {}
    return {"cause-str": str(cause), "cause-repr": repr(cause)}


def _shape_faces(url: str) -> dict:
    """``_all_five_faces(url)`` + ``__cause__`` 的 str/repr（若存在）。"""
    faces = _all_five_faces(url)
    box: dict = {}

    def _run() -> None:
        try:
            _parse_redis_url(url)
        except Exception as exc:  # noqa: BLE001
            box["exc"] = exc

    _capture_log_lines(_run)
    exc = box.get("exc")
    if exc is not None:
        faces.update(_cause_faces(exc))
    return faces


class TestEnumerateAllSpans(unittest.TestCase):
    """plaita#88 r6：把「找**那一个**凭据跨度」换成**枚举全部跨度**。

    r3/r4/r5 三次复发的是同一个缺陷类——每轮只找「the credential span」：
    r3 只认字面 ``://`` 之后的 authority、r4 的兜底按 userinfo 有无分档、r5 的
    ``_global_credential_span`` 拿 ``rfind("://")`` 当锚点且只看第一个候选冒号。
    这里钉住结构性后果：**每一个**候选跨度都被掩，掩码与 ``_passwords`` 出一门。
    """

    def test_enumerate_all_spans_rule_table(self):
        """R1/R2 及多跨度的精确掩码（r5 上全部泄露）。"""
        expected = {
            # R1：口令自己的 '://' 不再是 scheme 边界
            "redis://user:pa55word://x@host": "redis://user:***@host",
            "redis://user:Qz7Wm3Kp9://w@host": "redis://user:***@host",
            # R2：第二条跨度（query 值）与 userinfo 一起掩
            "redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1@x": (
                "redis://user:***@host:6379/0?db=Acct:***@x"
            ),
            "redis://:Pr0d2024x@host:6379/0?db=Acct:Secret1@x": (
                "redis://:***@host:6379/0?db=Acct:***@x"
            ),
            "redis://host:6379/0?u=X:Y@x&v=A:B@y": (
                "redis://host:6379/0?u=X:***@y"
            ),
            # 没有 '@' 的第二条跨度（r5 C 只看第一个候选冒号）
            "redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1": (
                "redis://user:***@host:6379/0?db=Acct:***"
            ),
            "redis://host:6379/0?u=X:Secret1": "redis://host:6379/0?u=X:***",
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)

    def test_enumerate_all_spans_never_returns_input_verbatim(self):
        """R1/R2 族在五个面上都不原样回显（r5 上 `_mask_url` 就漏）。"""
        for url, secret in (
            ("redis://user:pa55word://x@host", "pa55word"),
            ("redis://user:Qz7Wm3Kp9://w@host", "Qz7Wm3Kp9"),
            ("redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1@x", "Secret1"),
            ("redis://:Pr0d2024x@host:6379/0?db=Acct:Secret1@x", "Secret1"),
            ("redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1", "Secret1"),
        ):
            masked = _mask_url(url)
            self.assertNotEqual(masked, url, url)
            self.assertNotIn(secret, masked, url)
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            for kind, text in _surfaces(ctx.exception).items():
                self.assertNotIn(secret, text, f"{url}/{kind}")
                for needle in _partial_needles(secret):
                    self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")
            self.assertNotIn(secret, _logged_error_for(url), url)

    def test_r7_authority_userinfo_ambiguity_rule_table(self):
        """r7 规则级反例：authority 里候选冒号**之前还有 `@`** ⇒ 整段 userinfo fail-closed。

        ``redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0`` 的三个 oracle
        （``parse_url`` / ``urlsplit`` / ``_parse_redis_url``）都报
        ``zB0@@cB9cB@@Ccc``；r6 只把候选冒号**之后**那截当口令，用户段里
        **最后一个 `@` 之后**的 ``Ccc`` 没有任何跨度覆盖，输出因此是
        ``redis://***@@***@@Ccc:***@host:6379/0``——同一缺陷类的第四次复发。
        r7 的判据是结构性的：分界不可证 ⇒ 整段 userinfo 一个字符都不留。
        """
        expected = {
            "redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0": (
                "redis://***@host:6379/0"
            ),
            "redis://Q7w@E4r@T5y:Q7w@E4r@T5y@host:6379/0": "redis://***@host:6379/0",
            # 分界**可证**（候选冒号之前没有 `@`）的形态逐字不变
            "redis://user:p@ssword@redis.internal:0": (
                "redis://user:***@redis.internal:0"
            ),
            "redis://user:@Pr0d2024x@redis.internal:0": (
                "redis://user:***@redis.internal:0"
            ),
            "redis://user:Super Secret@redis.internal:0": (
                "redis://user:***@redis.internal:0"
            ),
        }
        for url, want in expected.items():
            self.assertEqual(_mask_url(url), want, url)
        for url, secret in (
            (
                "redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0",
                "zB0@@cB9cB@@Ccc",
            ),
            ("redis://Q7w@E4r@T5y:Q7w@E4r@T5y@host:6379/0", "Q7w@E4r@T5y"),
        ):
            for oracle in _ORACLE_NAMES:
                self.assertEqual(
                    _oracle_password_of(url, oracle), secret, f"{url}/{oracle}"
                )
            for kind, text in _all_five_faces(url).items():
                for needle in _fuzz_needles(secret):
                    self.assertNotIn(needle, text, f"{url}/{kind}/{needle}")

    def test_mask_and_passwords_share_one_enumeration(self):
        """``_mask_url`` 与 ``_passwords`` 都只经 ``_credential_spans``。

        源码级防漂移：掩码层不许自己长出一套「找跨度」的启发式。
        """
        import pathlib
        import re

        source = pathlib.Path(factory.__file__).read_text(encoding="utf-8")
        self.assertEqual(
            len(re.findall(r"^def _credential_spans\(", source, re.M)), 1
        )
        for name in ("_mask_url", "_passwords"):
            start = source.index(f"def {name}(")
            end = source.index("\ndef ", start + 1)
            body = source[start:end]
            self.assertIn("_credential_spans(", body, f"{name} 没走同一个枚举")
            self.assertIn("_redaction_forms(", body, f"{name} 没走同一套 needle")

    def test_consistency_invariant_on_fuzz_corpus(self):
        """r7：**合格化**的一致性不变量 + **完备性**（主验收）一起钉。

        不变量（合格化版本——r6 的无限定说法是**假**的：验证者给出 432 个反例，
        全是「口令形态与**有意保留的结构 token** 撞车」，例如
        ``redis://user:Q7w:9:6379@host:6379/0`` 的 ``_passwords`` 含 ``6379`` 而
        host 端口 ``6379`` 本来就该留下）：

        * ``_passwords(s) != ()`` ⟹ ``_mask_url(s)`` 不含其中任何**不是**
          scheme/host/port/db/可证用户名 子串的形态、且 ``!= s``；
        * 反之 ``_mask_url(s) != s`` ⟹ ``_passwords(s) != ()``。

        **完备性（主）**：每一个注入到凭据候选槽的口令，其任何 ≥3 前缀/后缀都不
        能出现在五个渲染面上。自洽（「我掩的 == 我洗的」）**不蕴含**完备——r6 的
        `_passwords` 与 `_mask_url` 完全自洽，仍然把 ``@@Ccc`` 留在了
        ``redis://***@@***@@Ccc:***@host`` 里。所以这里两条都断言，且完备性那条
        的 needle 用**全字母表**语料（r6 的语料把这个槽的 `@` 删掉了）。
        """
        rnd = random.Random(88)
        corpus = _slot_corpus(rnd, 2600)
        checked = 0
        for slot, kind, secret, url in corpus:
            passwords = _passwords(url)
            masked = _mask_url(url)
            preserved = _preserved_literals(url, authority_span_tokens=True)
            for form in passwords:
                if len(form) < 3:
                    # 口令里带 `?`/`#`/`/` 时 `_secret_forms` 会切出 1-2 字符的
                    # 残片（`/?`、`0`）；它们仍参与重洗，但逐形态断言会把
                    # scheme/host 的偶然子串算成假红（见 false-red 用例）。
                    continue
                if any(form in token or token in form for token in preserved):
                    continue  # 与有意保留的结构 token 撞车（合格化的那一半）
                self.assertNotIn(form, masked, f"{slot}/{url}: 形态 {form!r} 活了下来")
            if kind == "literal":
                checked += 1
                self.assertNotEqual(masked, url, f"{slot}/{url}: 候选槽却原样返回")
                faces = _cached_faces(url)
            elif kind == "suffix":
                checked += 1
                self.assertNotEqual(masked, url, f"{slot}/{url}: 候选槽却原样返回")
                faces = _cached_faces(url)
            elif kind == "message":
                # 该槽不是凭据候选 ⇒ `_mask_url` 按契约原样返回（掩码器不负责
                # 认 query 名）；但按本单的 default-deny，未知名 WARNING 里也不许
                # 回显（要求 7）。警告没发出（URL 先撞上 fragment 等分支）的形状
                # 不计入，计入数在下限断言里。
                warning = _unknown_param_warning(url)
                if not warning:
                    continue
                checked += 1
                faces = {"unknown-param-warning": warning}
            else:
                continue
            for needle, face in _literal_leaks(
                secret,
                faces,
                preserved,
                suffix_only=kind == "suffix",
            ):
                self.fail(f"{slot}/{face}/{needle} 泄露 URL={url!r}")
        self.assertGreaterEqual(checked, 1700, "候选槽样本太少")

    def test_converse_holds_on_clean_and_malformed_shapes(self):
        """反向：``_mask_url(s) != s`` ⟺ 至少识别出一条口令形态。"""
        rnd = random.Random(20261015)
        rows = [
            "redis://host:6379/0",
            "redis://host:6379/0?db=2",
            "redis://host:6379/0?socket_timeout=5",
            "redis://[::1]:6379/0",
            "redis://[2001:db8::1]:6380/2",
            "unix:///tmp/plaita-redis.sock?db=1",
            "redis://host:6379/0@x",
            "redis://host/0?ref=a@b",
            "redis://host:6379/0?db=1@2",
            "redis://host/0?x=1:2@y",
            "redis://host/@x",
        ]
        rows += [url for _slot, kind, _s, url in _slot_corpus(rnd, 520) if kind == "literal"]
        for url in rows:
            masked = _mask_url(url)
            passwords = _passwords(url)
            self.assertEqual(
                masked != url,
                bool(passwords),
                f"{url}: mask 变了={masked != url} 但 passwords={passwords}",
            )


#: r7 语料（含逐面渲染结果）的**进程内缓存**：多个断言共用同一批形状，运行时不翻倍。
_CORPUS_CACHE: dict = {}
_FACES_CACHE: dict = {}


def _cached_corpus(size: int) -> list:
    if size not in _CORPUS_CACHE:
        _CORPUS_CACHE[size] = _slot_corpus(random.Random(88), size)
    return _CORPUS_CACHE[size]


def _cached_faces(url: str) -> dict:
    if url not in _FACES_CACHE:
        _FACES_CACHE[url] = _shape_faces(url)
    return _FACES_CACHE[url]


class TestMaskerGenerativeProperty(unittest.TestCase):
    """r7 生成式性质测试：验收判据从「自洽」改为「**完备**」，needle 由 oracle 选。

    r6 的性质测试拿**注入的文本**当 needle——可 r6 的 `user==pw` 槽用的是
    ``safe_token(allow_at=False, allow_colon=False)``，字母表里 `@`/`:` 被删掉，
    于是语料自己选出来的 needle 里**不含** `Ccc` 这类后缀，洞就一直看不见。
    r7 反过来：口令由**独立 oracle**（`redis.connection.parse_url`，并用
    `urlsplit` 与 `_parse_redis_url` 交叉核对）报出，断言它的任何 ≥3 前缀/后缀
    与**所有** ≥3 子串都不出现在五个渲染面（+ `__cause__` 的 str/repr）上。
    """

    #: 形状数（远超 r6 的 3000；语料是全字母表，实测耗时见 MIGRATION.md）
    MIN_SHAPES = 9000

    def test_generated_secret_in_every_candidate_position_never_leaks(self):
        """注入文本回退网：候选槽注入的口令，五个面 + 日志上都不许有 ≥3 片段。

        （oracle 接地的那条在
        ``test_oracle_grounded_prefix_suffix_never_leaks``；这条覆盖 oracle 说
        不出话的槽——query 名/值、path、无 scheme、单斜杠——它们由「写 URL 的人
        知道口令在哪」的字面接地兜。）
        """
        shapes = 0
        for slot, kind, secret, url in _cached_corpus(self.MIN_SHAPES):
            if kind == "oracle":
                continue
            shapes += 1
            if kind in ("literal", "suffix"):
                faces = _cached_faces(url)
            else:  # message：只查未知名 WARNING 面（要求 7）
                warning = _unknown_param_warning(url)
                if not warning:
                    continue
                faces = {"unknown-param-warning": warning}
            for needle, face in _literal_leaks(
                secret,
                faces,
                _preserved_literals(url),
                suffix_only=kind == "suffix",
            ):
                self.fail(f"{slot}/{face}/{needle} 泄露 URL={url!r}")
        self.assertGreaterEqual(shapes, self.MIN_SHAPES * 7 // 10, "候选槽样本太少")

    def test_oracle_grounded_prefix_suffix_never_leaks(self):
        """**主验收（r7）**：oracle 报出的口令，任何 ≥3 前缀/后缀都不在任何面上。

        needle 来自 oracle 而不是注入文本——这才是能抓住 r6 那个洞的 instrument：
        ``redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0`` 的三个 oracle 都报
        ``zB0@@cB9cB@@Ccc``，r6 的输出里留着后缀 ``Ccc``。
        """
        grounded = needles_total = excused = 0
        for slot, _kind, _secret, url in _cached_corpus(self.MIN_SHAPES):
            passwords = _oracle_passwords(url)
            if not passwords:
                continue
            grounded += 1
            preserved = _preserved_literals(url)
            faces = _cached_faces(url)
            for name, password in passwords.items():
                for needle in _fuzz_needles(password):
                    needles_total += 1
                    if any(needle in token for token in preserved):
                        excused += 1
                        continue
                    for face, text in faces.items():
                        if needle in text:
                            self.fail(
                                f"{slot}/{name}/{face}: oracle 口令片段 {needle!r} "
                                f"（口令 {password!r}）活了下来 URL={url!r}"
                            )
        self.assertGreaterEqual(grounded, 2000, "oracle 接地的形状太少")
        self.assertLess(
            excused, needles_total // 10, "豁免比例过高——完备性被削弱了"
        )

    def test_oracle_grounded_no_substring_survives(self):
        """**完备性（r7，要求 3）**：oracle 口令的**所有** ≥3 子串都不许活下来。

        比前缀/后缀严格（r6 的 ``@@Ccc`` 型泄露两种都抓，前缀/后缀只抓后缀那一半）。
        唯一的限定项是 ``_preserved_literals``——与**有意保留**的
        scheme/可证用户名/host/db 文本偶然重合的子串不算泄露（否则
        ``redis://bAByC::01yC:pw0@host`` 的 ``yC:`` 会被误报：它落在保留的用户名
        ``bAByC:`` 里）。豁免数被计数并断言远小于总 needle 数，收窄不可能悄悄发生。
        """
        grounded = needles_total = excused = 0
        for slot, _kind, _secret, url in _cached_corpus(self.MIN_SHAPES):
            passwords = _oracle_passwords(url)
            if not passwords:
                continue
            grounded += 1
            preserved = _preserved_literals(url)
            faces = _cached_faces(url)
            for name, password in passwords.items():
                for needle in _fuzz_needles(password, all_substrings=True):
                    needles_total += 1
                    if any(needle in token for token in preserved):
                        excused += 1
                        continue
                    for face, text in faces.items():
                        if needle in text:
                            self.fail(
                                f"{slot}/{name}/{face}: 完备性违反 {needle!r} "
                                f"（口令 {password!r}）URL={url!r}"
                            )
        self.assertGreaterEqual(grounded, 2000)
        self.assertLess(
            excused, needles_total // 10, "豁免比例过高——完备性被削弱了"
        )

    def test_oracle_cross_check_agrees(self):
        """三个 oracle 互相交叉核对：能同时报出口令时必须**逐字相同**。"""
        agreed = 0
        for _slot, _kind, _secret, url in _cached_corpus(2600):
            passwords = _oracle_passwords(url)
            if len(passwords) < 2:
                continue
            self.assertEqual(
                len(set(passwords.values())),
                1,
                f"{url}: oracle 口径不一致 {passwords}",
            )
            agreed += 1
        self.assertGreater(agreed, 500)

    def test_generated_corpus_actually_exercises_the_enumeration(self):
        """防退化：语料必须真的覆盖五类候选（userinfo/query/path/fragment/无 '@'）。"""
        rows = _cached_corpus(1600)
        spans_per_shape = [len(factory._credential_spans(url)) for _s, _k, _x, url in rows]
        with_spans = sum(1 for n in spans_per_shape if n >= 1)
        self.assertGreaterEqual(
            with_spans, len(rows) * 9 // 10, "认不出跨度的形状太多了"
        )
        self.assertGreaterEqual(max(spans_per_shape), 2, "语料里没有多跨度形状")
        urls = [url for _s, _k, _x, url in rows]
        secrets = [s for _s, _k, s, _u in rows]
        # 无 '@' 的候选（r5 C）也要在语料里
        self.assertTrue(any("@" not in url for url in urls), "语料里没有无 '@' 的形状")
        # 含 '://' 的口令也要在语料里（r5 R1）
        self.assertTrue(any("://" in s for s in secrets), "语料里没有含 '://' 的口令")
        # 含 '@' / ':' / '/' / '?' / '#' / '%' / 空白的口令都要在语料里
        for ch in _FUZZ_REQUIRED_CHARS:
            self.assertTrue(
                any(ch in s for s in secrets), f"语料里没有含 {ch!r} 的口令"
            )

    def test_corpus_per_slot_counts_and_alphabet(self):
        """**逐槽**记账（要求 2）：样本数 + 全字母表覆盖 + oracle 接地数。

        任何「为让语料变绿而收窄某个槽的字母表」都会在这里红——r6 那个
        ``user==pw`` 槽（``safe_token(allow_at=False, allow_colon=False)``）会在
        `@`/`:` 两列上直接暴露。计数同时打出来（``-s`` 可见），便于把「某槽被
        静默删掉」也变成可核查的事实。
        """
        rows = _cached_corpus(self.MIN_SHAPES)
        per_slot: dict = {}
        for slot, kind, secret, url in rows:
            entry = per_slot.setdefault(
                slot, {"kind": kind, "n": 0, "oracle": 0, "alphabet": set()}
            )
            entry["n"] += 1
            entry["alphabet"] |= set(secret)
            if _oracle_passwords(url):
                entry["oracle"] += 1
        self.assertEqual(set(per_slot), {name for name, _k, _b in _SLOTS})
        base, extra = divmod(self.MIN_SHAPES, len(_SLOTS))
        expected = {name: base + (1 if i < extra else 0) for i, (name, _k, _b) in enumerate(_SLOTS)}
        for name, _kind, _build in _SLOTS:
            entry = per_slot[name]
            self.assertEqual(entry["n"], expected[name], f"{name}: 样本数变了")
            missing = [c for c in _FUZZ_REQUIRED_CHARS if c not in entry["alphabet"]]
            self.assertEqual(missing, [], f"{name}: 字母表缺 {missing!r}（被收窄了）")
            print(
                f"[r7-corpus] {name:18s} kind={entry['kind']:8s} "
                f"n={entry['n']:5d} oracle_grounded={entry['oracle']:5d}"
            )
        # oracle 说得出话的槽必须有接地样本；说不出话的槽也必须在册（不许删）。
        self.assertGreaterEqual(
            per_slot["userinfo-password"]["oracle"], base // 3
        )
        self.assertGreaterEqual(per_slot["user==pw"]["oracle"], base // 3)
        self.assertGreaterEqual(per_slot["fragment"]["oracle"], base)


class TestContextAndTotality(unittest.TestCase):
    """r5 R3（``__context__``）+ A（``_mask_url`` 链 total）+ 位置反例。"""

    #: 四个 ``raise … from _sanitized_cause(...)`` 站点各一个触发串。
    #: ``__context__`` 挂着**原始**异常（``str()`` 带明文），``raise … from`` 盖不住。
    SANITIZED_CAUSE_SITES = [
        ("urlsplit ValueError", "redis://user:Pr0d2024x@[::1:6379/0", "IPv6"),
        ("port ValueError", "redis://user:Pr0d2024x@host:notaport", "Port"),
        (
            "db query ValueError",
            "redis://user:Pr0d2024x@host:6379/0?db=x",
            "db",
        ),
        (
            "query value ValueError",
            "redis://user:Pr0d2024x@host:6379/0?socket_timeout=abc",
            "socket_timeout",
        ),
    ]

    def test_context_never_holds_the_original_exception(self):
        """R3：每个 ``from _sanitized_cause`` 站点，``__context__`` 都不含明文。"""
        for shape, url, wording in self.SANITIZED_CAUSE_SITES:
            with self.assertRaises(RedisUrlError) as ctx:
                _parse_redis_url(url)
            exc = ctx.exception
            self.assertIsNotNone(exc.__cause__, f"{shape}: 洗过的 cause 替身丢了")
            # 核心断言：隐式 context 被摘掉，原始异常（带明文）不再挂在链上
            self.assertIsNone(exc.__context__, f"{shape}: __context__ 仍挂着原始异常")
            self.assertNotIn("Pr0d2024x", str(exc.__cause__), shape)
            # 消息仍可诊断（原因文案保留）
            self.assertIn(wording.lower(), (str(exc) + str(exc.__cause__)).lower(), shape)
            for kind, text in _surfaces(exc).items():
                for needle in _partial_needles("Pr0d2024x"):
                    self.assertNotIn(needle, text, f"{shape}/{kind}/{needle}")
            # 任何一个 ``__context__`` 后代（如果存在）也不能带明文
            node = exc.__context__
            while node is not None:
                self.assertNotIn("Pr0d2024x", str(node), shape)
                node = node.__context__

    def test_context_would_leak_without_the_clearing_wrapper(self):
        """R3 的 red-then-green 对照：**没有**包装层时 ``__context__`` 带明文。

        端口值恰好等于口令的形态：``urlsplit`` 的 ``ValueError`` 文案里就是那个口令。
        直接调实现体 ``_parse_redis_url_checked``（= r4 的行为）时 ``__context__``
        挂着它；走生产包装层 ``_parse_redis_url`` 时被摘成 ``None``。
        """
        url = "redis://user:Pr0d2024x@host:Pr0d2024x"
        with self.assertRaises(RedisUrlError) as raw:
            factory._parse_redis_url_checked(url)
        self.assertIsNotNone(raw.exception.__context__, "对照组成立的前提")
        self.assertIn("Pr0d2024x", str(raw.exception.__context__))

        with self.assertRaises(RedisUrlError) as wrapped:
            _parse_redis_url(url)
        self.assertIsNone(wrapped.exception.__context__)

    def test_every_source_sanitized_cause_site_is_covered(self):
        """防漂移：源码里 ``from _sanitized_cause(`` 的站点数 == 本测试的触发数。"""
        import pathlib
        import re

        source = pathlib.Path(factory.__file__).read_text(encoding="utf-8")
        self.assertEqual(
            len(re.findall(r"from _sanitized_cause\(", source)),
            len(self.SANITIZED_CAUSE_SITES),
        )

    def test_masker_chain_is_total_on_surrogateescape_strings(self):
        """r5 A：任意 ``str``（含 surrogateescape 孤立代理）都不得抛。"""
        import random

        rnd = random.Random(88)
        chain = {
            "_mask_url": factory._mask_url,
            "_passwords": factory._passwords,
            "_scrub": lambda u: factory._scrub(u, u),
            "_safe_reason": lambda u: factory._safe_reason(ValueError(f"bad {u}"), u),
            "_sanitized_cause": lambda u: factory._sanitized_cause(
                ValueError(f"bad {u}"), u
            ),
        }
        shapes = [
            "redis://user:{b}@127.0.0.1:679/0",
            "redis://user:{b}/x@127.0.0.1:679/0",
            "redis://host:6379/0?db={b}",
            "redis://host:6379/0?u=X:{b}@x",
            "redis://user:Tr0ub4dor{b}@host:6379/0",
            "{b}",
        ]
        for url in SURROGATE_URLS:
            for name, fn in chain.items():
                try:
                    fn(url)
                except Exception as exc:  # noqa: BLE001
                    self.fail(f"{name}({url!r}) 抛了 {type(exc).__name__}: {exc}")
        for _ in range(400):
            blob = bytes(rnd.randrange(256) for _ in range(rnd.randrange(1, 6))).decode(
                "utf-8", "surrogateescape"
            )
            url = rnd.choice(shapes).replace("{b}", blob)
            for name, fn in chain.items():
                try:
                    fn(url)
                except Exception as exc:  # noqa: BLE001
                    self.fail(f"{name}({url!r}) 抛了 {type(exc).__name__}: {exc}")
            masked = factory._mask_url(url)
            if "Tr0ub4dor" in url:
                self.assertNotIn("Tr0ub4dor", masked, url)

    def test_surrogateescape_does_not_leak_the_password(self):
        """A 的「不泄」半边：代理字符口令的 ≥3 可打印片段不得出现在四个渲染面。"""
        literal = "Tr0ub4dor"
        for url in (
            f"redis://user:{literal}\udcff@host:6379/0",
            f"redis://user:{literal}\udcff/x@host:6379/0",
            f"redis://host:6379/0?u=X:{literal}\udcff@y",
        ):
            masked = _mask_url(url)
            self.assertNotIn(literal, masked, url)
            for needle in _partial_needles(literal):
                self.assertNotIn(needle, masked, f"{url}: {needle!r}")


class TestQueryOptionsPassThrough(unittest.TestCase):
    """r2：「redis-py 认的 query 参数」必须真的进客户端 kwargs，不再一律拒绝。"""

    #: 透传后能在存储客户端 ``connection_kwargs()`` 里直接观察到的参数
    PASSTHROUGH = [
        ("socket_timeout", "redis://user:pw@host:6379/0?socket_timeout=5"),
        ("socket_connect_timeout", "redis://host:6379/0?socket_connect_timeout=1.5"),
        ("socket_keepalive", "redis://host:6379/0?socket_keepalive=true"),
        ("socket_read_size", "redis://host:6379/0?socket_read_size=32768"),
        ("health_check_interval", "redis://host:6379/0?health_check_interval=30"),
        ("protocol", "redis://host:6379/0?protocol=2"),
        ("legacy_responses", "redis://host:6379/0?legacy_responses=true"),
        ("retry_on_error", "redis://host:6379/0?retry_on_error=ConnectionError"),
        ("ssl_min_version", "rediss://host:6380/0?ssl_min_version=771"),
        ("ssl_check_hostname", "rediss://host:6380/0?ssl_check_hostname=false"),
        (
            "ssl_include_verify_flags",
            "rediss://host:6380/0?ssl_include_verify_flags=VERIFY_X509_PARTIAL_CHAIN",
        ),
        (
            "ssl_exclude_verify_flags",
            "rediss://host:6380/0?ssl_exclude_verify_flags=VERIFY_X509_STRICT",
        ),
    ]

    #: 客户端/Pool 自己消化：值不在 ``connection_kwargs``，但确实生效
    CONSUMED_ELSEWHERE = [
        ("max_connections", "redis://host:6379/0?max_connections=10"),
        ("retry_on_timeout", "redis://host:6379/0?retry_on_timeout=true"),
    ]

    def _storage_client(self, url: str) -> Redis:
        storage = create_storage_component(
            "redis", "execution", tenant_routing=True, redis_url=url
        )
        return storage._current_storage().client

    def test_supported_query_params_reach_client_kwargs(self):
        for param, url in self.PASSTHROUGH:
            with self.subTest(param=param):
                kwargs = self._storage_client(url).connection_pool.connection_kwargs
                queue = Redis.from_url(url).connection_pool.connection_kwargs
                self.assertIn(param, kwargs)
                # 与队列侧逐值一致（含 redis-py 的 int/float/bool/VerifyFlags 转换）
                self.assertEqual(kwargs[param], queue[param])

    def test_consumed_query_params_still_build_a_client(self):
        for param, url in self.CONSUMED_ELSEWHERE:
            with self.subTest(param=param):
                self.assertIsNotNone(self._storage_client(url))

    def test_max_connections_reaches_the_pool(self):
        client = self._storage_client("redis://host:6379/0?max_connections=10")
        self.assertEqual(client.connection_pool.max_connections, 10)

    def test_combined_production_url_no_longer_crashes(self):
        """验证者实测的生产形态：``?socket_timeout=5&health_check_interval=30``。"""
        target = _parse_redis_url(
            "redis://user:sup3rsecret@host:6379/0?socket_timeout=5&health_check_interval=30"
        )
        self.assertEqual(target.query_options["socket_timeout"], 5.0)
        self.assertEqual(target.query_options["health_check_interval"], 30)

    def test_unknown_query_param_is_ignored_not_fatal(self):
        target = _parse_redis_url("redis://host:6379/0?foo=bar")
        self.assertEqual((target.host, target.port, target.db), ("host", 6379, 0))
        # 未知名字无法透传（redis.Redis.__init__ 没有 **kwargs），但不判死
        self.assertNotIn("foo", target.client_kwargs())

    def test_invalid_value_for_known_param_is_still_refused(self):
        with self.assertRaises(RedisUrlError) as ctx:
            _parse_redis_url("redis://host:6379/0?ssl_min_version=abc")
        self.assertIn("ssl_min_version", str(ctx.exception))

    def test_passthrough_table_covers_every_redis_py_query_parser(self):
        """枚举不许漂移：本模块认的参数名必须与 redis-py 的表**逐名**对齐。

        redis-py 将来新增 query 参数时这条会红，提醒把它加进 ``_QUERY_PARAMS``
        （透传）或 ``_UNSUPPORTED_QUERY_PARAMS``（具名拒绝）。
        """
        from redis.connection import URL_QUERY_ARGUMENT_PARSERS

        ours = set(factory._QUERY_PARAMS) | {"db"} | set(factory._UNSUPPORTED_QUERY_PARAMS)
        self.assertEqual(ours, set(URL_QUERY_ARGUMENT_PARSERS))


class TestFactoryCallerBehaviour(unittest.TestCase):
    """入口行为：解析器是 execution/flow 存储连接参数的唯一来源。"""

    def test_execution_and_flow_go_through_parser(self):
        calls = []
        original = factory._parse_redis_url

        def spy(url=None):
            calls.append(url)
            return original(url)

        factory._parse_redis_url = spy
        try:
            create_storage_component("redis", "execution", tenant_routing=True, redis_url="redis://h1:6379/0")
            create_storage_component("redis", "flow", tenant_routing=True, redis_url="redis://h2:6379/0")
        finally:
            factory._parse_redis_url = original
        self.assertEqual(calls, ["redis://h1:6379/0", "redis://h2:6379/0"])

    def test_malformed_refused_before_any_client_is_built(self):
        """具名失败发生在建客户端之前（fail-closed，不是连接期的偶然报错）。"""
        import plaita.storage.redis as storage_mod

        built = []
        real_client = storage_mod.redis.Redis

        def spy(*args, **kwargs):
            built.append(kwargs)
            return real_client(*args, **kwargs)

        storage_mod.redis.Redis = spy
        try:
            for url in ("http://h:6379", "redis://", "redis://host:notaport"):
                for component_type in ("execution", "flow"):
                    with self.subTest(url=url, component_type=component_type):
                        with self.assertRaises(RedisUrlError):
                            create_storage_component(
                                "redis", component_type, redis_url=url
                            )
        finally:
            storage_mod.redis.Redis = real_client
        self.assertEqual(built, [])

    def test_storage_and_queue_end_up_on_same_endpoint(self):
        """同一个 URL → 同一个端点：存储侧客户端与队列 ``Redis.from_url`` 等价。"""
        for url in ACCEPTED:
            with self.subTest(url=url):
                storage = create_storage_component(
                    "redis", "execution", tenant_routing=True, redis_url=url
                )
                inner = storage._current_storage()
                self.assertEqual(_effective(inner.client), _effective(Redis.from_url(url)))

    def test_no_more_second_endpoint_for_rediss(self):
        """历史缺陷：``rediss://redis.internal:6380/0`` 的存储侧被改写成明文 localhost。"""
        storage = create_storage_component(
            "redis", "flow", tenant_routing=True, redis_url="rediss://redis.internal:6380/0"
        )
        pool = storage._current_storage().client.connection_pool
        self.assertEqual(pool.connection_kwargs["host"], "redis.internal")
        self.assertEqual(pool.connection_kwargs["port"], 6380)
        self.assertEqual(pool.connection_class.__name__, "SSLConnection")

    def test_non_tenant_execution_and_flow_storage(self):
        """非租户路由路径（库内直接调用）同样走解析器、同端点。"""
        from plaita.storage.redis import RedisExecutionStorage, RedisFlowStorage

        for component_type, cls in (("execution", RedisExecutionStorage), ("flow", RedisFlowStorage)):
            with self.subTest(component_type=component_type):
                url = "redis://user:pw@host:6380/4"
                storage = create_storage_component("redis", component_type, redis_url=url)
                self.assertIsInstance(storage, cls)
                self.assertEqual(_effective(storage.client), _effective(Redis.from_url(url)))

    def test_subscription_branch_untouched(self):
        """边界：subscription 分支本来就走原始 URL（redis-py 解析），不在本单范围。"""
        from plaita.event.redis import RedisEventSubscriptionStorage

        storage = create_storage_component(
            "redis", "subscription", redis_url="redis://host:6379/0"
        )
        self.assertIsInstance(storage, RedisEventSubscriptionStorage)
        self.assertEqual(storage.redis_url, "redis://host:6379/0")


class TestSingleParser(unittest.TestCase):
    """全仓只有一个 Redis URL 解析器（#88 的根因是存储侧另有一套）。"""

    def test_parse_redis_url_defined_once(self):
        import pathlib

        root = pathlib.Path(__file__).resolve().parents[2] / "plaita"
        hits = [
            path
            for path in root.rglob("*.py")
            if "def _parse_redis_url" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual([p.name for p in hits], ["factory.py"])


if __name__ == "__main__":
    unittest.main()
