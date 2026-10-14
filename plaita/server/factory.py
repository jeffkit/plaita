"""
统一的存储组件和事件总线工厂

生产控制面路径：execution / flow / EventFilter 队列侧以 **redis**（或 memory 单测）为准。
``db`` / SQLAlchemy 标记为 experimental，需显式
``PLAITA_ALLOW_EXPERIMENTAL_DB=1`` 才可创建（且 execution/flow 的 db 仍因 sync/async
契约断裂而被拒绝）。

``_parse_redis_url`` 的拒绝文案一律经 ``_mask_url`` 渲染（plaita#88 的凭据掩码）。
掩码的**结构性契约**（r6 起，r7 加严）：

* ``_credential_spans(text)`` **枚举**字符串里每一个凭据候选跨度——冒号候选
  （非端口冒号 + 配对的 ``@``）、没有 ``@`` 的凭据 token、``@`` 前没有候选冒号的
  fail-closed token，以及 no-scheme 的粗档；**没有**「the credential span」这种东西。
  r3/r4/r5 三次复发的同一类缺陷都源于「只找那一个」；
* **r7**：authority 里候选冒号**之前还有 ``@``** 时用户/口令分界**不可证**
  （``_userinfo_split_is_provable``）⇒ 整段 userinfo fail-closed。r6 的
  ``redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0`` 里用户段最后一个 ``@``
  之后的 ``Ccc`` 没有任何跨度覆盖、输出留下 ``@@Ccc``——同一缺陷类的第四次复发；
* ``_mask_url`` 与 ``_passwords`` **只**经这一个枚举（``_redaction_forms`` 是它的
  needle 版）。**合格化**的一致性不变量：``_passwords(s) != ()`` ⟹
  ``_mask_url(s)`` 不含其中任何形态（与有意保留的结构 token——scheme / 可证用户名 /
  host / port / db / 非 authority 跨度的用户 token——偶然重合的除外）、且 ``!= s``；
  反之 ``_mask_url(s) != s`` ⟹ ``_passwords(s) != ()``。
* **完备性才是验收方向（r7）**：oracle（``redis.connection.parse_url``，并用
  ``urlsplit`` 与 ``_parse_redis_url`` 交叉核对）报出的口令，任何 ≥3 前缀/后缀
  与所有 ≥3 子串都不得出现在五个渲染面上。**自洽不蕴含完备**——r6 的
  ``_passwords`` 与 ``_mask_url`` 完全自洽，仍然泄露后缀 ``Ccc``。
"""
import logging
import os
from typing import NamedTuple, Optional
from urllib.parse import parse_qs, quote, unquote, unquote_plus, urlsplit

logger = logging.getLogger("plaita.server.factory")

_EXPERIMENTAL_DB_ENV = "PLAITA_ALLOW_EXPERIMENTAL_DB"

#: 「未配置 Redis」时的文档化默认端点（CLI/env 默认值同此，见 flow_worker /
#: event_filter 的 ``--redis-url``）。**只有**「没配」走它；「配错了」不走。
DEFAULT_REDIS_URL = "redis://localhost:6379/0"

#: 支持的 scheme——与队列/事件总线侧 ``redis.Redis.from_url`` 的白名单一致。
#: 解析器必须与之一致，否则同一个 URL 在「存储」与「队列」之间再次分叉
#: （plaita#88 的根因正是存储侧另有一套解析）。
_REDIS_SCHEMES = ("redis", "rediss", "unix")

#: 错误消息/日志里密码的固定掩码——凭据唯一允许出现的形态。
_REDACTED = "***"

#: authority 的结束符：与 ``urlsplit`` 的 netloc 语义一致（`://` 之后到首个
#: ``/`` / ``?`` / ``#``）。注意这**不是**正则能表达的：口令里可以合法出现
#: ``@``、空格、``:`` 等，唯一的权威定界是「凭据候选冒号所在字段里**最后**一个
#: ``@`` 是口令结束符，它之前**第一个** ``:`` 才是用户/口令分界」——这正是 r2 的
#: 正则 ``[^/@\s:]*:[^/@\s]*@`` 掩不住
#: ``redis://user:@Pr0d2024x@redis.internal:0`` / ``redis://user:p@ssword@host``
#: / ``redis://user:Super Secret@host`` 的原因（plaita#88 r3）。
#:
#: r6：**不再存在「the credential span」这个概念**——``_credential_spans`` 把
#: **每一个**候选冒号都配上一个 ``@`` 并全部掩掉。r5 的 ``rfind("://")`` 锚点
#: （把口令自己的 ``://`` 当 scheme 边界，R1）与「只看第一个候选冒号」（R2）
#: 因此不是被补了两个特例，而是不存在了。
_AUTHORITY_DELIMITERS = "/?#"
#: 字段边界：候选冒号配对 ``@`` 时先只看本字段（下一个 ``/?#&`` 之前）——
#: 这既让 userinfo 的 ``@`` 不越权吃掉 query 里的凭据（r5 R2），也不会因为
#: 口令里写了 ``/?#&`` 就整条落空（字段内没有 ``@`` 时回退全串）。
_FIELD_DELIMITERS = "/?#&"

#: 口令内部的分隔符：结构层会把口令按这些字符切开（userinfo 的 ``:``/``@``、
#: query 的 ``?``/``&``/``=``、path 的 ``/``、fragment 的 ``#``）。切开后的头/尾截
#: 都是同一个口令的书写形态，都要重洗——``redis://user:Pw?Tail@host`` 的 query
#: 字段里出现的是 ``Tail``。
_SECRET_SPLITTERS = (":", "@", "?", "#", "&", "=")

#: percent-escape 的十六进制字母大小写两种拼法都要认（``%2F`` / ``%2f``）。
_HEX_LOWER = str.maketrans("ABCDEF", "abcdef")


def _delimiter_stop(text: str, origin: int, delimiters: str) -> int:
    """``origin`` 起首个 ``delimiters`` 字符的位置（没有则 ``len(text)``）。"""
    stop = len(text)
    for ch in delimiters:
        idx = text.find(ch, origin)
        if idx != -1 and idx < stop:
            stop = idx
    return stop


#: 凭据字段（query 值 / path 段）向前找 user token 起点时的分隔符——与 query/path
#: 的字段边界一致（``&``/``=`` 也是）：``?db=Pr0d:x@y`` 的 user token 是 ``Pr0d``。
_VALUE_DELIMITERS = "/?#&=@"


def _is_port_colon(
    text: str, colon_at: int, at: int, terminators: str = "/?#@"
) -> bool:
    """冒号是 ``host:port`` 的端口分界（**不是**用户/口令分界）吗？

    ``at`` 是本串最后一个 ``@`` 的位置；``at = -1`` 表示没有 ``@``（此时数字段
    只要走到串尾或分隔符就算端口）。两种「端口形状」都要求冒号后紧跟数字，
    且数字段正好停在 ``terminators`` 里的某个字符（或串尾）之前：

    * ``redis://host:6379/0@x`` 的 ``host:``——数字后是 ``/``；
    * ``redis://host/0?x=1:2@y`` 的 ``1:``——冒号到 ``@`` 之间只有数字。

    r6 的边界（写在 MIGRATION.md）：**数字打头 + ``/``** 的口令（``?u=X:911/bzx``、
    ``redis://user:123/abc@host`` 的截断段）与 ``host:port/db`` 局部同形，端口判据
    按 r4 已验证的 keep 取「端口」——这一段靠 ``_passwords`` 的 ``cut`` 腿与重洗层
    兜，前缀仍可能留在消息字段里（HEAD 同形）。

    于是 ``redis://user:p/w@host``（数字段为空）、``?db=Pr0d:x@y``（``x`` 非数字）
    的冒号是**凭据**分界；``redis://host:6379/0@x`` / ``?ref=a@b`` / ``?db=1@2``
    没有凭据分界，保持 r4 已验证的 keep 形态。

    ``terminators`` 默认 ``/?#@``（query/path 侧）。**authority 里**改用 ``/``：
    ``redis://user:9?0b@host`` / ``redis://user:9#Z@host`` 的 ``?``/``#`` 是口令里
    的普通字符，把它们当端口结束符会把真口令挤出掩码之外。authority 里唯一无歧义
    的端口形态是「数字后紧跟 ``/`` 或串尾」——r4 已验证的 keep
    ``redis://host:6379/0@x`` 正是这一种；``redis://user:6379@host`` 与
    ``redis://host:6379@x`` 在 r5 上也都按凭据掩掉（可对照 HEAD 行为）。
    """
    i = colon_at + 1
    stop = at if at > colon_at else len(text)
    j = i
    while j < stop and text[j].isdigit():
        j += 1
    return j > i and (j == stop or text[j] in terminators)


class _CredentialSpan(NamedTuple):
    """一个凭据候选跨度：``[mask_from, stop)`` 是口令段，``[start, mask_from)`` 是用户 token。

    两种来源共用同一个形状：

    * **冒号候选**（``_at_colon_spans``）：``mask_from == colon_at + 1``、``stop`` 是
      配对的 ``@``；
    * **无 ``@`` 候选**（``_no_at_colon_spans``）：``stop`` 是 token 末尾（exclusive），
      没有 ``@`` 可掩。
    """

    start: int
    mask_from: int
    stop: int


def _span_at(text: str, colon_at: int) -> int:
    """候选冒号配对的 ``@``——边界**按冒号所在区域**取。

    * **authority 里**（``://`` 之后到首个 ``/?#``）：口令段止于首个 ``/``
      （r5 的 ``_PATH_DELIMITERS``）。``redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1@x``
      的 userinfo ``@`` 因此不会越权吃掉 query 里的凭据（r5 R2），而
      ``redis://user:?@00bbb0?  @host`` 里口令自己的 ``@``/``?`` 也不会把它
      截成空口令；
    * **authority 之外**（query / path / fragment）：取该冒号之后**最后**一个
      ``@``（r5 ``_global_credential_span`` 的语义）——``?db=Acct:b0@ /@x`` 的
      口令跨 ``@``/``/``，取首个 ``/`` 会把它截断。

    两种情况都在「段内没有 ``@``」或「段内那个 ``@`` 让口令为空」时回退到
    「该冒号之后**最后**一个 ``@``」：口令里写了 ``/`` 的畸形态
    （``redis://user:pa55word://x@host``）首个 ``/`` 落在口令内部，凭据其实还在
    后面。回退只会把 ``at`` 取更靠后（**只多掩不少掩**）。单趟 ``rfind``，无回溯。
    """
    sep = text.find("://")
    auth_start = sep + 3 if sep != -1 else 0
    auth_stop = _delimiter_stop(text, auth_start, _AUTHORITY_DELIMITERS)
    if colon_at < auth_stop:
        at = text.rfind("@", colon_at, _delimiter_stop(text, colon_at, "/"))
        if at != -1 and at > colon_at + 1:
            return at
    return text.rfind("@", colon_at)


def _scheme_colon_at(text: str) -> int:
    """scheme 的 ``:`` 位置（没有则 -1）——**局部**判据，不是扫描锚点。

    两种形态，都用「第一个」而不是 r5 的 ``rfind("://")``：

    * 含 ``://``：**第一个** ``://`` 的 ``:``（``redis://a://b://user:pw@host`` 里
      只有第一个是 scheme，后面那些是畸形输入里的字面 ``://``）；
    * 不含 ``://``：串首 scheme token（字母/数字/``+``/``-``/``.``）之后、**紧跟
      ``/``** 的那个 ``:``——``redis:/user:pw@host`` 的单斜杠笔误。``user:pw@host``
      的 ``user:`` 不算（它后面不是 ``/``，是凭据的用户/口令分界）。

    只有这一个冒号可以是 scheme 分隔符：其它所有非端口冒号都是凭据候选。r5 的
    ``rfind("://", 0, at)`` 把**口令自己的** ``://`` 也当成 scheme 边界，于是
    ``redis://user:pa55word://x@host`` 的扫描起点被推到口令尾巴上（R1）。
    """
    sep = text.find("://")
    if sep != -1:
        return sep
    i = 0
    while i < len(text) and (text[i].isalnum() or text[i] in "+-."):
        i += 1
    if i > 0 and i < len(text) and text[i] == ":" and text[i + 1 : i + 2] == "/":
        return i
    return -1


def _candidate_colons_raw(text: str):
    """左→右产出**所有** depth 0 冒号（端口/scheme 都算）——只给粗档做「被端口判据
    吃掉了哪些冒号」的盘点用。"""
    depth = 0
    for i, ch in enumerate(text):
        if ch == "[":
            depth += 1
        elif ch == "]":
            if depth:
                depth -= 1
        elif ch == ":" and depth == 0:
            yield i


def _candidate_colons(text: str, scan_from: int):
    """左→右产出**凭据候选冒号**的位置（r6 的唯一冒号判据）。

    三条判据都是**局部**的，互不依赖、无回溯：

    * depth 0——跳过 IPv6 字面量 ``[...]`` 里的冒号（``redis://[::1]:6379/0``）；
    * **非端口冒号**——复用 ``_is_port_colon``（``host:6379`` 是端口，不是凭据分界）；
    * **不是 scheme 冒号**——``_scheme_colon_at`` 只认「第一个 ``://`` 的冒号」或
      「串首 ``scheme:`` 的冒号」。r5 拿 ``rfind("://")`` 当**锚点**（把口令自己的
      ``://`` 也算进去，R1），这里改成对每个冒号问一句「你是不是 scheme 冒号」。
    """
    n = len(text)
    scheme_colon = _scheme_colon_at(text)
    sep = text.find("://")
    auth_start = sep + 3 if sep != -1 else 0
    auth_stop = _delimiter_stop(text, auth_start, _AUTHORITY_DELIMITERS)
    depth = 0
    i = scan_from
    while i < n:
        ch = text[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            if depth:
                depth -= 1
        elif ch == ":" and depth == 0:
            if i == scheme_colon:
                pass
            elif i == auth_start:
                # authority 的第一个冒号（``redis://:9@host`` 的空用户名）：
                # ``:9`` 在局部上与 ``host:port`` 同形，但端口分界前面一定有主机
                # token，这里前面什么都没有 ⇒ 是用户/口令分界。r5 的 authority 档
                # 也是这样（它压根不对 authority 的 userinfo 冒号跑端口判据）。
                yield i
            elif not _is_port_colon(
                text, i, -1, "/" if i < auth_stop else "/?#@"
            ):
                yield i
        i += 1


def _userinfo_split_is_provable(
    text: str, auth_start: int, auth_stop: int, colon_at: int
) -> bool:
    """authority 里这个候选冒号**能证明**是用户/口令分界吗（r7 的结构性判据）。

    证明要求「候选冒号之前（authority 起点到该冒号）没有任何 ``@``」。理由就是
    ``urlsplit`` 的语义：userinfo 是 netloc 里**最后一个** ``@`` 之前那段，用户名
    是 userinfo 里**第一个** ``:`` 之前那段。若候选冒号之前还有 ``@``，那个 ``@``
    可能是真正的 userinfo 分隔符，于是「谁是用户、谁是口令」不可证——
    ``redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0`` 里 r6 只把
    ``[候选冒号+1, 最后一个 @)`` 当口令，用户段里最后一个 ``@`` 之后的
    ``Ccc`` 因此**没有任何跨度覆盖**（r6 复发 r3/r4/r5 同一缺陷类的根因）。

    不可证时的处置是**整段 userinfo fail-closed**（``[auth_start, 配对 @)`` 全掩），
    不是再补一个「用户 == 口令」特例——口令里含 ``@``/``:``/``/``/``?``/``#``/
    ``%``/空白的任何形态都走这条同一条路。authority 之外（query 值 / path /
    fragment）的 ``@`` 是值的一部分，不参与本判据。
    """
    return not (auth_start <= colon_at < auth_stop and "@" in text[auth_start:colon_at])


def _at_colon_spans(text: str) -> tuple:
    """冒号候选 + 配对的 ``@``：**全部**候选，不是第一个（r5 R1/R2 的结构性修复）。

    r7：候选冒号落在 authority 内、且它前面还有 ``@`` 时，用户/口令分界**不可证**
    ⇒ 整段 userinfo fail-closed（见 ``_userinfo_split_is_provable``）。
    """
    spans = []
    last_at = text.rfind("@")
    if last_at == -1:
        return ()
    sep = text.find("://")
    auth_start = sep + 3 if sep != -1 else 0
    auth_stop = _delimiter_stop(text, auth_start, _AUTHORITY_DELIMITERS)
    consumed_until = 0
    for colon_at in _candidate_colons(text, 0):
        if colon_at > last_at:
            break  # 再往后没有 '@'，配不出跨度
        if colon_at < consumed_until:
            continue  # 已被前一个跨度吃掉（左→右、互不重叠）
        at = _span_at(text, colon_at)
        if at == -1:
            continue
        start = colon_at
        while start > 0 and text[start - 1] not in _VALUE_DELIMITERS:
            start -= 1
        if not _userinfo_split_is_provable(text, auth_start, auth_stop, colon_at):
            # 用户/口令分界不可证 → 整段 userinfo 当凭据（只多掩不少掩）。
            start = auth_start
            mask_from = auth_start
        elif at <= colon_at + 1:
            # 空口令（``:`` 紧跟 ``@``，``redis://user:@host``）：认不出用户/口令
            # 分界 → fail-closed 把**整个** userinfo token 当凭据（只多掩不少掩），
            # 否则 ``[mask_from, stop)`` 为空、这一条 URL 会原样落盘。
            mask_from = start
        else:
            mask_from = colon_at + 1
        spans.append(_CredentialSpan(start, mask_from, at))
        consumed_until = at + 1
    return tuple(spans)


def _no_at_colon_spans(text: str) -> tuple:
    """**没有 ``@``** 的凭据样 token 跨度（r5 C 的枚举版）。

    ``redis://user:sup3rsecret127.0.0.1:679/0``（``@`` 写漏/被抹）在 redis-py 里
    userinfo 位为空，但 ``user:`` 仍是凭据样文本。这里枚举**每一个**候选冒号
    （r5 的 ``_credential_token_span`` 只看第一个——同一类「只看第一个候选」缺陷），
    token 末尾取三者最早：之后的**端口形** ``:digits``、字段分隔符 ``/?#&@``、串尾。
    """
    spans = []
    n = len(text)
    sep = text.find("://")
    scan_from = sep + 3 if sep != -1 else 0
    for colon_at in _candidate_colons(text, scan_from):
        end = colon_at + 1
        while end < n and text[end] not in "?#&@":
            if text[end] == ":" and _is_port_colon(text, end, -1):
                break
            end += 1
        if end == colon_at + 1:
            continue  # 空 token（``http://`` 里的 ``http:``）不是凭据
        start = colon_at
        while start > 0 and text[start - 1] not in _VALUE_DELIMITERS:
            start -= 1
        spans.append(_CredentialSpan(start, colon_at + 1, end))
    return tuple(spans)


def _covered_at(text: str, spans: tuple) -> frozenset:
    """已被凭据候选跨度吃掉的 ``@`` 位置——fail-closed 的 ``@`` token 腿不再重复处理。

    覆盖的是跨度 ``[start, stop]`` 里的 ``@``（含收尾那个）。紧邻跨度 ``start``
    左边的 ``@`` **不**算覆盖：``redis://a?ZA9%@a:A@host`` 的第一个 ``@`` 前的
    token 是凭据，fail-closed 层必须照掩（掩码后的用户名段会被重洗，所以
    「掩掉的」与「洗掉的」仍出一门）。
    """
    covered = set()
    for span in spans:
        covered.add(span.stop)
        i = text.find("@", span.start, span.stop)
        while i != -1:
            covered.add(i)
            i = text.find("@", i + 1, span.stop)
    return frozenset(covered)


def _bare_at_token_spans(text: str, covered: frozenset) -> tuple:
    """``@`` 前没有任何候选冒号时的 fail-closed 跨度（r5 B）。

    ``@`` 本身就是凭据分隔符的唯一信号：``redis://user/sup3rsecret@host``（把
    ``:`` 写成了 ``/``/``?``/``#``）没有任何候选冒号——不能再原样返回。把 ``@`` 前
    那截 token 当凭据抹掉。

    已 r4 验证的 **keep** 形态不抹：空 token（``redis://host/@x``）、纯数字 db
    （``redis://host:6379/0@x`` 的 ``0``）、``=``/``:`` 之后的 token
    （``?ref=a@b``、``?x=1:2@y``）。单趟左→右，O(n)、无回溯。
    """
    spans = []
    i = 0
    while True:
        at = text.find("@", i)
        if at == -1:
            return tuple(spans)
        if at in covered:
            i = at + 1
            continue
        start = at
        while start > 0 and text[start - 1] not in _VALUE_DELIMITERS:
            start -= 1
        token = text[start:at]
        keep = (
            not token
            or _REDACTED in token
            or (token.isascii() and token.isdigit())
            or (start > 0 and text[start - 1] in "=:")
        )
        if not keep:
            spans.append(_CredentialSpan(start, start, at))
        i = at + 1


def _authority_fail_closed_span(text: str) -> tuple:
    """authority 里有 ``@`` 但**一个候选凭据冒号都没有** ⇒ fail-closed 抹整段 userinfo。

    r3 已验证：``redis://user@host:6379`` → ``redis://***@host:6379``（用户名也
    不留——认不出用户/口令分界）。``redis://@@@A@host``、``redis://@?Aa:@host``
    这类「凭据位置全是分隔符」的畸形态同样整段抹掉，而不是只抹最后一个 token
    （r5 在 tier 1 就是这样：authority 里认出 ``@`` 却认不出 ``:`` → fail-closed）。
    """
    sep = text.find("://")
    if sep == -1:
        return ()
    auth_start = sep + 3
    auth_stop = _delimiter_stop(text, auth_start, _AUTHORITY_DELIMITERS)
    at = text.rfind("@", auth_start, auth_stop)
    if at <= auth_start:
        return ()
    for colon_at in _candidate_colons(text, auth_start):
        if colon_at >= at:
            break
        return ()
    return (_CredentialSpan(auth_start, auth_start, at),)


def _no_scheme_coarse_span(text: str, at_spans: tuple) -> tuple:
    """**没有 scheme**（``@`` 之前没有 ``://``）时的粗档（r3/r4 的 no-scheme 兜底）。

    没有 scheme ⇒ 没有 authority 可言，``_candidate_colons`` 的端口判据会把
    ``redis:/user:9/zx/#@host`` 的 ``user:``（数字后跟 ``/``）、
    ``REDIS:/user:9#0A::bZZZ@host`` 的 ``user:``（数字后跟 ``#``）当端口吃掉。
    r3/r4 在这里的判据是「``@`` 之前有 ``:`` ⇒ 整段前缀当 userinfo」——照办：
    把 ``[首个冒号+1, 最后一个 ``@``)`` 整段掩成 ``***``（``redis:/user:9/…@host``
    → ``redis:***@host``，与 r5 逐字一致）。

    只在（a）细档全空，或（b）**端口判据在首个被接受的候选冒号之前吃掉过一个
    冒号**时生效——所以 ``redis:/user:pw@host``（细档直接认出 ``user:pw``）、
    ``user:pw@host:6379``（被吃掉的端口冒号在 ``@`` 之后）都不受影响；有 ``://``
    的输入由细档全权负责。
    """
    anchor = text.rfind("@")
    if anchor == -1 or text.rfind("://", 0, anchor) != -1:
        return ()
    if ":" not in text[:anchor]:
        return ()
    scheme_colon = _scheme_colon_at(text)
    first_accepted = at_spans[0].start if at_spans else None
    coarse = not at_spans
    if not coarse:
        auth_stop = _delimiter_stop(text, 0, _AUTHORITY_DELIMITERS)
        for colon_at in _candidate_colons_raw(text):
            if colon_at == scheme_colon or colon_at >= anchor:
                continue
            if _is_port_colon(
                text, colon_at, -1, "/" if colon_at < auth_stop else "/?#@"
            ):
                if first_accepted is None or colon_at < first_accepted:
                    coarse = True
                    break
    if not coarse:
        return ()
    start = text.find(":", 0, anchor) + 1
    if start >= anchor:
        return ()
    return (_CredentialSpan(0, start, anchor),)


def _credential_spans(text: str) -> tuple:
    """**枚举 ``text`` 里每一个凭据候选跨度**（r6 的结构性要求，唯一入口）。

    r3/r4/r5 的失效模式是同一个：每轮都在找「**那一个**凭据跨度」——r3 只认字面
    ``://`` 之后的 authority；r4 的全局兜底按「authority 里有没有 ``@``」分档；
    r5 的 ``_global_credential_span`` 拿 ``rfind("://")`` 当锚点，而且碰到第一个
    候选冒号就 ``return``，``_credential_token_span`` 同理。三次漏的都是
    「**别的**跨度」。这里把三条腿**一次枚举齐**：

    * ``_at_colon_spans``——冒号候选配 ``@``（userinfo、query 值、path、fragment）；
    * ``_no_at_colon_spans``——没有 ``@`` 的凭据样 token（r5 C）；
    * ``_bare_at_token_spans``——``@`` 前没有候选冒号的 fail-closed token（r5 B）。

    ``_mask_url`` 与 ``_passwords`` **都**只经这一个函数，所以「掩了什么」与
    「洗了什么」不可能分叉（一致性不变量，见 ``_mask_url`` 文档）。

    单趟左→右；每个候选冒号的 ``_span_at`` 要么很快撞上字段边界、要么把
    ``consumed_until`` 推到那个 ``@`` 之后，故整体 O(n)、无回溯（70k 线性用例仍成立）。
    """
    at_spans = _at_colon_spans(text)
    no_at_spans = _no_at_colon_spans(text)
    coarse = _no_scheme_coarse_span(text, at_spans)
    failed = _authority_fail_closed_span(text) if not (at_spans or coarse) else ()
    bare = _bare_at_token_spans(
        text, _covered_at(text, at_spans + no_at_spans + coarse + failed)
    )
    return tuple(
        sorted(
            at_spans + no_at_spans + coarse + failed + bare,
            key=lambda span: (span.mask_from, span.stop),
        )
    )


def _lower_hex(encoded: str) -> str:
    """把 ``%4A`` 式转义的十六进制字母改成小写（``%4a``）——URL 里两种拼法都合法。"""
    out = []
    i = 0
    while i < len(encoded):
        if encoded[i] == "%" and i + 2 < len(encoded):
            out.append("%")
            out.append(encoded[i + 1 : i + 3].translate(_HEX_LOWER))
            i += 3
        else:
            out.append(encoded[i])
            i += 1
    return "".join(out)


def _quoted_forms(text: str) -> frozenset:
    """``quote(text, safe="")`` 的大小写两种拼法；孤立代理字符按**字节**渲染。

    ``os.environ`` 用 ``surrogateescape`` 把非 UTF-8 字节解码成孤立代理字符
    （``0xFF`` → ``U+DCFF``），``quote`` 内部的 ``utf-8`` 编码随即抛
    ``UnicodeEncodeError``。兜底分两种，都以**字节**为准：

    * ``surrogateescape`` 能编回的（``U+DC80``-``U+DCFF``）：拿到的是**原始字节**，
      于是 ``_quoted_forms('\udcff')`` = ``%FF``——这正是 on-the-wire 形态
      （r6 更正 r5 的注释：r5 一律走 ``surrogatepass``，得到的是 WTF-8 字节
      ``%ED%B3%BF``，与线上的 ``%FF`` **不同**）；
    * 编不回的孤立代理（``U+D800`` 之类，不是 ``surrogateescape`` 的产物）：
      退回 ``surrogatepass``（WTF-8 字节，``%ED%A0%80``）——这一版只是「不抛 +
      多一种写法」，不声称与线上一致。

    无论哪一支都绝不把异常抛给调用方（r5 A：``_mask_url`` 必须 total）。
    """
    try:
        encoded = quote(text, safe="")
    except UnicodeError:
        try:
            payload = text.encode("utf-8", "surrogateescape")
        except UnicodeError:
            payload = text.encode("utf-8", "surrogatepass")
        encoded = quote(payload, safe="")
    return frozenset({encoded, _lower_hex(encoded)})


def _encoding_forms(secret: str) -> frozenset:
    """一个字符串的三种书写形态：原文 / percent-decoded / percent-encoded。

    **对任意 ``str`` 都不抛**（r5 A）：``_mask_url`` 必须是 total——它挂在
    ``_parse_redis_url`` 的每个拒绝分支上，抛出去就是启动期硬崩（#158 实测）。
    """
    forms = {secret}
    try:
        decoded = unquote(secret)
    except UnicodeError:  # 含 ``%`` 的孤立代理串：``unquote`` 的 utf-8 编码失败
        decoded = secret
    if decoded:
        forms.add(decoded)
        forms |= _quoted_forms(decoded)
    forms |= _quoted_forms(secret)
    forms.discard("")
    return frozenset(forms)


def _secret_forms(secret: str) -> tuple:
    """一个口令候选的**所有书写形态**（原文 / percent-decoded / percent-encoded）。

    兜底重洗（见 ``_mask_url``）要认出「同一个口令换个写法又出现在 query/path
    里」——``?socket_timeout=@Pr0d2024x`` 与 ``?socket_timeout=%40Pr0d2024x``
    都必须能被洗掉。按长度降序返回：长形态先替换，避免短形态把长形态切碎。

    结构定位若多吞了一段（畸形态里口令本身含 ``/``/``@``：候选 ``/:p/w``、
    ``u@u:a\nb``），真正的口令往往是**第一个** ``:``/``@`` 之后那截——把它连同
    它的编码形态一起纳入（只多洗、不少洗）。

    r6 再补一版「剥掉空白」的：``urlsplit`` 解析前会做 WHATWG 归一化，把 URL 里的
    ``\t``/``\n``/``\r`` 直接删掉，于是底层异常文案里的口令是无空白版本——
    ``redis://user:0Cxya1B\tAxCaaC/x@host`` 的 ``Port could not be cast …`` 里是
    ``0Cxya1BAxCaaC``。少了这一版，``\t`` 后面的尾巴会以 ≥3 字符后缀活下来。
    """
    if not secret:
        return ()
    forms = set(_encoding_forms(secret))
    variants = [secret]
    stripped = secret.replace("\t", "").replace("\n", "").replace("\r", "")
    if stripped != secret:
        variants.append(stripped)
    # ``parts.port`` 的报错文案是 ``%r``——非 ASCII 与 ``\t`` 在里面是**转义写法**
    # （``Super\xa0Secret`` → ``'Super\\xa0Secret'``）。这一版同样要洗，否则
    # ``\xa0`` 之前的前缀会以 ≥3 字符前缀的形式活下来。
    # ``unicode_escape`` 对孤立代理字符走 backslashreplace（``\udcff``），不抛。
    escaped = secret.encode("unicode_escape").decode("ascii")
    if escaped != secret:
        variants.append(escaped)
    for variant in variants:
        for sep in _SECRET_SPLITTERS:
            cut = variant.find(sep)
            if cut != -1:
                # 尾截（r4：畸形态里真口令往往是首个分隔符之后那截）
                forms |= _encoding_forms(variant[cut + 1 :])
                # 头截（r6：口令本身含 ':' 且**用户名 == 口令**时
                # ``redis://Pw:Pw@host`` 的用户 token 段会留下口令的头部）
                forms |= _encoding_forms(variant[:cut])
        if variant is not secret:
            forms |= _encoding_forms(variant)
    forms.discard("")
    return tuple(sorted(forms, key=len, reverse=True))


def _protected_regions(text: str) -> tuple:
    """**重洗**不碰的结构性区间：scheme（``scheme://``）与 authority 的 host。

    它们是诊断信息，也可能恰好是口令的子串——口令 ``redis`` + host
    ``redis.example.com`` 时无差别全局替换会把 scheme 抹成 ``***://``（假红），
    且没有任何安全收益（``test_password_substring_of_scheme_or_host_is_not_a_false_red``）。
    scheme 与 host 之外的**一切**（userinfo、path、query、fragment，以及没有
    ``://`` 的畸形前缀）都可能被重洗，所以「用户名 == 口令」「口令重复写在
    ``/PwX`` / ``?socket_timeout=PwX``」都活不下来。

    host 的起点是**首个 ``/`` 之前的最后一个 ``@``**（不是首个 ``/?#`` 之前）：
    ``redis://C1Bacyayzzc?B9b:C1Bacyayzzc?B9b@host:6379/0`` 的口令里带 ``?``，
    按 ``/?#`` 定界会把用户名段整段当 host 保护起来、漏洗。
    """
    regions = []
    sep = text.find("://")
    auth_start = sep + 3 if sep != -1 else 0
    if sep != -1:
        start = sep
        while start > 0 and (text[start - 1].isalnum() or text[start - 1] in "+-."):
            start -= 1
        if start < sep:
            regions.append((start, sep + 3))
    at = text.rfind("@", auth_start, _delimiter_stop(text, auth_start, "/"))
    host_start = at + 1 if at != -1 else auth_start
    regions.append((host_start, _delimiter_stop(text, host_start, "/?#")))
    return tuple(regions)


def _render_redacted(text: str, spans: tuple, forms: tuple) -> str:
    """单趟渲染：保护区间原样、凭据候选段 ``***``、其余按 ``forms`` 重洗。

    ``spans`` 是 ``_credential_spans`` 的**全部**跨度，不是第一个——R1
    （``redis://user:pa55word://x@host``）与 R2
    （``redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1@x``）的每一条跨度都会
    被掩，所以「口令自己的 ``://`` 把扫描起点推到尾巴」与「只看第一个候选冒号」
    两个漏点在结构上没有立足点。三态优先序：掩码 > 保护 > 重洗。
    """
    n = len(text)
    kind = bytearray(b"s" * n)  # s=可重洗 / p=结构性保护 / m=必掩
    for start, stop in _protected_regions(text):
        for i in range(start, min(stop, n)):
            kind[i] = ord("p")
    for span in spans:
        for i in range(span.mask_from, min(span.stop, n)):
            kind[i] = ord("m")
    out = []
    i = 0
    while i < n:
        k = kind[i]
        j = i
        while j < n and kind[j] == k:
            j += 1
        if k == ord("p"):
            out.append(text[i:j])
        elif k == ord("m"):
            out.append(_REDACTED)
        else:
            segment = text[i:j]
            for form in forms:
                if form in segment:
                    segment = segment.replace(form, _REDACTED)
            out.append(segment)
        i = j
    return "".join(out)


def _redaction_forms(text: str, spans: tuple) -> tuple:
    """``text`` 里**所有**凭据候选的书写形态（原文 / percent-decoded / encoded）。

    r6 的一致性不变量要求「掩掉的」与「洗掉的」出自**同一个** ``_credential_spans``
    枚举：``_mask_url`` 用它的掩码区间，本函数是同一套跨度的 needle 版，
    ``_passwords`` 直接返回它。两条腿各自独立、各有变异单腿钉死：

    * **凭据段腿**：每个 ``_CredentialSpan`` 的 ``text[mask_from:stop]``；
    * **authority 截断腿**（``cut < stop``）：口令里含 ``/`` / ``?`` / ``#`` 时
      urlsplit / redis-py 只看到 authority 内那截——端口/db cast 失败的消息里出现
      的正是它（``Port could not be cast ... 'pa55word'``）。判据是「分隔符确实
      切在凭据跨度**内部**」，普通 ``user:pw@host/0`` 不算。
    """
    forms = set()
    for span in spans:
        forms.update(_secret_forms(text[span.mask_from : span.stop]))
        cut = _delimiter_stop(text, span.mask_from, _AUTHORITY_DELIMITERS)
        if span.mask_from < cut < span.stop:
            forms.update(_secret_forms(text[span.mask_from:cut]))
    forms.discard("")
    return tuple(sorted(forms, key=len, reverse=True))


def _mask_url(value: object) -> str:
    """把 URL 渲染成可安全写进异常/日志的形态：口令一律 ``***``。

    ``redis://user:sup3rsecret@host:6379/0`` → ``redis://user:***@host:6379/0``。
    口令里带 ``@`` / 空格 / tab / nbsp / ``:`` / ``%`` / ``/`` / ``://`` / unicode 等
    （r2 正则的漏网形态）同样整体抹掉；scheme/host/port/db/path 原样保留，消息仍可
    诊断。

    **cannot-leak by construction**（安全性不依赖输入形态是否合法）：

    * **枚举层**（r6）：``_credential_spans`` 把**每一个**候选跨度（冒号候选 +
      无 ``@`` token + ``@`` 前无冒号的 fail-closed token）一次枚举齐，
      ``_render_redacted`` 把它们的口令段**全部**换成 ``***``。r3/r4/r5 每轮都在找
      「**那一个**凭据跨度」（r3 只认字面 ``://`` 之后的 authority、r4 的兜底按
      userinfo 有无分档、r5 拿 ``rfind("://")`` 当锚点且只看第一个候选冒号）——
      同一个缺陷类复发三次；枚举之后「口令含 ``/``」（R1）与「凭据只写在 query 值
      里、还有第二条跨度」（R2）不再各自需要一个特例。
    * **fail-closed 层**（r5 B/C）：``@`` 前没有任何候选冒号时把该 token 抹成
      ``***``；没有 ``@`` 的凭据样 token 同样枚举抹掉。已 r4 验证的 **keep** 形态
      逐字不动：``redis://host:6379/0@x``、``redis://host/0?ref=a@b``、
      ``redis://host:6379/0?db=1@2``、``redis://host/0?x=1:2@y``、IPv6 ``[::1]`` /
      ``[2001:db8::1]``、``unix://…``。
    * **重洗层**：所有候选口令的原文 / percent-encoded / decoded 形态在 scheme 与
      host **之外**再洗一遍（``_protected_regions``）——口令重复写在
      ``?socket_timeout=PwX``、``/PwX``、``?<口令>=1`` 里都活不下来；scheme 与 host
      是结构性诊断信息，且可能是口令的子串（口令 ``redis`` + host
      ``redis.example.com``），无差别全局替换只会给假红。
    * **total**（r5 A）：任意 ``str``（含 ``os.environ`` 的 surrogateescape 孤立代理
      字符）都不抛。

    ``_parse_redis_url`` 的**所有**拒绝分支只允许用它渲染原始输入：``RedisUrlError``
    会被 ``flow_worker`` / ``event_filter`` 的 ``logger.error("...: %s", e)`` 原样写进
    生产日志（后者还带 ``exc_info=True``——``from exc`` 的 cause 是**另一个独立渲染
    面**，见 ``_sanitized_cause``）。

    **合格化的一致性不变量**（``TestEnumerateAllSpans`` 钉住）：
    ``_passwords(s) != ()`` ⟹ ``_mask_url(s)`` 不含其中任何形态、且
    ``_mask_url(s) != s``；反之 ``_mask_url(s) != s`` ⟹ ``_passwords(s) != ()``。
    无限定版本是**假**的：口令形态与**有意保留的结构 token** 撞车是合法的
    （``redis://user:Q7w:9:6379@host:6379/0`` 的 ``_passwords`` 含 ``6379``，而
    host 端口 ``6379`` 本就该留下；验证者的 432 个规则级反例全是这一类）。
    两者共用 ``_credential_spans`` 这一个枚举，所以「掩了什么」与「洗了什么」不可能
    分叉——r5 的 R1（``_passwords`` 取到 ``pa55word`` 而 ``_mask_url`` 掩不住）与
    R2（``_mask_url`` 掩了、``_passwords`` 取不到）都是这条不变量的违反。

    **但自洽不蕴含完备（r7 的验收方向）**：r6 的 ``_mask_url`` 与 ``_passwords``
    完全自洽，输出 ``redis://***@@***@@Ccc:***@host:6379/0`` 里仍然留着口令后缀
    ``Ccc``——因为「没有跨度覆盖的用户段尾巴」两头都不在枚举里。判据因此换成
    oracle 接地的完备性：口令的任何 ≥3 前缀/后缀、所有 ≥3 子串都不得出现在五个
    渲染面上（``TestMaskerGenerativeProperty``）。**结构上的处置**是
    ``_userinfo_split_is_provable``：authority 里分界不可证时整段 userinfo
    fail-closed，而不是给某个形态加特例。
    """
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", "replace")
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"

    spans = _credential_spans(value)
    if not spans:
        return value
    return _render_redacted(value, spans, _redaction_forms(value, spans))


def _passwords(text: str) -> tuple:
    """原始 URL 里出现的口令字面量（原文 + percent-decoded + encoded 形态）。

    与 ``_mask_url`` **同源同枚举**（r6）：两者都走 ``_credential_spans`` ——带
    ``@``/空格/裸 ``#``/``/``/``://`` 的口令、以及**没有 userinfo**、只写在 query
    值里的凭据（``?db=Pr0d:x@y``）都能被完整取出。r4 的 ``_passwords`` 依赖
    ``_locate_credentials``（没有 userinfo 时整层空转，r5 R2）；r5 的
    ``_global_credential_span`` 只取第一个候选冒号（同样取不到第二条跨度）。
    """
    if not isinstance(text, str):
        return ()
    return _redaction_forms(text, _credential_spans(text))


def _raw_query_names(query_text: str) -> tuple:
    """query 串里**原始**（未 percent-decode）的参数名，``&``/``;`` 分段。

    与 ``parse_qs`` 的分段一致，但不解码：解码后的名字可能是 U+FFFD（非法转义），
    在原文里根本匹配不到（r7 实测 ``?9yc%%bCAz1b=1`` 的未知名因此洗不掉）。
    """
    names = []
    for segment in query_text.replace(";", "&").split("&"):
        raw = segment.split("=", 1)[0]
        if raw:
            names.append(raw)
    return tuple(names)


def _scrub_literal(text: str, literals: tuple) -> str:
    """把**显式给定**的字面量从 ``text`` 里抹成 ``***``（default-deny，r7）。

    ``_scrub`` 只洗「已识别为口令」的形态；``?db=<非法值>`` / ``?<未知名>=1``
    这类文本**不是**口令候选，识别不出来，于是被原样回显到拒绝消息与 WARNING 里。
    这里把「调用点自己知道的那段攻击者可控文本」点名抹掉——同时保留参数名
    （``db`` / ``socket_timeout`` / 消息里的固定措辞）以便诊断。
    """
    for literal in literals:
        if literal:
            text = text.replace(literal, _REDACTED)
    return text


def _safe_reason(exc: BaseException, text: str, extra: tuple = ()) -> str:
    """底层异常的**安全**渲染（只用在嵌套 cause 的位置）。

    不假设 ``urlsplit``/``int``/``float`` 的报错文案里没有整条 URL 或口令：
    先抹掉口令的所有书写形态与 ``extra``（调用点已知的非法取值），再掩码裸 URL，
    然后才允许进入消息/traceback。
    """
    reason = _scrub_literal(str(exc), extra)
    for secret in _passwords(text):
        reason = reason.replace(secret, _REDACTED)
    # 先洗口令字面量、再掩 URL：反过来的话 ``could not convert ...: 'p@ssword'``
    # 会被 _mask_url 从中间切开（``...float:***@ssword'``），留下 ≥3 后缀。
    return _mask_url(reason)


def _scrub(value: object, text: str) -> str:
    """把 ``text`` 里识别到的口令从**任意消息字段**里抹掉。

    ``?<口令>=1``（口令写在 query **名**里）、``?db=<口令>`` / ``/<口令>``
    （写在**值**里）这些字段会被拒绝消息原样 ``{...!r}`` 回显——它们不是 URL，
    不能靠 ``_mask_url``，只能按同一套 ``_passwords`` 形态洗。
    """
    text_value = str(value)
    for secret in _passwords(text):
        text_value = text_value.replace(secret, _REDACTED)
    return text_value


def _sanitized_cause(
    exc: BaseException, text: str, extra: tuple = ()
) -> BaseException:
    """底层异常的**安全替身**：``from exc`` 的 ``__cause__`` 是对象，会被
    ``traceback`` / ``exc_info=True`` **独立渲染**——只把文案洗进
    ``RedisUrlError`` 的消息不够（plaita#88 r4 Finding 2）。

    返回同类型、只带 ``_safe_reason`` 文案的新异常；它没有 traceback，所以链上
    只剩洗过的消息，原始 URL/口令不会随 ``__cause__`` 落盘。``extra`` 是调用点
    已知的非法取值（r7：``?db=<非法值>`` 之类原本在 cause 里原样回显）。
    """
    safe = _safe_reason(exc, text, extra)
    try:
        return type(exc)(safe)
    except Exception:  # pragma: no cover - 构造签名特殊的异常类型
        return ValueError(safe)


class RedisUrlError(ValueError):
    """Redis URL 具名失败：畸形/不支持的 URL 抛它，绝不静默回落 localhost。

    继承 ``ValueError`` 以贴合「坏配置」的既有语义；关键区别是**带上 URL 与
    原因**，且在建立任何连接之前抛出（fail-closed，启动期一次性暴露）。
    URL 一律是 ``_mask_url`` 渲染过的形态（密码 ``***``）——本异常会被
    ``flow_worker`` / ``event_filter`` 直接写进生产日志。
    """


class RedisTarget(NamedTuple):
    """``_parse_redis_url`` 的解析结果：存储侧连接参数的唯一来源。"""

    host: Optional[str]
    port: int
    db: int
    password: Optional[str]
    username: Optional[str]
    scheme: str
    unix_socket_path: Optional[str] = None
    #: URL query 里 redis-py 认识的连接参数（如 ``socket_timeout``）——原样透传给
    #: ``redis.Redis(**kwargs)``，保证存储侧与队列侧 ``Redis.from_url`` 同参数。
    query_options: Optional[dict] = None

    @property
    def ssl(self) -> bool:
        """``rediss://`` → TLS。历史缺陷把 TLS 目标改写成明文 localhost。"""
        return self.scheme == "rediss"

    def client_kwargs(self) -> dict:
        """``redis.Redis(...)`` 连接参数（与队列 ``Redis.from_url`` 同端点）。"""
        kwargs: dict = {"db": self.db}
        if self.username is not None:
            kwargs["username"] = self.username
        if self.password is not None:
            kwargs["password"] = self.password
        if self.query_options:
            kwargs.update(self.query_options)
        if self.unix_socket_path is not None:
            kwargs["unix_socket_path"] = self.unix_socket_path
            return kwargs
        kwargs["host"] = self.host
        kwargs["port"] = self.port
        if self.ssl:
            kwargs["ssl"] = True
        return kwargs


#: 存储侧**透传**给 ``redis.Redis(**kwargs)`` 的 URL query 参数。名字与队列侧
#: ``redis.connection.URL_QUERY_ARGUMENT_PARSERS`` 同名（``db`` 单独处理），
#: 取值也用它自己的解析器转换——同一个 URL 在存储与队列必须得到同一个值。
_QUERY_PARAMS = frozenset({
    "health_check_interval",
    "legacy_responses",
    "max_connections",
    "protocol",
    "retry_on_error",
    "retry_on_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_read_size",
    "socket_timeout",
    "ssl_check_hostname",
    "ssl_exclude_verify_flags",
    "ssl_include_verify_flags",
    "ssl_min_version",
})

#: redis-py 的 ``parse_url`` 认、客户端却**用不了**的参数：``?timeout=3`` 会被塞进
#: connection_kwargs，而 ``AbstractConnection`` 没有 ``timeout`` 形参——``from_url``
#: 本身能构造，第一次连接就 ``TypeError``。与其把它传下去变成连接期炸，不如启动期
#: 具名拒绝并指向 ``socket_timeout``。这是与 ``parse_url`` 之间**唯一**保留的
#: query 分歧（见 MIGRATION.md）。
_UNSUPPORTED_QUERY_PARAMS = frozenset({"timeout"})


def _coerce_query_value(key: str, value: str, text: str) -> object:
    """已知 query 参数取值：用 redis-py 的解析器转换，非法即具名拒绝（掩码消息）。"""
    try:
        from redis.connection import URL_QUERY_ARGUMENT_PARSERS
    except ImportError:  # pragma: no cover - 没装 redis 时 storage_type=redis 本就不可用
        return value
    parser = URL_QUERY_ARGUMENT_PARSERS.get(key)
    if parser is None:  # pragma: no cover - _QUERY_PARAMS 与 redis-py 同源，防漂移
        return value
    try:
        return parser(value)
    except (TypeError, ValueError) as exc:
        # 取值在原文里有多种拼法（原文 / percent-decoded / percent-encoded），
        # 三种一起抹，否则 ``?socket_timeout=%40Pr0d2024x`` 的编码形态照样回显。
        literals = tuple(_encoding_forms(value))
        raise RedisUrlError(
            f"Redis URL 的 query 参数 {_scrub(key, text)!r} 取值非法: "
            f"{_safe_reason(exc, text, literals)}"
            f"（URL={_mask_url(_scrub_literal(text, literals))!r}）"
        ) from _sanitized_cause(exc, text, literals)


def _parse_query_options(query: dict, text: str) -> dict:
    """URL query → 客户端 kwargs（与队列侧 ``Redis.from_url`` 对齐）。

    * 已知参数（``_QUERY_PARAMS``）→ 按 redis-py 语义转换后透传；值非法 → 具名拒绝；
    * ``?timeout=`` → 具名拒绝（见 ``_UNSUPPORTED_QUERY_PARAMS``）；
    * 其余名字 → **不判死**（``?foo=bar`` 曾让生产启动直接崩）：掩码警告后忽略。
      它们无法透传——``redis.Redis.__init__`` 没有 ``**kwargs``；而队列侧也只是把未知
      名字塞进 connection_kwargs、第一次连接同样 ``TypeError``，故「忽略」不比队列侧
      更差，且不再炸启动。
    """
    options: dict = {}
    unknown = []
    for key, values in query.items():
        if key == "db" or not values:
            continue
        value = values[0]
        if key in _UNSUPPORTED_QUERY_PARAMS:
            raise RedisUrlError(
                f"Redis URL 的 query 参数 {_scrub(key, text)!r} 不被 Redis 客户端支持"
                f"（连接期会 TypeError，请改用 socket_timeout）: {_mask_url(text)}"
            )
        if key in _QUERY_PARAMS:
            options[key] = _coerce_query_value(key, value, text)
        else:
            unknown.append(key)
    if unknown:
        # r7 默认拒绝：未知名是攻击者可控文本，**连同回显的 URL 一起**抹掉再进日志
        # （只保留「有几个未知名」这个可诊断信息，名字本身不留）。两点：
        #
        # * 顺序是「先抹字面量、再掩码」——反过来的话未知名自己的 `://`/`:` 会先被
        #   当成凭据掩成 `***`，整名替换就落空、名字的前缀照样活下来；
        # * 抹的是 query 串里的**原始**名字：`parse_qs` 会把非法 percent 转义解成
        #   U+FFFD（`9yc%%bCAz1b` → `9yc%\ufffdAz1b`），解码后的名字根本不在原文里。
        unknown_set = set(unknown)
        raw_unknown = tuple(
            raw
            for raw in _raw_query_names(urlsplit(text).query)
            if unquote_plus(raw) in unknown_set
        )
        logger.warning(
            "Redis URL 含 redis-py 未定义的 query 参数 %s，已按 parse_url 语义忽略: %s",
            [_REDACTED for _ in unknown],
            _mask_url(_scrub_literal(text, raw_unknown or tuple(unknown))),
        )
    return options


def _redis_db_from_url(
    path: Optional[str], db_query: Optional[str], shown: str, text: str
) -> int:
    """URL 路径/``db`` 查询参数 → 数据库编号（非法即具名失败）。

    ``db`` 查询参数优先于路径（与 ``Redis.from_url`` 同序）；``/`` 与空路径
    都表示默认 db 0。``path=None`` 用于 ``unix://``——socket 路径不是 db，
    此时 db 只认查询参数（缺省 0）。

    ``shown`` 必须是 ``_mask_url`` 处理过的 URL——本函数的消息会进生产日志。
    ``text`` 是**原始**输入：``int()`` 失败的 cause 是另一个渲染面，要用它把
    口令从 ``_sanitized_cause`` 里洗掉（``shown`` 已被掩码，取不出口令）。
    """
    if db_query:
        try:
            db = int(db_query)
        except ValueError as exc:
            # r7 默认拒绝：非法取值本身是攻击者可控文本，消息/`__cause__`/回显的
            # URL 都不带上它（保留参数名 ``db`` 与固定措辞，诊断仍能定位到
            # 「db 查询参数」）。
            literals = tuple(_encoding_forms(db_query))
            raise RedisUrlError(
                f"Redis URL 的 db 查询参数不是整数: {_REDACTED!r}"
                f"（URL={_mask_url(_scrub_literal(text, literals))!r}）"
            ) from _sanitized_cause(exc, text, literals)
    elif path is None or path in ("", "/"):
        db = 0
    elif path.startswith("/") and path[1:].isascii() and path[1:].isdigit():
        db = int(path[1:])
    else:
        literals = tuple(_encoding_forms(path))
        raise RedisUrlError(
            f"Redis URL 的路径必须是数据库编号（形如 /0）: {_REDACTED!r}"
            f"（URL={_mask_url(_scrub_literal(text, literals))!r}）"
        )
    if db < 0:
        raise RedisUrlError(f"Redis URL 的数据库编号不能为负: {db}（URL={shown!r}）")
    return db


def _parse_redis_url(redis_url: Optional[str] = None) -> RedisTarget:
    """解析 Redis URL 为连接目标（host/port/db/username/password/scheme）。

    契约（plaita#88）：

    * **未配置**（``None`` / 空串 / 纯空白）→ 文档化默认
      ``DEFAULT_REDIS_URL``（``redis://localhost:6379/0``）。「没配 Redis」不是
      错误，保持既有行为。
    * **合法 URL** → 完整解析，且结果与队列侧 ``Redis.from_url`` 同端点：
      ``redis://`` / ``rediss://``（``ssl=True``）/ ``unix://``；端口可省略
      （默认 6379）、db 可省略（默认 0）；``user:pass@`` 的用户名不再被当成
      host、密码不再丢失；凭据按 percent-encoding 解码；IPv6 字面量
      ``redis://[::1]:6379/0`` 正确取主机。
    * **畸形/不支持** → 抛 ``RedisUrlError``（带 URL 与原因，命名到具体形态）。
      绝不静默回落 localhost——那正是本缺陷：同一进程连两个 Redis。
      消息里的 URL **一律**经 ``_mask_url`` 渲染（密码 ``***``）：它会被
      ``flow_worker`` / ``event_filter`` 原样写进生产日志。
    * **query 参数** → redis-py 认识的那些（``socket_timeout`` /
      ``health_check_interval`` / ``max_connections`` / ``ssl_*`` 等）转换后透传给
      存储客户端，与队列侧同值；``?timeout=`` 因客户端不接受而具名拒绝；未知名字
      不再判死（掩码警告后忽略），见 ``_parse_query_options``。

    r5 R3：异常链上**只剩**掩码后的 ``__cause__`` 替身——``__context__`` 会被摘成
    ``None``（见下），因为 ``raise … from exc`` 只覆盖 ``__cause__``，
    ``__context__`` 仍是那个 ``str()`` 带明文口令的**原始**异常。
    """
    try:
        return _parse_redis_url_checked(redis_url)
    except RedisUrlError as exc:
        # 隐式 ``__context__`` 是 ``raise … from`` **盖不住**的第二个渲染面：它挂着
        # 原始异常对象（``Port could not be cast ... 'pa55word'``），任何走
        # ``__context__`` / ``traceback.format_exc()`` 的消费者都能读到明文。
        # 在异常离开解析器前摘掉；``__cause__``（掩码替身）与消息原样保留。
        exc.__context__ = None
        raise


def _parse_redis_url_checked(redis_url: Optional[str] = None) -> RedisTarget:
    """``_parse_redis_url`` 的实现体（``__context__`` 的摘除在包装层做）。"""
    raw = "" if redis_url is None else redis_url
    if not isinstance(raw, str):
        raise RedisUrlError(
            f"redis_url 必须是字符串，收到 {type(raw).__name__}: {_mask_url(raw)!r}"
        )
    text = raw.strip()
    if not text:
        # 显式声明的回退（未配置）：不能把「没配」变成硬错误（#99/#95 先例）。
        return RedisTarget("localhost", 6379, 0, None, None, "redis")

    shown = _mask_url(text)
    scheme_token = text.split("://", 1)[0] if "://" in text else ""
    if scheme_token != scheme_token.lower() or scheme_token not in _REDIS_SCHEMES:
        supported = " / ".join(f"{s}://" for s in _REDIS_SCHEMES)
        raise RedisUrlError(
            f"不支持的 Redis URL: {shown!r}；只支持 {supported}"
            f"（scheme 必须小写，缺 scheme 或 http:// 之类都不是 Redis URL）"
        )

    try:
        parts = urlsplit(text)
    except ValueError as exc:  # 非法 IPv6 字面量等
        raise RedisUrlError(
            f"Redis URL 非法（{_safe_reason(exc, text)}）: {shown!r}"
        ) from _sanitized_cause(exc, text)

    if parts.fragment:
        # r7 默认拒绝：fragment 是攻击者可控文本——回显的 URL 里把它抹掉，否则
        # ``?u=X:<口令里带 '#'>`` 这类形态会把口令尾巴留在这条消息上（实测）。
        # 抹的是**原文**里从首个 ``#`` 到串尾那截：``urlsplit`` 解析前会做 WHATWG
        # 归一化把 ``\t``/``\n``/``\r`` 删掉，所以 ``parts.fragment`` 与原文不逐字
        # 相同（实测 ``#% \t\xa0…Cxz9a=1`` 洗不掉）。
        raw_fragment = text[text.find("#") :]
        raise RedisUrlError(
            f"Redis URL 不支持 fragment: "
            f"{_mask_url(_scrub_literal(text, (raw_fragment, raw_fragment[1:])) if raw_fragment else text)!r}"
            f"（凭据里的 '#' 请写成 %23）"
        )
    query = parse_qs(parts.query, keep_blank_values=True)
    # 空 options 归一成 None：与「未配置」早退分支的 RedisTarget 可逐字段比较。
    options = _parse_query_options(query, text) or None
    db_query = query.get("db", [None])[0]
    username = unquote(parts.username) if parts.username else None
    password = unquote(parts.password) if parts.password else None

    if scheme_token == "unix":
        socket_path = unquote(parts.path) if parts.path else None
        if not socket_path:
            raise RedisUrlError(f"unix:// URL 缺少 socket 路径: {shown!r}")
        db = _redis_db_from_url(None, db_query, shown, text)
        return RedisTarget(
            None, 0, db, password, username, "unix", socket_path, options
        )

    try:
        host = unquote(parts.hostname) if parts.hostname else None
        port = parts.port
    except ValueError as exc:  # 端口非整数 / 端口越界
        raise RedisUrlError(
            f"Redis URL 非法（{_safe_reason(exc, text)}）: {shown!r}"
        ) from _sanitized_cause(exc, text)

    if not host:
        raise RedisUrlError(f"Redis URL 缺少主机名: {shown!r}")
    if port is None:
        port = 6379
    if port == 0:
        raise RedisUrlError(f"Redis URL 的端口不能为 0: {shown!r}")
    db = _redis_db_from_url(parts.path, db_query, shown, text)
    return RedisTarget(host, port, db, password, username, scheme_token, None, options)


def _create_async_engine(database_url: str):
    """从 database_url 创建 SQLAlchemy 异步引擎（event/subscription db 后端需要）。"""
    if not database_url:
        raise ValueError("db 后端需要 database_url")
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(database_url)


def _require_experimental_db(feature: str) -> None:
    if os.environ.get(_EXPERIMENTAL_DB_ENV) == "1":
        logger.warning(
            "%s: enabling experimental db/sqlalchemy backend (%s=1)",
            feature,
            _EXPERIMENTAL_DB_ENV,
        )
        return
    raise ValueError(
        f"{feature} 的 type='db'（SQLAlchemy）为 experimental，不在生产控制面支持范围内。"
        f"请改用 redis（推荐）或 memory（单测）。"
        f"若仅为实验，设置环境变量 {_EXPERIMENTAL_DB_ENV}=1 后重试。"
    )


# FlowWorker / EventFilter 以同步方式调用 ExecutionStorage / FlowStorage。
# Sqlalchemy* 实现是 async def，与 ABC 契约断裂，经 factory 创建会导致静默丢状态。
_UNSUPPORTED_SYNC_DB_COMPONENTS = frozenset({"execution", "flow"})


def create_storage_component(storage_type, component_type, **kwargs):
    """
    创建存储组件实例

    Args:
        storage_type: 存储类型 (memory, redis, db)
            - execution / flow：仅 memory、redis（db 永久拒绝，契约断裂）
            - subscription：memory、redis；db 需 PLAITA_ALLOW_EXPERIMENTAL_DB=1
        component_type: 组件类型 (execution, flow, subscription)
        **kwargs: 组件初始化参数 (redis_url, database_url)

    Returns:
        创建的存储组件实例
    """
    if storage_type == "memory":
        if component_type == "execution":
            from plaita.storage.memory import MemoryExecutionStorage
            return MemoryExecutionStorage()
        elif component_type == "flow":
            from plaita.storage.memory import MemoryFlowStorage
            return MemoryFlowStorage()
        elif component_type == "subscription":
            from plaita.event.memory import InMemoryEventSubscriptionStorage
            return InMemoryEventSubscriptionStorage()

    elif storage_type == "redis":
        redis_url = kwargs.get("redis_url", DEFAULT_REDIS_URL)

        if component_type in ("execution", "flow"):
            # #88：解析在建立连接之前完成；畸形/不支持的 URL 在这里抛
            # RedisUrlError（具名、可操作），不再静默回落 localhost:6379/0
            # ——那会让存储与队列/事件总线连到两个 Redis。
            target = _parse_redis_url(redis_url)
            conn = target.client_kwargs()
            # 多租户路由包装器：按 ContextVar 租户选择 namespace
            # （default → 历史 plaita 前缀；其余 plaita:{tenant}）
            if kwargs.get("tenant_routing"):
                from plaita.server.tenant_context import (
                    TenantRoutingExecutionStorage,
                    TenantRoutingFlowStorage,
                )
                if component_type == "execution":
                    return TenantRoutingExecutionStorage(**conn)
                return TenantRoutingFlowStorage(**conn)
            if component_type == "execution":
                from plaita.storage.redis import RedisExecutionStorage
                return RedisExecutionStorage(**conn)
            else:
                from plaita.storage.redis import RedisFlowStorage
                return RedisFlowStorage(**conn)
        elif component_type == "subscription":
            from plaita.event.redis import RedisEventSubscriptionStorage
            return RedisEventSubscriptionStorage(redis_url=redis_url, key_prefix="plaita:subscription:")

    elif storage_type == "db":
        if component_type in _UNSUPPORTED_SYNC_DB_COMPONENTS:
            raise ValueError(
                f"storage_type='db' 不支持 component_type={component_type!r}："
                "SqlalchemyExecutionStorage / SqlalchemyFlowStorage 为 async 实现，"
                "与 ExecutionStorage/FlowStorage 同步 ABC 及 FlowWorker/EventFilter "
                "的同步调用不兼容，会导致状态无法落盘。"
                "请改用 storage_type='redis' 或 'memory'。"
            )
        _require_experimental_db(f"create_storage_component({component_type})")
        database_url = kwargs.get("database_url")
        if component_type == "subscription":
            from plaita.event.sqlalchemy import SqlalchemyEventSubscriptionStorage
            return SqlalchemyEventSubscriptionStorage(engine=_create_async_engine(database_url))
        raise ValueError(f"不支持的组件类型: {component_type}")
    else:
        raise ValueError(f"不支持的存储类型: {storage_type}")


def create_event_bus(bus_type, **kwargs):
    """
    创建事件总线实例

    Args:
        bus_type: 事件总线类型 (memory, redis, db)
            db 需 PLAITA_ALLOW_EXPERIMENTAL_DB=1；生产请用 redis
        **kwargs: 组件初始化参数 (redis_url, database_url)

    Returns:
        创建的事件总线实例
    """
    if bus_type == "memory":
        from plaita.event.memory import InMemoryEventBus
        return InMemoryEventBus()
    elif bus_type == "redis":
        from plaita.event.redis import RedisEventBus
        return RedisEventBus(redis_url=kwargs.get("redis_url"))
    elif bus_type == "db":
        _require_experimental_db("create_event_bus")
        from plaita.event.sqlalchemy import SqlalchemyEventBus
        return SqlalchemyEventBus(engine=_create_async_engine(kwargs.get("database_url")))
    else:
        raise ValueError(f"不支持的事件总线类型: {bus_type}")
