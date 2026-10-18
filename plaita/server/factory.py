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
  **#165 的溢出分支**：输入的**形状**超出 #165 的预算时（``_MAX_SPAN_SCAN_WORK``
  形状闸门，或 ``_MAX_CLOSURE_CHARS`` / ``_MAX_REDACTION_CHARS`` 字符预算）
  ``_mask_url`` fail-closed 退化为常量 ``***``、``_passwords`` 返回 ``(text,)``
  （整条原文当唯一 needle，即「整段洗掉」的保守解）。不变量因此在该分支上照旧
  成立：``'***'`` 里没有原文的任何字节，而 ``(text,)`` 非空。溢出只由**分隔符
  密度与规模**决定，与凭据内容无关。
* **完备性才是验收方向（r7）**：oracle（``redis.connection.parse_url``，并用
  ``urlsplit`` 与 ``_parse_redis_url`` 交叉核对）报出的口令，任何 ≥3 前缀/后缀
  与所有 ≥3 子串都不得出现在五个渲染面上。**自洽不蕴含完备**——r6 的
  ``_passwords`` 与 ``_mask_url`` 完全自洽，仍然泄露后缀 ``Ccc``。
* **r8：机制对分隔符闭合 + 非 authority 值位置 default-deny**。第五次复发
  （``redis://user:AA@BB@SuperSecretTail@host:6379/0?u=X:AA@BB@SuperSecretTail``）
  的根因是「每个分隔符只切第一个出现位置」+「非 authority 跨度停在最后一个
  ``@``」。现在 ``_secret_forms`` 是**不动点闭包**（``_delimiter_pieces`` 切每个
  出现位置，片段重新入队；闭合性质见其 docstring）；``_span_at`` /
  ``_no_at_colon_spans`` 在非 authority 位置**掩到值 token 末尾**（``&``/``#``/
  串尾），裸 query 名/取值/path 段/fragment 按 ``_query_segment_is_provably_benign``
  / ``_token_is_provably_benign`` 默认拒绝；调用点点名的字面量与跨度进**同一趟**
  渲染（``_mask_url(..., literals=...)``）。**完备性 = 对分隔符的闭包 + 每个可能
  承载凭据的非 authority 值位置都被覆盖**。
"""
import contextvars
import functools
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

#: 口令内部的分隔符（**闭包的分隔符集**）：结构层会把口令按这些字符切开——
#: userinfo 的 ``:``/``@``、query 的 ``?``/``&``/``=``、path 的 ``/``、
#: fragment 的 ``#``、``%`` 转义的分界、以及空白。切开后的头/尾截、以及**每一
#: 个出现位置**切出来的片段都是同一个口令的书写形态，都要重洗——
#: ``redis://user:Pw?Tail@host`` 的 query 字段里出现的是 ``Tail``，
#: ``ByCAa/x9xczyz://9b9bz1`` 在 path 里按 ``/`` 分段出现。
#:
#: r8 更正 r4-r7：``/``、``%`` 与空白此前**不在**这个集合里，于是
#: ``_secret_forms`` 永远切不出 path 段级/转义级的片段（r8 实测的
#: ``/x9xczyz:`` 残余）。
_SECRET_SPLITTERS = (
    ":",
    "@",
    "?",
    "#",
    "&",
    "=",
    "/",
    "%",
    " ",
    "\t",
    "\n",
    "\r",
    "\xa0",
)

# --------------------------------------------------------------------------- #
# plaita#165：清洗器的**复杂度上界**（硬上限 + fail-closed）
# --------------------------------------------------------------------------- #
# 背景（实测，见 /tmp 的证据与 MIGRATION.md 的 #165 节）：``_mask_url`` 对
# ``"x://" * n`` 这类**分隔符密集**的输入是**超二次**的，两个独立的来源：
#
# 1. ``_secret_forms_cached`` 的**闭包**：形态集要求「每个分隔符的每个出现位置
#    都切一刀」⇒ 分隔符出现 k 次就有 Θ(k²) 个片段，片段长度又是 O(n)
#    ⇒ Θ(n³) 字符工作（n = 输入长度）。实测 ``"x://"*40``（160 字符）里
#    ``_push`` 被调用 **3,471,881** 次（cProfile）。
# 2. ``_no_at_colon_spans`` 的**逐候选前向扫描**：每个候选冒号都要扫到
#    ``?#&@`` / 端口形 / 串尾，而 ``"x://"*n`` 里没有 ``?#&@`` ⇒ 每个候选扫到
#    串尾 ⇒ Θ(k²)（实测 n=1000 时 ``_is_port_colon`` 被调用 **499,500** 次）。
#
# 清洗器挂在**第三方错误文本**路径上（``HTTP.handle_http_node_err`` 等），输入
# 长度由对端决定 ⇒ 几百 KB 就能占满 worker（与 #68「输入规模决定成本」同族）。
#
# 修法遵循本模块既有的两条纪律：**要么在预算内精确**、**要么 fail-closed**
# （``_mask_url`` 退化常量、绝不回原文），**绝不半掩**（半掩 = 悄悄漏）。
#
# 预算一律按**字符数**而不是「输入长度」或「条数」计费，理由有实测支撑：
# ``test_masker_is_linear_on_long_input`` 钉住「70 000 字符的口令必须一趟掩完」
# （那条口令只有 1 个分隔符 ⇒ 闭包是 O(n)、本该线性），所以**长度上限**会误杀
# 真正线性的长输入；而 ``"x://"*n`` 的代价来自**分隔符密度**。字符预算正好只惩罚
# 后者：同样的 n，分隔符稀疏时闭包是 O(n) 字符、稠密时是 Θ(n³)。
#
# * :data:`_MAX_SPAN_SCAN_WORK`——**输入形状闸门**：``len(text) * text.count(":")``
#   是 ``_credential_spans`` 里两个 Θ(n²) 项的计费量（``_no_at_colon_spans`` 的
#   逐候选前向扫描 + ``_query_segment_is_provably_benign`` 的切片，两者都按
#   「候选冒号数 × 扫描长度」付费）。它是 O(1) 的（``str.count`` 走 C），超限即
#   fail-closed，不进入 ``_credential_spans``。
# * :data:`_MAX_CLOSURE_CHARS`——**单个凭据候选**的闭包字符预算（种子 + 每个派生
#   形态 + 每个切片都按长度计费，见 :func:`_secret_forms_cached`）。
# * :data:`_MAX_REDACTION_CHARS`——**一次清洗累计**的字符预算（ContextVar 传递，
#   见 :data:`_closure_budget`），覆盖「凭据片」与「闭包」两部分。单条跨度合规
#   不蕴含总量合规：一条 URL 上可以有 O(n) 条各不超限的跨度。
#
# **直调 ``_secret_forms`` 不受预算限制**（ContextVar 默认 ``None``）：那条路是
# r8 闭合性质的契约面（``test_secret_forms_closed_under_resplitting`` 直接对它
# 断言「每个分隔符的每个出现位置的头/尾截都在形态集里」），预算只加在
# **掩码/口令路径**上——那才是 #165 的 DoS 面。
#
# 标定（实测，见 MIGRATION.md 的 #165 节与 RED-PROOF.md 的曲线表）：
#
# ====================================  ==================  ====================
# 形状                                   字符计费量           实测耗时
# ====================================  ==================  ====================
# ``"P"*69990 + " Secret"``（1 分隔符）  ~7×10⁵（O(n)）      一趟线性
# 语料最重的真实形状（79 字符、稠密）     ~9×10⁵ / 88,992 切   20 ms
# ``"x://"*40``（160 字符）              2.8×10⁵ 切 / 2×10⁷   51 ms（无预算）
# ``"x://"*400``（1 600 字符）           ~2.8×10⁶ 切          本应在秒级
# ``"x://"*20000``（80 KB）              —                    > 30 s（不返回）
# ====================================  ==================  ====================
_MAX_SPAN_SCAN_WORK = 1 << 21
_MAX_CLOSURE_CHARS = 1 << 20
_MAX_REDACTION_CHARS = 1 << 24


class _RedactionOverflow(Exception):
    """#165：清洗的形态闭包**超出预算**——调用方必须整体 fail-closed。

    刻意继承 ``Exception`` 而不是复用 ``ValueError``/``RuntimeError``：
    ``_mask_url`` 的 total 契约（``#158`` r1 A）要求它**任何**输入都不上抛，
    这个信号只在模块内的两个调用点（``_mask_url`` / ``_passwords``）被吃掉。
    """


#: 一次清洗的**累计**闭包切点预算（元素为剩余额度），由 :func:`_mask_url` /
#: :func:`_passwords` 在调用期间设置、``finally`` 复位。默认 ``None`` = **不设限**
#: ——直调 :func:`_secret_forms` 的路径（r8 闭合性质契约面）逐字保持旧行为。
#:
#: 用 ``ContextVar`` 而不是模块级计数器：``_mask_url`` 会被多个 flow worker 线程
#: 并发调用，共享计数器会让**另一个线程的消耗**在这里造成假 fail-closed（丢诊断，
#: 且测试会 flaky）。ContextVar 对每个线程/任务各有一份，天然隔离。
_closure_budget: "contextvars.ContextVar[Optional[list]]" = contextvars.ContextVar(
    "plaita_mask_closure_budget", default=None,
)


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
    #: r8：这条跨度来自**非 authority 值位置的 default-deny**（未声明 query 名 /
    #: 裸取值 / 裸 path 段 / fragment），不是「凭据形态」本身。它的重洗形态里
    #: 子 3 字符的片段（``?u=…`` 的 1 字符名字、``=`` 切出的残片）不参与重洗——
    #: 满足不了 ≥3 的完备性判据，误伤面却很大（实测会把 userinfo 的 ``u`` 洗成
    #: ``redis://***ser:***@…``）。凭据跨度（``_at_colon_spans`` 等）的短片段照旧
    #: 参与（r6 已验证的 ``redis:/u@u:p/w@/0@x`` → ``redis:/***@***:***@x``）。
    default_deny: bool = False


def _value_token_stop(text: str, origin: int) -> int:
    """``origin`` 起**值 token** 的末尾：下一个字段分隔符 ``&``/``#``/``/`` 或串尾。

    authority **之外**没有 host——query 值 / path 段 / fragment 的「值」以字段
    分隔符收边（``?u=X:AA@BB@Tail&v=1`` 的第一个值 token 是 ``X:AA@BB@Tail``）。
    """
    stop = len(text)
    for ch in "&#/":
        idx = text.find(ch, origin)
        if idx != -1 and idx < stop:
            stop = idx
    return stop


def _residual_tail_is_benign(text: str, at: int) -> bool:
    """非 authority 位置里配对 ``@`` **之后的残余尾巴**可证良性吗（r8 限定项）。

    ``@`` + 1 个字符只有 2 个字符，短于完备性判据的下限（≥3）——不可能承载口令
    片段；而 r2 契约（``?db=Acct:***@x``）与 r4 已验证的 keep 形态
    （``?ref=a@b``、``?db=1@2``、``host:6379/0@x``、``host/@x``）用的**正是**
    单字符尾，所以它们逐字不变。

    长于 1 个字符的尾巴一律按凭据处理（default-deny）：
    ``?u=X:AA@BB@SuperSecretTail`` 的 ``SuperSecretTail`` 正是 r8 的规则级反例
    ——它既不在任何 ``[mask_from, stop)`` 区间里、也不在 needle 集里。判据不看
    字符是不是数字（``?u=X:AA@BB@2`` 与 ``…@x`` 同类），只看「能否承载 ≥3 片段」
    这一个完备性事实，不引入新的形态白名单。
    """
    return _value_token_stop(text, at + 1) - (at + 1) <= 1


def _tail_is_credential_material(
    text: str, colon_at: int, sep: int, auth_start: int, auth_stop: int
) -> bool:
    """非 authority 冒号之后的尾巴是**凭据材料**（而不是 authority 的 host）吗。

    ``@`` 之后是 host 的两种情形不适用本轮的「掩到值 token 末尾」：
    ``redis:/user:pw@host``（单斜杠笔误）与 ``user:pw@host:6379``（完全没有
    scheme）——它们的冒号虽然落在首个 ``/?#`` 之后（``_span_at`` 的区域判据只能
    看到 ``redis:`` 之后那个 ``/``），但 r5 的粗档按「``@`` 之后是 host」处理，
    是逐字已验证的 keep。判据因此要求**作证**：冒号之前有 query/fragment 分隔符
    （``?``/``#``），或者输入有字面 ``://`` 且冒号确实在 authority 之外。
    """
    if "?" in text[:colon_at] or "#" in text[:colon_at]:
        return True
    # ``auth_start < auth_stop``：authority **非空**。``redis:///user:pw@host``
    # 的 authority 是空的（多写了一个斜杠），r3/r5 已验证它按 ``@`` 之后是 host
    # 处理（``redis:///user:***@host``）——那截不能吞。
    return sep != -1 and auth_start < auth_stop and colon_at >= auth_stop


def _span_at(text: str, colon_at: int) -> int:
    """候选冒号配对的 ``@``——边界**按冒号所在区域**取。

    * **authority 里**（``://`` 之后到首个 ``/?#``）：口令段止于首个 ``/``
      （r5 的 ``_PATH_DELIMITERS``）。``redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1@x``
      的 userinfo ``@`` 因此不会越权吃掉 query 里的凭据（r5 R2），而
      ``redis://user:?@00bbb0?  @host`` 里口令自己的 ``@``/``?`` 也不会把它
      截成空口令；
    * **authority 之外**（query / path / fragment）：取该冒号之后**最后**一个
      ``@``（r5 ``_global_credential_span`` 的语义）——``?db=Acct:b0@ /@x`` 的
      口令跨 ``@``/``/``，取首个 ``/`` 会把它截断。**r8：不再停在那个 ``@``**
      ——配对 ``@`` 之后的尾巴若长于 1 个字符（``_residual_tail_is_benign``），
      口令段一直掩到**值 token 末尾**：authority 之外没有 host，``@`` 之后的文本
      同样是凭据材料（``?u=X:AA@BB@SuperSecretTail`` → ``?u=X:***``）。

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
    at = text.rfind("@", colon_at)
    if (
        at != -1
        and _tail_is_credential_material(
            text, colon_at, sep, auth_start, auth_stop
        )
        and not _residual_tail_is_benign(text, at)
    ):
        return _value_token_stop(text, at + 1)
    return at


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
    auth_stop = _delimiter_stop(text, scan_from, _AUTHORITY_DELIMITERS)
    for colon_at in _candidate_colons(text, scan_from):
        end = colon_at + 1
        port_stop = False
        while end < n and text[end] not in "?#&@":
            if text[end] == ":" and _is_port_colon(text, end, -1):
                port_stop = True
                break
            end += 1
        # r8：**非 authority 的值 token** 不在 ``?``/``@`` 收边——query 取值里的
        # ``?`` 是取值的一部分（``?u=X:Ccyy%%BBCa0xb??99xaxzx`` 的 ``??99xaxzx``
        # 在 r7 上原样回显）。掩到值 token 末尾（``&``/``#``/串尾），与
        # ``_span_at`` 的尾巴规则同一条。端口形截断（``127.0.0.1:679``）不受影响。
        if (
            not port_stop
            and sep != -1
            and scan_from < auth_stop
            and colon_at >= auth_stop
        ):
            extended = end
            while extended < n and text[extended] not in "&#":
                extended += 1
            # r2/r5 的单字符尾（``?db=Pr0d:x@y`` → ``?db=Pr0d:***@y``）不在延长
            # 范围里——值 token 可证良性时保持原样。
            if not _query_segment_is_provably_benign(text[colon_at + 1 : extended]):
                end = extended
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


def _is_declared_query_name(name: str) -> bool:
    """这个 query 名是**声明过的**（`db` / redis-py 的 ``_QUERY_PARAMS``）吗。

    r8 的 default-deny 只对**没有声明过**的名字生效：名字是攻击者可控文本
    （r7 的未知名 WARNING 已经这么处理），而 ``socket_timeout`` /
    ``max_connections`` 这些是诊断必需的固定词。``_QUERY_PARAMS`` 定义在文件
    后半部，故这里按调用期解析而不是模块期常量。
    """
    return name == "db" or name in _QUERY_PARAMS


def _token_is_provably_benign(token: str) -> bool:
    """非 authority 的**裸 token** 可证良性吗（r8 的 default-deny 判据）。

    良性只有两种，都不依赖「我以为它不是口令」：

    * 空 token；
    * 纯 ASCII 十进制（``/0``、``?db=2``、``?socket_timeout=5`` 这类编号/数值）；
    * 含凭据分隔符 ``:`` / ``@`` 的 token——它们由 ``_at_colon_spans`` /
      ``_no_at_colon_spans`` / ``_bare_at_token_spans`` 三条跨度腿**精确**处理，
      r2/r4 的 keep 形态（``?ref=a@b``、``?db=1@2``、``?x=1:2@y``、
      ``host:6379/0@x``）正是这些。

    其余一律按凭据处理：**裸 token 与口令在文本上不可区分**
    （``?socket_timeout=Pr0d2024x``、``?Pr0d2024x=1``、``redis://host/0/Pr0d2024x``
    在 r7 上原样返回，零覆盖）。``%`` 编码的取值也算裸 token
    （``?socket_timeout=%40Pr0d2024x``）。
    """
    if not token:
        return True
    if token.isascii() and token.isdigit():
        return True
    return ":" in token or "@" in token


def _query_segment_is_provably_benign(segment: str) -> bool:
    """query 段（名字 / 取值 / fragment）可证良性吗——**紧**判据。

    与 ``_token_is_provably_benign``（path 段用的松判据）的差别只在含 ``@`` 的段：
    query 里没有 host，``@`` 之后的文本同样是凭据材料，所以要求**最后一个 ``@``
    的尾巴短到承载不了 ≥3 片段**（``_residual_tail_is_benign``）才算良性。

    r2/r4 已验证的 keep 形态正好都是单字符尾（``?ref=a@b``、``?db=1@2``、
    ``?x=1:2@y``、``?u=X:Y@x``、``?db=Acct:***@x``、``?user:***@x``），逐字不变；
    ``?aa1??9zAC@@y0bbyCb=1`` / ``?u=AA@BB@SuperSecretTail`` 这类长尾段整段掩。
    """
    if not segment:
        return True
    if segment.isascii() and segment.isdigit():
        return True
    if "@" in segment:
        # 尾巴量到**段末**（不是下一个字段分隔符）：query/fragment 里没有 host，
        # ``@`` 之后的文本即便以 ``/``/``:`` 开头也仍是凭据材料（r8 实测
        # ``?ybAbbabzAZ09@:/?#…`` 的 ``@:/`` 用「到下一个 /」量是 1 字符 ⇒ 漏判）。
        if len(segment) - (segment.rfind("@") + 1) <= 1:
            return True
        # r5 B 契约（``redis://user?Tr0ub4dor@host:6379/0`` → 只掩 ``Tr0ub4dor``）：
        # **恰好一个** ``@`` 且尾巴是 host 形时保留 ``@host…``。两个及以上的 ``@``
        # 已经证明这段是凭据材料（``?u=AA@BB@SuperSecretTail``）⇒ 整段掩。
        return (
            segment.count("@") == 1
            and _host_like_tail(segment[segment.rfind("@") + 1 :])
        )
    return ":" in segment


def _host_like_tail(tail: str) -> bool:
    """``@`` 之后那截是 r5 B 契约里的 ``host[:port][/path]`` 形吗。

    ``redis://user#Tr0ub4dor@host:6379/0``（``:`` 写成 ``#``）的 ``@`` 之后是
    host——r5 已验证只掩 ``Tr0ub4dor``、保留 ``@host:6379/0``。**fragment** 的
    default-deny 因此对「``@`` + host 形尾巴」保持这个形态；其余（``@:/?#% \t…``
    这类含 query 分隔符的尾巴）整段掩。query 段不用本判据——那里没有 host，
    ``@`` 之后的文本一律按凭据处理（``?u=AA@BB@SuperSecretTail`` 整段掩）。
    """
    if not tail:
        return True
    i, n = 0, len(tail)
    while i < n and (tail[i].isalnum() or tail[i] in ".-_[]"):
        i += 1
    if i == 0:
        return False
    if i < n and tail[i] == ":":
        i += 1
        start = i
        while i < n and tail[i].isdigit():
            i += 1
        if i == start:
            return False
    if i == n:
        return True
    return tail[i] == "/"


def _fragment_is_provably_benign(fragment: str) -> bool:
    """fragment 文本可证良性吗（r8）。

    r7 的 fragment 只在**具名拒绝**的回显里被字面抹掉，``_mask_url`` 本身对它
    零覆盖：``?{secret}#…`` 这类形状（口令里带 ``#``）会让 ``#`` 之后的文本落在
    另一个字段里原样回显。判据：不可证良性的一律**整段掩**；唯一例外是 r5 B 的
    ``@`` + ``host[:port][/path]`` 契约（逐字已验证）。
    """
    if not fragment or (fragment.isascii() and fragment.isdigit()):
        return True
    # r8：fragment 里**没有 host 可读**——只有 r5 B 的「``@`` + host 形尾巴」例外
    # （``#Tr0ub4dor@host:6379/0``）。``#% \t…z11xz://ba1c`` 这类含 ``:`` 的
    # fragment 一律整段掩，否则 ``:`` 之前那截会原样回显。
    if "@" in fragment:
        return _host_like_tail(fragment[fragment.rfind("@") + 1 :])
    return False


def _query_pair_spans(query_start: int, query_stop: int, text: str) -> tuple:
    """query 里**未声明名字 / 裸取值**的 default-deny 跨度（r8）。

    逐个 ``&`` 段处理（与 ``parse_qs`` 的分段一致）：名字不在
    ``_DECLARED_QUERY_NAMES`` 且不含 ``:``/``@`` ⇒ 掩名字；取值不良性 ⇒ 掩取值。
    ``socket_timeout=Pr0d2024x`` 因此是 ``socket_timeout=***``（名字保留给诊断），
    ``Pr0d2024x=1`` 整段变 ``***``（名字就是攻击者可控文本）。带 ``:``/``@`` 的段
    交给跨度腿——``?ref=a@b`` / ``?u=X:Y@x`` 这些 r2/r4 已验证形态逐字不变。
    """
    spans = []
    i = query_start
    while i <= query_stop:
        amp = text.find("&", i)
        end = query_stop if amp == -1 or amp > query_stop else amp
        eq = text.find("=", i, end)
        name = text[i : end if eq == -1 else eq]
        if _is_declared_query_name(name):
            # 名字是声明过的固定词（诊断必需）：只对**不可证良性的取值** default-deny。
            if eq != -1 and not _query_segment_is_provably_benign(text[eq + 1 : end]):
                spans.append(_CredentialSpan(eq + 1, eq + 1, end, True))
        elif not _query_segment_is_provably_benign(text[i:end]):
            # 未声明且整段不可证良性：名字与取值都不可证（``?Pr0d2024x=1`` 的名字
            # 就是攻击者可控文本）⇒ 整段掩。r2/r4 的 keep 形态（单字符尾）逐字不变。
            spans.append(_CredentialSpan(i, i, end, True))
        if amp == -1 or amp >= query_stop:
            break
        i = amp + 1
    return tuple(spans)


def _path_segment_spans(path_start: int, path_stop: int, text: str) -> tuple:
    """path 段（redis/rediss 的 db 位与额外段）的 default-deny 跨度（r8）。

    ``redis://host:6379/0/Pr0d2024x`` 的 ``Pr0d2024x`` 在 r7 上原样返回（零覆盖），
    而 ``#159`` 的整个前提就是「秘密在 path」。规则与 query 取值同一条：段不良性
    就整段掩。**空 authority**（``redis:///user:pw@host``）与 **unix socket 路径**
    不走这条——前者的 ``@`` 之后是 host（r5 粗档的已验证形态），后者本来就不是
    db/凭据位。

    r9 反例（``redis://host:6379/X:Pw&Sec9`` → ``X:***&Sec9``）：含 ``:`` 的段被
    ``_token_is_provably_benign`` 交给冒号腿，而冒号腿在 ``&`` 处收边
    （``_no_at_colon_spans`` 的延长与 ``_value_token_stop`` 都止于 ``&``），``&``
    之后的裸残段无人认领——path 扫描只按 ``/`` 切，不再有规则覆盖它。规则：
    **已被证明携带凭据的 path 段污染到段末——段内按 ``&`` 切出的每个裸子 token
    （不含 ``:``/``@``，那些归冒号腿）重走同一条 default-deny 判据，不可证良性
    就掩到子 token 末尾**；无凭据段的路径（含 ``&`` 与否）逐字不变。
    """
    spans = []
    i = path_start
    while i <= path_stop:
        slash = text.find("/", i)
        end = path_stop if slash == -1 or slash > path_stop else slash
        token = text[i:end]
        if not _token_is_provably_benign(token):
            spans.append(_CredentialSpan(i, i, end, True))
        elif ":" in token or "@" in token:
            j = i
            while j < end:
                amp = text.find("&", j, end)
                sub_end = end if amp == -1 else amp
                sub = text[j:sub_end]
                if (
                    sub
                    and ":" not in sub
                    and "@" not in sub
                    and not _token_is_provably_benign(sub)
                ):
                    spans.append(_CredentialSpan(j, j, sub_end, True))
                if amp == -1:
                    break
                j = amp + 1
        if slash == -1 or slash >= path_stop:
            break
        i = slash + 1
    return tuple(spans)


def _non_authority_default_deny_spans(text: str) -> tuple:
    """非 authority **值位置**的 default-deny（r8 要求 2 的结构性一片）。

    只处理「确实在 authority 之外」的位置：有字面 ``://`` **且** authority 非空
    （``auth_start < auth_stop``）。``redis:/user:pw@host``（单斜杠）与
    ``redis:///user:pw@host``（空 authority）不是值位置，交给 r5 的粗档。unix://
    的 path 是 socket 路径，不参与。
    """
    sep = text.find("://")
    if sep == -1:
        return ()
    auth_start = sep + 3
    auth_stop = _delimiter_stop(text, auth_start, _AUTHORITY_DELIMITERS)
    if auth_start >= auth_stop:
        return ()
    scheme = text[:sep]
    query_at = text.find("?", auth_stop)
    frag_at = text.find("#", auth_stop)
    spans = []
    path_stop = min(
        [x for x in (query_at, frag_at, len(text)) if x != -1]
    )
    if scheme != "unix":
        spans.extend(_path_segment_spans(auth_stop + 1, path_stop, text))
    if query_at != -1 and (frag_at == -1 or query_at < frag_at):
        query_stop = frag_at if frag_at != -1 else len(text)
        spans.extend(_query_pair_spans(query_at + 1, query_stop, text))
    if frag_at != -1 and not _fragment_is_provably_benign(text[frag_at + 1 :]):
        spans.append(_CredentialSpan(frag_at + 1, frag_at + 1, len(text), True))
    return tuple(spans)


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
    default_deny = _non_authority_default_deny_spans(text)
    coarse = _no_scheme_coarse_span(text, at_spans)
    failed = _authority_fail_closed_span(text) if not (at_spans or coarse) else ()
    bare = _bare_at_token_spans(
        text,
        _covered_at(
            text,
            at_spans + no_at_spans + default_deny + coarse + failed,
        ),
    )
    return tuple(
        sorted(
            at_spans + no_at_spans + default_deny + coarse + failed + bare,
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


@functools.lru_cache(maxsize=65536)
def _quoted_forms(text: str) -> frozenset:
    """``quote(text, safe="")`` 的大小写两种拼法；孤立代理字符按**字节**渲染。

    ``os.environ`` 用 ``surrogateescape`` 把非 UTF-8 字节解码成孤立代理字符
    （``0xFF`` → ``U+DCFF``），``quote`` 内部的 ``utf-8`` 编码随即抛
    ``UnicodeEncodeError``。兜底分两种，都以**字节**为准：

    * ``surrogateescape`` 能编回的（``U+DC80``-``U+DCFF``）：拿到的是**原始字节**，
      于是 ``_quoted_forms('\\udcff')`` = ``%FF``——这正是 on-the-wire 形态
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


def _percent_decoded(text: str) -> str:
    """``unquote(text)`` 的**不抛**版本（含 ``%`` 的孤立代理串会触发 utf-8 编码失败）。"""
    try:
        return unquote(text)
    except UnicodeError:  # pragma: no cover - 兜底分支，行为与 _encoding_forms 一致
        return text


@functools.lru_cache(maxsize=65536)
def _encoding_forms(secret: str) -> frozenset:
    """一个字符串的三种书写形态：原文 / percent-decoded / percent-encoded。

    **对任意 ``str`` 都不抛**（r5 A）：``_mask_url`` 必须是 total——它挂在
    ``_parse_redis_url`` 的每个拒绝分支上，抛出去就是启动期硬崩（#158 实测）。
    """
    forms = {secret}
    decoded = _percent_decoded(secret)
    if decoded:
        forms.add(decoded)
        forms |= _quoted_forms(decoded)
    forms |= _quoted_forms(secret)
    forms.discard("")
    return frozenset(forms)


@functools.lru_cache(maxsize=65536)
def _delimiter_pieces(text: str) -> tuple:
    """``text`` 在 ``_SECRET_SPLITTERS`` **每一个**出现位置上的头/尾截。

    r7 只取每个分隔符的**第一个**出现位置（``variant.find(sep)``），于是
    ``AA@BB@SuperSecretTail`` 只切出 ``AA`` 与 ``BB@SuperSecretTail``——
    第二个 ``@`` 之后的 ``SuperSecretTail`` 永远不是 needle（r8 的规则级反例）。
    这里把**每一个**出现位置都切开（含中间片段：对 ``a?b?c`` 切两次得到
    ``a``/``b?c`` 与 ``a?b``/``c``，闭包迭代后再得 ``b``、``c``）。
    """
    out = []
    for sep in _SECRET_SPLITTERS:
        start = 0
        while True:
            cut = text.find(sep, start)
            if cut == -1:
                break
            for piece in (text[:cut], text[cut + 1 :]):
                # 纯分隔符的片段（``@@@A`` 切出的 ``@``/``@@``）也进**形态集**——
                # 闭包性质要求「所有子形态都在」。它们是结构本身，**重洗层**不拿
                # 它们当 needle（见 ``_render_redacted``），否则结构性 ``@`` 会被
                # 抹成 ``redis://******host…``。
                if piece:
                    out.append(piece)
            start = cut + 1
    return tuple(out)


def _secret_forms(secret: str) -> tuple:
    """一个口令候选的**所有书写形态**——对分隔符集**闭合**（r8）。

    兜底重洗（见 ``_mask_url``）要认出「同一个口令换个写法又出现在 query/path
    里」——``?socket_timeout=@Pr0d2024x`` 与 ``?socket_timeout=%40Pr0d2024x``
    都必须能被洗掉。按长度降序返回：长形态先替换，避免短形态把长形态切碎。

    **闭合性质（r8 的结构性要求）**：返回值对 ``_SECRET_SPLITTERS`` **闭合**——
    对任意返回的形态 ``f``、任意分隔符 ``sep``、``f`` 里 ``sep`` 的**每一个**
    出现位置 ``k``，``f[:k]`` 与 ``f[k+1:]`` 也都在返回值里；且 ``f`` 的
    percent-decoded 形态同样在（并对它继续按分隔符切）。r6/r7 的实现是「每个
    分隔符只切**第一次**」的单趟生成器，于是 ``AA@BB@SuperSecretTail`` 的
    ``SuperSecretTail`` 既不在任何跨度里、也不在 needle 集里——同一缺陷类第五次
    复发。这里是**不动点**迭代（工作队列），不是再补几条模板：

    * 每个串的**编码形态**（原文 / percent-decoded / percent-encoded）入队；
    * 每个串在**每个**分隔符的**每个**出现位置上的头/尾截入队，片段继续切；
    * 编码输出的串**不再展开编码**（``quote`` 对 ``%`` 会 ``%25``→``%2525``
      无限自增）；编码输出不含裸分隔符（``: @ ? # & =`` 全被 ``quote`` 转义），
      故这一支不损失覆盖、也不破坏闭合性。

    终止性：入队串最多入 ``forms`` 一次，编码只对「非编码输出」展开，切分只会
    变短——整条链有限。结果按 ``secret`` 缓存（``_secret_forms_cached``）。

    结构定位若多吞了一段（畸形态里口令本身含 ``/``/``@``：候选 ``/:p/w``、
    ``u@u:a\nb``），真正的口令往往是**第一个** ``:``/``@`` 之后那截——闭包把它
    连同它的编码形态一起纳入（只多洗、不少洗）。

    r6 再补一版「剥掉空白」的：``urlsplit`` 解析前会做 WHATWG 归一化，把 URL 里的
    ``\t``/``\n``/``\r`` 直接删掉，于是底层异常文案里的口令是无空白版本——
    ``redis://user:0Cxya1B\tAxCaaC/x@host`` 的 ``Port could not be cast …`` 里是
    ``0Cxya1BAxCaaC``。少了这一版，``\t`` 后面的尾巴会以 ≥3 字符后缀活下来。
    """
    cached = _secret_forms_cached(secret)
    return cached


@functools.lru_cache(maxsize=1024)
def _secret_forms_cached(secret: str) -> tuple:
    """``_secret_forms`` 的**进程内缓存**（r8）。

    一条 URL 上 ``_mask_url`` / ``_passwords`` / ``_safe_reason`` 会对**同一个**
    跨度文本反复求形态集（一条 URL 上 5-10 次），闭包（含编码形态）每次 1ms 级。
    缓存把每形状的形态计算量压回一次。键是跨度文本本身，故缓存值不含任何全局状态。

    #165：闭包是 ``Θ(k²)`` 个片段（k = 分隔符出现次数）、片段又长 O(n)，敌对形状
    上是 ``Θ(n³)``。这里是**按字符计费**的预算（超预算抛
    :class:`_RedactionOverflow`，由 ``_mask_url`` / ``_passwords`` 整体
    fail-closed）：种子、每个派生形态、每个切片入队时都扣 ``len(value)``；单个
    候选另有 :data:`_MAX_CLOSURE_CHARS` 上限，整次清洗共享
    :data:`_closure_budget`（ContextVar）。

    **不设长度上限**：``test_masker_is_linear_on_long_input`` 钉住 70 000 字符的
    口令必须一趟掩完——那条口令只有 1 个分隔符，闭包本来就是 O(n)，按长度设限会
    误杀真正线性的输入。字符预算只惩罚**分隔符密度**（同样的长度，稀疏时 O(n)
    字符、稠密时 Θ(n³)）。

    预算内**逐字**等于 r8 的闭包——预算是上界，不是近似。预算不命中时抛异常、
    **绝不**返回一个「少洗了几条」的形态集：那才是静默漏点。

    预算**只在** :data:`_closure_budget` 非 ``None`` 时生效（掩码/口令路径）；
    直调 :func:`_secret_forms` 时 ``chars_left is None`` ⇒ 不设限，r8 的闭合性质
    契约（``test_secret_forms_closed_under_resplitting``）因此逐字不变。
    """
    if not secret:
        return ()
    forms: set = set()
    budget = _closure_budget.get()
    if budget is None:
        chars_left = None
    else:
        chars_left = min(_MAX_CLOSURE_CHARS, budget[0])
    #: 工作队列项 = ``(串, 是否还允许展开它的派生形态)``。**派生**=percent
    #: 编码/解码 + ``unicode_escape``；派生输出只再参与「按分隔符切」，不再派生
    #: （``quote`` 对 ``%`` 会 ``%25``→``%2525``、``unicode_escape`` 对 ``\`` 会
    #: 翻倍，都会无限自增）。``queued`` 去重：同一串只入队一次。
    work = [(secret, True)]
    queued = {secret}
    stripped = secret.replace("\t", "").replace("\n", "").replace("\r", "")
    if stripped != secret:
        work.append((stripped, True))
        queued.add(stripped)

    def _charge(length: int) -> None:
        """扣本候选与本次清洗的字符预算；超预算即具名溢出信号。

        计费点是**入队**（``_push``）与**切片产出**（``_delimiter_pieces`` 的每条
        片段）——两者都是真实的字符搬运，正是要界住的东西。
        """
        nonlocal chars_left
        if chars_left is None:
            return
        chars_left -= length
        budget[0] -= length
        if chars_left < 0 or budget[0] < 0:
            raise _RedactionOverflow(
                f"credential-form closure exceeds {_MAX_CLOSURE_CHARS} charged "
                f"chars for one candidate or the {_MAX_REDACTION_CHARS}-char "
                f"per-call budget (candidate is {len(secret)} chars)"
            )

    _charge(len(secret))
    if stripped != secret:
        _charge(len(stripped))

    def _push(value: str, expand: bool) -> None:
        if not value:
            return
        # 计费在**每一次**调用上（含去重命中的重复片段）：重复片段也要真实地
        # 切一次字符串，正是要界住的搬运量。
        _charge(len(value))
        if value not in queued:
            queued.add(value)
            work.append((value, expand))

    while work:
        cur, expand = work.pop()
        if not cur or cur in forms:
            continue
        forms.add(cur)
        if expand:
            decoded = _percent_decoded(cur)
            if decoded and decoded != cur:
                # 解码形态本身继续可派生：``%0a`` 解成真换行后，``int()`` 文案里
                # 出现的是它的转义写法 ``\n``（r8 实测的规则级残余）。
                _push(decoded, True)
            for variant in _quoted_forms(cur):
                if variant != cur:
                    _push(variant, False)
            try:
                escaped = cur.encode("unicode_escape").decode("ascii")
            except UnicodeError:  # pragma: no cover - unicode_escape 不抛
                escaped = cur
            # ``parts.port`` / ``int()`` 的报错文案是 ``%r``——非 ASCII 与
            # ``\t``/``\n`` 在里面是**转义写法**（``Super\xa0Secret`` →
            # ``'Super\\xa0Secret'``）。这一版同样要洗，否则 ``\xa0`` 之前的前缀
            # 会以 ≥3 字符前缀的形式活下来。
            # ``unicode_escape`` 对孤立代理字符走 backslashreplace（``\\udcff``），不抛。
            if escaped != cur:
                _push(escaped, False)
        for piece in _delimiter_pieces(cur):
            _push(piece, expand)
    forms.discard("")
    return tuple(sorted(forms, key=len, reverse=True))


def _protected_regions(text: str, spans: tuple = ()) -> tuple:
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
    if at == -1:
        # 口令里写了 ``/`` 的畸形态（``redis://user:pässwörd/h@host``）：首个 ``/``
        # 落在口令内部，上面那条判据取不到 ``@``。真正的主机分隔符是**最后一个
        # 凭据跨度的 stop**（r8 实测：漏保护 host 会让 `_secret_forms` 切出的
        # ``h`` 把主机名洗成 ``***ost``）。
        stops = [
            span.stop
            for span in spans
            if span.stop > auth_start and text[span.stop : span.stop + 1] == "@"
        ]
        if stops:
            at = max(stops)
    host_start = at + 1 if at != -1 else auth_start
    regions.append((host_start, _delimiter_stop(text, host_start, "/?#")))
    return tuple(regions)


def _literal_ranges(text: str, literals: tuple) -> tuple:
    """``literals`` 在 ``text`` 里的**全部**出现区间（各出现一次算一个区间）。

    r8：调用点已知的默认拒绝文本（非法 query 取值 / 未知名 / 裸 fragment）不再
    先做 ``str.replace`` 再掩码——那会把凭据**切碎**，掩码层随后认不出残余
    （``redis://user:xCb0B??AxB#xcC@host…`` 的 fragment 消息留下 ``??AxB``）。
    这些字面量改为进**同一趟**掩码（``_render_redacted`` 的 ``m`` 态），位置在
    **原文**上算，掩码与重洗因此对同一个文本一致。
    """
    ranges = []
    for literal in literals:
        # 子 3 字符的字面量不抹：满足不了完备性判据（≥3），误伤面却很大——未知名
        # ``u`` 会把 ``redis://user:…`` 洗成 ``redis://***ser:…``（r8 实测，WARNING
        # 面）。真正的凭据短口令仍由跨度掩码覆盖。
        if not literal or len(literal) < 3:
            continue
        start = text.find(literal)
        while start != -1:
            ranges.append((start, start + len(literal)))
            start = text.find(literal, start + 1)
    return tuple(ranges)


def _is_structural_form(form: str) -> bool:
    """形态**只由分隔符组成**（``@``、``@@``、``?#``）⇒ 是结构本身，不是凭据材料。

    ``_secret_forms`` 的闭包会产出这类片段（``@@@A`` 切出 ``@``/``@@``）——留在
    形态集里是闭包性质的要求，但渲染层拿它当 needle 会把结构性 ``@`` 抹掉。
    """
    return all(ch in _SECRET_SPLITTERS for ch in form)


def _render_redacted(
    text: str, spans: tuple, forms: tuple, extra_masks: tuple = ()
) -> str:
    """单趟渲染：保护区间原样、凭据候选段 ``***``、其余按 ``forms`` 重洗。

    ``spans`` 是 ``_credential_spans`` 的**全部**跨度，不是第一个——R1
    （``redis://user:pa55word://x@host``）与 R2
    （``redis://user:Pr0d2024x@host:6379/0?db=Acct:Secret1@x``）的每一条跨度都会
    被掩，所以「口令自己的 ``://`` 把扫描起点推到尾巴」与「只看第一个候选冒号」
    两个漏点在结构上没有立足点。``extra_masks`` 是调用点点名要抹的字面量区间
    （``_literal_ranges``），与跨度同处 ``m`` 态。三态优先序：掩码 > 保护 > 重洗。
    """
    n = len(text)
    #: r8：闭包把形态集推到上千条（``@ : / ? # %`` + 空白全在分隔符集里），
    #: 逐条对每个可重洗段做 ``in``/``replace`` 是 O(|forms| × |text|)。先在**整条
    #: 原文**上过一遍 C 层 ``in``，留下真的出现过的形态（通常 1-3 条），重洗段
    #: 只对这几条做替换——语义不变（不出现的形态替换是恒等）。
    present = tuple(form for form in forms if form in text)
    kind = bytearray(b"s" * n)  # s=可重洗 / p=结构性保护 / m=必掩
    # #165：区间标记用 bytearray 切片赋值（C 层 memcpy）而不是逐字节
    # ``for i in range(...)``。语义逐字相同（后写覆盖先写，次序不变），但把
    # 「O(条数) × O(长度)」的 Python 级循环降成 C 循环——重叠跨度密集时这是
    # 唯一还在的 Θ(n²) 项（实测 n=1000 的 ``"x://"*n`` 上占 _mask_url 的 24%）。
    for start, stop in _protected_regions(text, spans):
        stop = min(stop, n)
        if stop > start:
            kind[start:stop] = b"p" * (stop - start)
    for span in spans:
        start, stop = span.mask_from, min(span.stop, n)
        if stop > start:
            kind[start:stop] = b"m" * (stop - start)
    for start, stop in extra_masks:
        stop = min(stop, n)
        if stop > start:
            kind[start:stop] = b"m" * (stop - start)
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
            for form in present:
                # 纯分隔符形态是**结构本身**（``@@@A`` 的 ``@``/``@@``），不是
                # 凭据材料——拿它当 needle 会把结构抹掉（``redis://@@@A@host``
                # → ``redis://******host``）。形态集照旧包含它（闭包性质），
                # 只是渲染层不施加。
                if _is_structural_form(form):
                    continue
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

    #165：**单条跨度合规不蕴含总量合规**——一条 URL 上可以有 O(n) 条跨度，每条都
    在 :func:`_secret_forms_cached` 的预算内。两道闸门：
    :data:`_closure_budget`（ContextVar，一次清洗累计字符）与只看**单条跨度长度**
    的提前判据。提前判据**先于切片**做——否则「O(n) 条各含 O(n) 字符的跨度」光是
    切片搬运就是 Θ(n²)（``_delimiter_stop`` 与切片都在这个量级）。
    """
    forms = set()
    budget = _closure_budget.get()
    for span in spans:
        length = span.stop - span.mask_from
        if budget is not None:
            budget[0] -= length
            if budget[0] < 0:
                raise _RedactionOverflow(
                    f"redaction spans exceed the {_MAX_REDACTION_CHARS}-char "
                    f"per-call budget (span of {length} chars)"
                )
        pieces = [text[span.mask_from : span.stop]]
        cut = _delimiter_stop(text, span.mask_from, _AUTHORITY_DELIMITERS)
        if span.mask_from < cut < span.stop:
            pieces.append(text[span.mask_from:cut])
        for piece in pieces:
            if not piece:
                continue
            piece_forms = _secret_forms(piece)
            if span.default_deny:
                # default-deny 跨度不是「凭据形态」本身：它切出的子 3 字符片段
                # （``?u=…`` 的 1 字符名字）不参与重洗（见 ``_CredentialSpan``）。
                piece_forms = tuple(form for form in piece_forms if len(form) >= 3)
            forms.update(piece_forms)
    forms.discard("")
    return tuple(sorted(forms, key=len, reverse=True))


def _mask_url(value: object, *, literals: tuple = ()) -> str:
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
    * **默认拒绝层（r8）**：``literals=`` 是调用点**点名**要抹的字面量（非法
      query 取值 / 未知名 / 裸 fragment）。它们与跨度进**同一趟**渲染
      （``_literal_ranges`` → ``m`` 态），位置在**原文**上计算——r7 的
      「先 ``str.replace`` 再 ``_mask_url``」会把凭据切碎，掩码层随后认不出
      残余（``redis://user:xCb0B??AxB#xcC@host…`` 的 fragment 消息留下 ``??AxB``，
      ``?u=X:AA@BB@SuperSecretTail`` 的 WARNING 留下 ``?0xzzAC?…`` 型尾巴）。
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

    ``literals`` 是**关键字**参数（默认 ``()``），``_mask_url(x)`` 的单参调用
    逐字不变——``#158``/``#159`` 按名字 import 这个函数，签名只增不减。

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

    # plaita#165：**输入形状闸门**（O(1)，``str.count`` 走 C）。``_credential_spans``
    # 里两个 Θ(n²) 项按「候选冒号数 × 扫描长度」计费——``_no_at_colon_spans`` 的
    # 逐候选前向扫描与 ``_query_segment_is_provably_benign`` 的切片。乘积
    # ``len(text) * text.count(":")`` 是这两个量的上界，低于阈值即可放心进入枚举；
    # 超阈即 O(1) fail-closed 退化为 :data:`_REDACTED`（绝不回原文、绝不半掩）。
    #
    # **刻意不是「输入长度上限」**：长度不是代价的来源——70 000 字符、3 个冒号的
    # URL（``test_masker_is_linear_on_long_input``）乘积仅 2×10⁵，必须照常一趟掩完；
    # 而 80 KB 的 ``"x://"*20000`` 乘积 1.6×10⁹、实测 >30 s 不返回。
    # plaita#185：**闸门必须数全部 ``_SECRET_SPLITTERS``，不是只数 ``":"``**。
    # ``_delimiter_pieces`` 对**每一个**分隔符的**每一个**出现位置都要切两片
    # ⇒ 代价是 ``len(text) * Σ count(sep)``，其中 sep 遍历全部 13 个分隔符。
    # 只数 ``":"`` 时，``"x@"*32768`` 这类**零冒号**的输入乘积为 0 ⇒ 闸门形同虚设
    # ⇒ 直接落进 Θ(n²)（实测 n 翻倍耗时 ×3.6~4.3）。计入全部后此类输入
    # O(1) fail-closed 退化为 :data:`_REDACTED`，与 #165 的既定语义一致（绝不回原文）。
    _delim_work = sum(value.count(sep) for sep in _SECRET_SPLITTERS)
    if len(value) * _delim_work > _MAX_SPAN_SCAN_WORK:
        return _REDACTED

    #: #165 累计预算：整次清洗（含**每一条**跨度与其闭包）共享
    #: :data:`_MAX_REDACTION_CHARS` 个字符额度。``finally`` 复位 —— ContextVar
    #: 不会被泄漏给后续调用。
    budget_token = _closure_budget.set([_MAX_REDACTION_CHARS])
    try:
        spans = _credential_spans(value)
        extra_masks = _literal_ranges(value, literals)
        if not spans and not extra_masks:
            return value
        forms = _redaction_forms(value, spans)
    except _RedactionOverflow:
        # 形态闭包超预算（分隔符密集的敌对形状）：整体掩码。**不是**「掩掉跨度、
        # 跳过重洗」——重洗层正是「同一个口令换个写法又出现」的唯一防线，跳过它
        # 等于把 r8 的完备性悄悄关掉。
        return _REDACTED
    finally:
        _closure_budget.reset(budget_token)
    return _render_redacted(value, spans, forms, extra_masks)



# plaita#160/#161 重切：掩码语法的家留在本模块（50 条已落地锚在此），
# 但 foundation 的 url_redaction（node/server 双方共用的清洗器）需要它——
# server→core 向下注册，foundation 不得反向 import server。
from plaita.core.url_redaction import register_mask_url_impl as _register_url_redaction_mask  # noqa: E402
_register_url_redaction_mask(_mask_url)
def _passwords(text: str) -> tuple:
    """原始 URL 里出现的口令字面量（原文 + percent-decoded + encoded 形态）。

    与 ``_mask_url`` **同源同枚举**（r6）：两者都走 ``_credential_spans`` ——带
    ``@``/空格/裸 ``#``/``/``/``://`` 的口令、以及**没有 userinfo**、只写在 query
    值里的凭据（``?db=Pr0d:x@y``）都能被完整取出。r4 的 ``_passwords`` 依赖
    ``_locate_credentials``（没有 userinfo 时整层空转，r5 R2）；r5 的
    ``_global_credential_span`` 只取第一个候选冒号（同样取不到第二条跨度）。

    #165：与 ``_mask_url`` **同一套预算**（同一个 ``_redaction_forms``）。超预算时
    返回 ``(text,)``——**整条原文当唯一 needle**（把整段洗掉的保守解），而不是
    ``()``：``_mask_url`` 在同一输入上走 ``_REDACTED`` 常量分支，方向不变量
    （``_mask_url(s) != s`` ⟺ ``_passwords(s) != ()``，见
    ``TestEnumerateAllSpans::test_converse_holds_on_clean_and_malformed_shapes``）
    因此**在溢出分支上依然成立**。常量里没有原文的任何字节，所以反方向
    （``_mask_url(s)`` 不含任何返回形态）也成立。
    """
    if not isinstance(text, str):
        return ()
    budget_token = _closure_budget.set([_MAX_REDACTION_CHARS])
    try:
        return _redaction_forms(text, _credential_spans(text))
    except _RedactionOverflow:
        return (text,)
    finally:
        _closure_budget.reset(budget_token)


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


def _safe_reason(exc: BaseException, text: str, extra: tuple = ()) -> str:
    """底层异常的**安全**渲染（只用在嵌套 cause 的位置）。

    不假设 ``urlsplit``/``int``/``float`` 的报错文案里没有整条 URL 或口令：
    先抹掉口令的所有书写形态与 ``extra``（调用点已知的非法取值），再掩码裸 URL，
    然后才允许进入消息/traceback。
    """
    # r8：字面量与口令形态**一起**进**同一趟**掩码。先 ``str.replace`` 再掩码会把
    # 凭据切碎（``...float:'p@ssword'`` 被切成 ``...float:***@ssword'``），留下的
    # ≥3 后缀掩码层再也认不出来；同一趟里 ``m`` 态覆盖原文，不存在中间态。
    return _mask_url(str(exc), literals=tuple(extra) + _passwords(text))


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
            f"（URL={_mask_url(text, literals=literals)!r}）"
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
            _mask_url(text, literals=raw_unknown or tuple(unknown)),
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
                f"（URL={_mask_url(text, literals=literals)!r}）"
            ) from _sanitized_cause(exc, text, literals)
    elif path is None or path in ("", "/"):
        db = 0
    elif path.startswith("/") and path[1:].isascii() and path[1:].isdigit():
        db = int(path[1:])
    else:
        literals = tuple(_encoding_forms(path))
        raise RedisUrlError(
            f"Redis URL 的路径必须是数据库编号（形如 /0）: {_REDACTED!r}"
            f"（URL={_mask_url(text, literals=literals)!r}）"
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
            f"{_mask_url(text, literals=(raw_fragment, raw_fragment[1:]))!r}"
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
