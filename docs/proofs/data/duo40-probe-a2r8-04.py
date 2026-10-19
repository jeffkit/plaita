"""duo40 / A2R8-04 探针：300 行 × 128 KiB、max_bytes=1024 的 tracemalloc 峰值。

用法：``PN_SRC=<src 路径> python duo40-probe-a2r8-04.py``。
pre-fix 树（663dbd0）与修复后树的输出对比见 ``duo40-probe-a2r8-04.txt``。
"""
import os
import sys
import tempfile
import tracemalloc

sys.path.insert(0, os.environ["PN_SRC"])

import sqlalchemy  # noqa: E402

import plaita_nodes.database as db  # noqa: E402
from plaita_nodes.database import SqlQueryNode, SqlResultLimitError  # noqa: E402

path = os.path.join(tempfile.mkdtemp(prefix="duo40-sql-"), "rows.db")
url = f"sqlite:///{path}"
engine = sqlalchemy.create_engine(url)
payload = "x" * (128 * 1024)
with engine.begin() as conn:
    conn.execute(sqlalchemy.text("CREATE TABLE t (id INTEGER, payload TEXT)"))
    conn.execute(
        sqlalchemy.text("INSERT INTO t VALUES (:i, :p)"),
        [{"i": i, "p": payload} for i in range(300)],
    )
engine.dispose()


class Exec:
    def get_global_variable(self, key, default=None):
        return default

    def evaluate(self, value):
        return value


db.get_credential = lambda name: {"url": url}
node = SqlQueryNode(id="q", credential="cred", query="SELECT id, payload FROM t",
                    max_bytes=1024)

tracemalloc.start()
try:
    node.execute(Exec())
    print("NO ERROR")
except SqlResultLimitError as exc:
    print("limit error: observed_rows=%s observed_bytes=%s"
          % (exc.observed_rows, exc.observed_bytes))
cur, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
print("peak=%d bytes = %.3f MiB = %.1fx budget"
      % (peak, peak / 1024 / 1024, peak / 1024))
