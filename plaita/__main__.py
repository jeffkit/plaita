"""``python -m plaita`` — CLI 入口。

裸调用（无子命令）打印版本号退出（历史安装验证行为）；
``python -m plaita build <源码.py>`` 把 @flow 源码编译成 JSON 产物，
详见 :mod:`plaita.cli`。
"""

from plaita.cli import main

raise SystemExit(main())
