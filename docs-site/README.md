# plaita 文档站

基于 MkDocs Material 的 plaita 中文文档站，API 参考由 mkdocstrings 从源码自动生成。

## 本地预览

```bash
# 1. 安装文档构建依赖
pip install -r docs-site/requirements.txt

# 2. （可选）安装主项目，使 mkdocstrings 能内省 plaita 包
pip install -e .
# 或至少保证仓库根目录在 sys.path 上（从根目录运行 mkdocs 即可）

# 3. 启动本地预览（从仓库根目录运行，保证 plaita 可被内省）
mkdocs serve -f docs-site/mkdocs.yml
# 浏览器打开 http://127.0.0.1:8000
```

## 构建静态站点

```bash
mkdocs build -f docs-site/mkdocs.yml
# 产物在 docs-site/site/，可部署到任意静态托管
```

构建时清理旧产物：

```bash
mkdocs build -f docs-site/mkdocs.yml --clean
```

## 目录结构

```
docs-site/
├── mkdocs.yml          # 站点配置（主题/导航/插件）
├── requirements.txt    # 文档构建依赖
├── .gitignore
└── docs/
    ├── index.md        # 首页
    ├── assets/         # SVG 架构图（复用自 docs/zh/images）
    ├── guide/          # 使用指南（8 页）
    ├── architecture/   # 架构（5 页）
    ├── nodes/          # 节点系统（4 页）
    ├── distributed/    # 断点续执（5 页）
    ├── scenarios/      # 应用场景（5 页）
    ├── api/            # API 参考（mkdocstrings ::: 指令）
    ├── reference/      # 配置/CLI/迁移
    └── about/          # 更新日志/许可证
```

## 写作约定

- **API 参考**：每个 `api/*.md` 只放 `::: module.path` 指令，由 mkdocstrings 从 docstring + Pydantic 字段自动渲染。改 API 页内容 → 改源码 docstring。
- **图表**：架构图用 `docs-site/docs/assets/*.svg`（复用 `docs/zh/images`）；流程图用 mermaid 代码块（`mkdocs-mermaid2-plugin` 渲染）。
- **代码块**：启用 `content.code.copy`，代码块带复制按钮。
- ** admonition**：用 `!!! note` / `!!! tip` / `!!! warning` 突出提示。
- **内容与代码同步**：修改 `plaita/` 源码后，按 `docs/DOC_CODE_MAP.md`（若存在）检查是否需同步文档；API 页由 mkdocstrings 自动跟随源码。

## 部署

通过 GitHub Actions 自动部署到 GitHub Pages，配置见 `.github/workflows/docs.yml`：push 到 `main` 时构建并发布。

## 技术栈

- [MkDocs](https://www.mkdocs.org/) —— markdown 站点生成器
- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) —— 主题
- [mkdocstrings](https://mkdocstrings.github.io/) —— Python API 自动生成
- [mkdocs-mermaid2-plugin](https://github.com/pugong/mkdocs-mermaid2-plugin) —— mermaid 图渲染
