Mira 本地执行器 v1.5.0

═══════════════════════════════════════
  全自动 Token 提取 + 内置工具集
═══════════════════════════════════════

安装：
1. chrome://extensions → 开发者模式 → 加载已解压的扩展程序 → 选择本文件夹
2. 在 Chrome 中打开 facebook.com 并登录
3. 插件自动静默提取 Token — 无需任何手动操作

4 个标签页：
  📊 状态 — Token 有效期/用户/权限/来源 + Mira 连接心跳
  📤 像素分享 — BM列表 → 选择像素 → 输入合作伙伴BM → 一键分享
  👥 BM邀请 — 选择BM → 输入邮箱+角色 → 一键邀请
  ⚙️ 配置 — Mira 绑定码/手动Token（均可留空）

Mira 配合：
  1. 在⚙️配置中输入绑定码 → 点击"激活执行器"
  2. 插件每30秒心跳上报 Token/权限/账户摘要
  3. Mira 自动判断：在线 + Token有效 + ads_management + 账户匹配 → 优先使用

全自动运行：
  • 启动后自动静默提取 Token（无 FB 标签页时自动打开 adsmanager）
  • 每 ~55 分钟静默刷新
  • Token 变化时立刻心跳上报 Mira

文件清单：
  mira-local-api-executor/
  ├── manifest.json
  ├── background.js          Service Worker (全自动核心)
  ├── content-script.js      注入 Facebook 提取 Token
  ├── popup.html             状态仪表盘 + 像素分享 + BM邀请 + 配置
  ├── popup.js               仪表盘逻辑
  └── README.txt
