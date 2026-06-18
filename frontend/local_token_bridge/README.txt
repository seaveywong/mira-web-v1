Mira 本地执行器

安装：
1. 打开 chrome://extensions
2. 开启“开发者模式”
3. 选择“加载已解压的扩展程序”，加载本文件夹
4. 在 Mira > Token 管理 > 本地执行器 生成一次性绑定码
5. 插件填写 Mira 地址和绑定码，点击“激活浏览器”
6. 保持 Chrome 已登录 Facebook，插件会自动接收 Mira 下发的本地浏览器任务

说明：
- 插件不会读取 Facebook Cookie 或隐藏浏览器会话。
- Mira 只下发任务参数；本地 Chrome 负责打开页面、检测状态并回传结果。
- 手动备用 Token 是可选项，只有用户明确填写时才会上报 Mira。
