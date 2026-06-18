Mira 本地 Token

安装：
1. 打开 chrome://extensions
2. 开启“开发者模式”
3. 选择“加载已解压的扩展程序”，加载本文件夹
4. 在 Mira > Token 管理 > 本地 Token 生成一次性绑定码
5. 插件填写 Mira 地址和绑定码，点击“绑定插件”
6. 复制插件内的“插件回调地址”，加入 Meta App 的有效 OAuth 跳转 URI
7. 填写 Meta App ID，点击“授权并自动上报”

说明：
- 插件不会读取 Facebook Cookie 或隐藏浏览器会话。
- Token 来源必须是用户点击“授权并自动上报”后的官方 OAuth 返回，或用户手动填写的备用 Token。
- 到期前插件会尽量自动续取；如果 Facebook 要求重新确认，需要用户再点一次授权。
