RH Protected Geo Worker Template - Production Version

文件结构：
_worker.js
index.html      备用跳转页
landing.html    真实落地页
robots.txt

部署方式：
1. 直接把本 ZIP 上传到 Cloudflare Pages。
2. ZIP 根目录必须直接包含 _worker.js、index.html、landing.html、robots.txt。
3. 不要让这些文件再套一层文件夹。

正式版规则：
- 手机端 + 目标国家：返回真实 landing.html
- 电脑端：跳 FALLBACK_URL
- 非目标国家：跳 FALLBACK_URL
- 直接访问 /landing.html、/landing、/landing/：跳 FALLBACK_URL
- 常见爬虫 / 自动化工具：跳 FALLBACK_URL
- 已移除调试入口

后续换落地页：
1. 把新的真实落地页命名为 landing.html。
2. 替换本包里的 landing.html。
3. 重新打 ZIP 上传 Cloudflare Pages。

后续改国家或开关：
打开 _worker.js 顶部 CONFIG 修改：
TARGET_COUNTRIES: ['US']       // 美国
TARGET_COUNTRIES: ['SG']       // 新加坡
TARGET_COUNTRIES: ['US','SG']  // 美国+新加坡

MOBILE_ONLY: true              // 只允许移动端
MOBILE_ONLY: false             // 电脑也允许

STRICT_ASN_FILTER: false       // 宽松，减少误伤
STRICT_ASN_FILTER: true        // 严格，拦截更多机房/VPN/代理

FALLBACK_URL: 'https://www.facebook.com/'
