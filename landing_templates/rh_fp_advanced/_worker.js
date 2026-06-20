/**
 * Edge Guard for Cloudflare Pages - Production Version
 *
 * 结构：
 * _worker.js
 * index.html      备用页 / fallback
 * landing.html    真实落地页
 * robots.txt
 *
 * 只需要改顶部 CONFIG：
 * - TARGET_COUNTRIES：允许进入真实落地页的国家/地区，例如 ['US'] / ['SG'] / ['US', 'SG']。
 * - REAL_HTML_FILE：真实落地页文件名，默认 landing.html。
 * - FALLBACK_URL：不符合规则时跳转的白页/备用页。
 * - STRICT_ASN_FILTER：开启后会拦截常见云服务器、机房、VPN、代理网络。
 * - MOBILE_ONLY：开启后只允许手机端访问，电脑端跳转备用页。
 *
 * 生产版说明：
 * - 已移除调试入口。
 * - 保留 Cloudflare Pages 308 clean-url 兼容。
 * - 直接访问 /landing.html、/landing、/landing/ 都会被拦截。
 */
const CONFIG = {
  // 允许国家/地区，ISO 两位国家代码，全部大写。
  // 美国：['US']；新加坡：['SG']；美国+新加坡：['US', 'SG']。
  TARGET_COUNTRIES: ['US'],

  // 真实落地页文件名。以后复用时建议统一叫 landing.html。
  REAL_HTML_FILE: 'landing.html',

  // 未通过门禁、直接访问真实页、未知路径时跳转。
  FALLBACK_URL: 'https://www.facebook.com/',
  FALLBACK_STATUS: 302,

  // true：更严格，拦截常见数据中心 / 云服务器 / VPN / 代理。
  // 如果目标国家真实用户误伤，先改成 false。
  STRICT_ASN_FILTER: false,

  // true：只允许手机端访问；电脑端/桌面浏览器会被跳转到 FALLBACK_URL。
  MOBILE_ONLY: true,

  // true：iPad / Android 平板也当作移动端放行；false：只放行手机。
  ALLOW_TABLET: true,

  // Cloudflare 付费 Bot Management 有 bot score 时才生效；没有该字段会自动忽略。
  MIN_BOT_SCORE: 25,

  // ASN 组织名关键词。只有 STRICT_ASN_FILTER=true 才会启用。
  BLOCKED_AS_ORG_KEYWORDS: [
    'amazon', 'aws', 'google cloud', 'google llc', 'microsoft', 'azure',
    'digitalocean', 'ovh', 'hetzner', 'linode', 'akamai', 'vultr',
    'oracle', 'alibaba', 'tencent', 'huawei', 'choopa', 'm247', 'datacamp',
    'leaseweb', 'colo', 'hosting', 'host', 'datacenter', 'data center',
    'server', 'cloud', 'vpn', 'proxy', 'proxies', 'relay', 'anonymous'
  ],

  // 明显爬虫/自动化工具 UA。
  UA_BLOCKLIST: [
    'curl', 'wget', 'python', 'requests', 'scrapy', 'httpclient', 'go-http-client',
    'java/', 'okhttp', 'phantom', 'selenium', 'playwright', 'puppeteer',
    'headlesschrome', 'headless', 'censys', 'shodan', 'masscan', 'nikto', 'sqlmap',
    'ahrefs', 'semrush', 'mj12', 'dotbot', 'petalbot', 'bytespider', 'claudebot',
    'baiduspider', 'yandex', 'bingbot', 'googlebot', 'facebookexternalhit',
    'facebookcatalog', 'twitterbot', 'linkedinbot', 'slackbot', 'discordbot'
  ],

  // 开启后，首页必须像正常浏览器导航请求。
  REQUIRE_BROWSER_NAVIGATION: true,

  // 是否允许静态资源直接访问。当前 landing 是单 HTML，通常用不到；保持 true 方便后续加 assets。
  ALLOW_STATIC_ASSETS: true
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = normalizePath(url.pathname);

    if (path === '/robots.txt') return robotsResponse();

    // 真实页路径即使被猜到，也不允许直接访问。
    if (isDirectRealPagePath(path)) return fallbackResponse();

    const isHome = path === '/' || path === '/index.html';
    const isAsset = CONFIG.ALLOW_STATIC_ASSETS && isStaticAsset(path);

    // 未知路径统一回退。
    if (!isHome && !isAsset) return fallbackResponse();

    const gate = passGate(request, isAsset);
    if (!gate.ok) return fallbackResponse();

    if (isHome) {
      const result = await fetchRealHtmlAsset(request, env);
      if (!result.response || result.response.status < 200 || result.response.status >= 300) {
        return fallbackResponse();
      }
      return wrapResponse(result.response, false);
    }

    const response = await env.ASSETS.fetch(request);
    return wrapResponse(response, true);
  }
};

function passGate(request, isAsset) {
  const method = request.method.toUpperCase();
  if (method !== 'GET' && method !== 'HEAD') return deny('bad-method');

  const cf = request.cf || {};
  const country = String(cf.country || request.headers.get('CF-IPCountry') || '').toUpperCase();
  const allowCountries = CONFIG.TARGET_COUNTRIES.map(c => String(c).toUpperCase());
  if (!allowCountries.includes(country)) return deny('country-blocked');

  const uaRaw = request.headers.get('user-agent') || '';
  const ua = uaRaw.toLowerCase();
  if (!ua) return deny('empty-ua');
  if (CONFIG.UA_BLOCKLIST.some(word => ua.includes(word))) return deny('ua-blocked');

  if (!isAsset && CONFIG.MOBILE_ONLY && !isMobileClient(request, ua)) {
    return deny('desktop-blocked');
  }

  if (CONFIG.STRICT_ASN_FILTER) {
    const asOrg = String(cf.asOrganization || '').toLowerCase();
    if (asOrg && CONFIG.BLOCKED_AS_ORG_KEYWORDS.some(word => asOrg.includes(word))) {
      return deny('asn-org-blocked');
    }
  }

  const bm = cf.botManagement;
  if (bm && bm.score !== undefined && !bm.verifiedBot && bm.score < CONFIG.MIN_BOT_SCORE) {
    return deny('bot-score-low');
  }

  if (!isAsset && CONFIG.REQUIRE_BROWSER_NAVIGATION) {
    const accept = (request.headers.get('accept') || '').toLowerCase();
    const secMode = (request.headers.get('sec-fetch-mode') || '').toLowerCase();
    const secDest = (request.headers.get('sec-fetch-dest') || '').toLowerCase();

    if (accept && !accept.includes('text/html') && !accept.includes('*/*')) return deny('bad-accept');
    if (secMode && secMode !== 'navigate') return deny('bad-fetch-mode');
    if (secDest && secDest !== 'document') return deny('bad-fetch-dest');
  }

  return { ok: true, reason: 'ok' };
}

function deny(reason) {
  return { ok: false, reason };
}

async function fetchRealHtmlAsset(request, env) {
  const paths = realHtmlCandidatePaths();

  for (const path of paths) {
    const result = await fetchAssetPath(request, env, path);

    if (result.response && result.response.status >= 200 && result.response.status < 300) {
      return result;
    }

    // 兼容 Cloudflare Pages 的 308 clean URL：/landing.html -> /landing
    if (result.locationPath) {
      const follow = await fetchAssetPath(request, env, result.locationPath);
      if (follow.response && follow.response.status >= 200 && follow.response.status < 300) {
        return follow;
      }
    }
  }

  return { response: null, path: '' };
}

async function fetchAssetPath(request, env, path) {
  const assetUrl = new URL(request.url);
  assetUrl.pathname = path;
  assetUrl.search = '';

  const headers = new Headers();
  headers.set('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8');
  const ua = request.headers.get('user-agent');
  if (ua) headers.set('User-Agent', ua);

  let response = null;
  let location = '';
  let locationPath = '';

  try {
    const assetRequest = new Request(assetUrl.toString(), {
      method: 'GET',
      headers,
      redirect: 'manual'
    });
    response = await env.ASSETS.fetch(assetRequest);
    location = response.headers.get('location') || '';
    if (location) {
      try {
        const nextUrl = new URL(location, assetUrl);
        locationPath = normalizePath(nextUrl.pathname);
      } catch (_) {}
    }
  } catch (_) {}

  return { response, path, location, locationPath };
}

function realHtmlCandidatePaths() {
  const raw = CONFIG.REAL_HTML_FILE.replace(/^\/+/, '');
  const lower = raw.toLowerCase();
  const withSlash = '/' + raw;
  const paths = [];

  // Pages 常会把 /landing.html 308 到 /landing，所以 clean path 优先。
  if (lower.endsWith('.html')) {
    paths.push('/' + raw.slice(0, -5));
  }
  paths.push(withSlash);

  return [...new Set(paths.map(p => p.replace(/\/+/g, '/')))];
}

function isDirectRealPagePath(path) {
  const raw = CONFIG.REAL_HTML_FILE.replace(/^\/+/, '').toLowerCase();
  const real = '/' + raw;
  const variants = new Set([real]);
  if (raw.endsWith('.html')) {
    const clean = '/' + raw.slice(0, -5);
    variants.add(clean);
    variants.add(clean + '/');
  }
  return variants.has(path);
}

function isMobileClient(request, ua) {
  const chMobile = (request.headers.get('sec-ch-ua-mobile') || '').trim();
  if (chMobile === '?1') return true;

  const isPhone = /(iphone|ipod|android.*mobile|windows phone|blackberry|bb10|mobile safari|opera mini|opera mobi|iemobile)/i.test(ua);
  if (isPhone) return true;

  if (CONFIG.ALLOW_TABLET) {
    const isTablet = /(ipad|tablet|android(?!.*mobile)|kindle|silk|playbook)/i.test(ua);
    if (isTablet) return true;
    if (/macintosh/i.test(ua) && /mobile\/\w+\s+safari/i.test(ua)) return true;
  }

  return false;
}

function isStaticAsset(path) {
  return /^\/assets\//i.test(path) || /\.(png|jpg|jpeg|webp|svg|ico|css|js|woff|woff2|gif|avif|map)$/i.test(path);
}

function normalizePath(path) {
  try { path = decodeURIComponent(path); } catch (_) {}
  path = path.replace(/\/+/g, '/');
  return path.toLowerCase();
}

function fallbackResponse() {
  return new Response(null, {
    status: CONFIG.FALLBACK_STATUS,
    headers: secureHeaders({
      'Location': CONFIG.FALLBACK_URL,
      'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0'
    })
  });
}

function robotsResponse() {
  return new Response('User-agent: *\nDisallow: /\n', {
    status: 200,
    headers: secureHeaders({
      'Content-Type': 'text/plain; charset=utf-8',
      'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0'
    })
  });
}

function wrapResponse(response, isAsset) {
  const headers = new Headers(response.headers);
  for (const [key, value] of secureHeaders({})) headers.set(key, value);
  headers.set('Cache-Control', isAsset ? 'public, max-age=600' : 'no-store, no-cache, must-revalidate, max-age=0');
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}

function secureHeaders(headers) {
  const h = new Headers(headers);
  h.set('X-Robots-Tag', 'noindex, nofollow, noarchive');
  h.set('Referrer-Policy', 'no-referrer');
  h.set('X-Content-Type-Options', 'nosniff');
  h.set('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  return h;
}
