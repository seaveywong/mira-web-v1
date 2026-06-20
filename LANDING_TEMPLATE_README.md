# Landing Page Template Specification

This document defines the neutral landing page package format.

Public package rules:

- Do not include backend domains, server IPs, internal API paths, or platform branding.
- Do not include Worker files. Runtime Worker files are generated during publishing.
- Put page markup in `landing.html`.
- Keep final user actions routed through `LP_TARGET_URL`.
- Keep Pixel initialization controlled by `LP_PIXEL_ID`.

Required variables:

```html
<script>
  var LP_PIXEL_ID = "";
  var LP_TARGET_URL = "";
</script>
```

CTA rule:

```html
<a data-lp-cta href="#">Continue</a>
```

Recommended click binding:

```js
document.querySelectorAll("[data-lp-cta]").forEach(function (el) {
  el.href = LP_TARGET_URL || "#";
  el.addEventListener("click", function (event) {
    if (!LP_TARGET_URL) {
      event.preventDefault();
      return;
    }
  });
});
```

Pixel rule:

```js
if (window.fbq && LP_PIXEL_ID) {
  fbq("track", "Contact");
}
```

Validation checklist:

- `landing.html` exists.
- `LP_PIXEL_ID` and `LP_TARGET_URL` exist exactly once.
- Final CTA/buttons/forms use `LP_TARGET_URL`.
- No hard-coded final redirect links are used for the main action.
- No public HTML/JS contains backend domains, server IPs, or internal API paths.
