Landing Page Template Guide

This folder is a content template. The publishing system injects tracking,
redirect rotation, access control, and edge runtime files during publish.

Required files:
- landing.html: the real landing page.
- index.html: fallback entry. It can redirect to Facebook or show a neutral fallback.
- robots.txt: optional crawler instruction.

Required variables in landing.html:
- LP_PIXEL_ID: filled during publish.
- LP_TARGET_URL: filled during publish.

Rules:
- Do not hard-code pixel IDs.
- Do not hard-code final chat or redirect links.
- Do not include server-side code, worker files, API keys, tokens, or platform-specific publish files.
- Do not add analytics beacons that expose the publishing platform.

To create a new design:
1. Keep landing.html as the entry page.
2. Keep LP_PIXEL_ID and LP_TARGET_URL variable names.
3. Connect all CTA buttons/forms to LP_TARGET_URL.
4. Zip the template files with landing.html at the root.
5. Upload the zip in the template manager and fix any validation errors before publishing.
