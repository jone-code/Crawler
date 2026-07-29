# Proxy Pool Procurement Guide

This document defines a practical procurement checklist for proxy pools used by this crawler project (Xiaohongshu and Douyin scenarios).

## 1) Procurement Goal

Build a stable proxy supply for anti-bot resilience with:

- high request success rate
- low ban/challenge ratio
- measurable and auditable vendor quality

## 2) Preferred Proxy Types

Priority order:

1. Rotating residential proxies (primary)
2. Mobile proxies (4G/5G) for hard anti-bot targets
3. Datacenter proxies as supplemental capacity only

## 3) Mandatory Technical Requirements

Vendors must provide:

- Protocols: HTTP/HTTPS (SOCKS5 is preferred)
- Authentication: username/password and/or IP allowlist
- Rotation modes:
  - per-request rotation
  - sticky session (for example 5/10/30 minutes)
- Geo targeting: at least country-level (city/ASN preferred)
- Programmatic management API
- Real-time usage/health visibility:
  - success rate
  - latency
  - error classes (403/429/timeouts)

## 4) SLA and Quality Baselines (Suggested)

Use these as default negotiation targets:

- Availability: >= 99.5%
- Target-site success rate: >= 90% under acceptance test workload
- P95 latency: <= 2500 ms
- Support response window: <= 30 minutes during vendor service hours

## 5) Commercial Terms (Suggested)

- Trial first: 3-7 days or 5-20 GB test quota
- Prefer pay-as-you-go or low commitment plans
- Written clauses for:
  - overage price
  - billing granularity
  - refund policy
  - invoice capability

## 6) Acceptance Test Procedure

Before large purchase:

1. Run at least 1,000 requests per candidate vendor using this project.
2. Execute across multiple time windows (not one short burst only).
3. Record and compare:
   - success rate
   - 403 ratio
   - 429 ratio
   - challenge/captcha ratio
   - average latency and P95 latency
4. Keep at least 2 qualified vendors as primary + backup.

## 7) Recommended Vendor Websites (for PoC)

Common global providers:

- Bright Data: https://brightdata.com
- Oxylabs: https://oxylabs.io
- Decodo (Smartproxy): https://decodo.com
- SOAX: https://soax.com
- NetNut: https://netnut.io
- IPRoyal: https://iproyal.com
- DataImpulse: https://dataimpulse.com
- Scrapeless: https://www.scrapeless.com/en

Chinese/localized sites for easier communication:

- Bright Data CN: https://www.bright.cn
- Decodo CN: https://decodo.cn
- IPRoyal CN page: https://www.iproyal.cc/zh-hans/cn
- IPWO: https://www.ipwo.net
- IPWeb: https://ipweb.cc

## 8) Integration Format for This Project

The current project expects proxy entries in the proxy pool as:

- `proxy_name`: custom identifier
- `proxy_url`: for example `http://user:pass@host:port`
- `priority`: lower number = higher priority
- `enabled`: true/false

In Admin Web:

1. Add proxy via the "Proxy Pool" form
2. Enable `use_proxy_pool` when triggering a crawl
3. Optionally combine with `use_account_pool` for account + proxy joint rotation

## 9) Compliance Notes

- Use proxies only for lawful access and compliant data collection.
- Ensure vendor sourcing and usage model satisfy your legal/compliance requirements.
- Review each target platform's policy and local regulations before production use.
