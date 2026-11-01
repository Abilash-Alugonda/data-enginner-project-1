# Launch Checklist

> Current Status: NOT LAUNCH READY (Pre-Launch Verification Gate)
> Config Domain Placeholder: `DOMAIN=example.com`

The project must remain in non-launched status until all manual infrastructure, legal, and operational gates below are checked.

## Launch Gate Items

- [ ] Custom domain connected and HTTPS working (`DOMAIN=example.com` updated to production domain)
- [ ] SSL/TLS certificate issued and HTTP to HTTPS automatic redirection validated
- [ ] Favicon verified on the live domain across modern browsers (SVG, PNG, and ICO formats)
- [ ] Legal pages (Privacy Policy and Terms and Conditions) reviewed and approved by qualified legal counsel
- [ ] No generator tags or AI attribution marks present in HTML headers, meta tags, or footers
- [ ] All automated unit, integration, and equivalence tests passing in production deployment environment
- [ ] All REST API endpoints and web dashboard routes verified against live production database
- [ ] Production backup and disaster recovery snapshot schedule established for DuckDB warehouse and Parquet partitions
- [ ] Rate limiting and security boundary checks enabled on edge reverse proxy (e.g. Nginx, Cloudflare)

