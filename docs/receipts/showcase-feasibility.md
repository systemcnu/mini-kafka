# Showcase feasibility check (SL7-pre, run at SL0 exit per SLICES SD-7)

**Date: 2026-07-28. Verdict: PASS — the conditional showcase remains feasible. Kill criterion re-armed at SL7 (account-creation moment).**

## Facts, from primary sources

- **Free instance hours:** "Render grants 750 Free instance hours to each workspace per calendar month"; exhaustion → services suspended until month start. A 24/7 service uses ~720 h — the showcase fits even if it never sleeps, and it will sleep. [render.com/docs/free]
- **Spin-down:** after "15 minutes without receiving any inbound traffic"; wakes on "an HTTP request or new WebSocket connection. This process takes about one minute", and Render itself shows a loading page during startup — on top of our own GitHub Pages shim (DD-23). [render.com/docs/free]
- **Disk:** "an ephemeral filesystem" — changes "lost every time the service redeploys, restarts, or spins down"; persistent disks not available on free. Matches DD-23's restart-fresh assumption; no size cap stated → verify live allowance before enabling the feeder (SHOW-4 threshold stays configurable). [render.com/docs/free]
- **Restarts:** "Render might restart a Free web service at any time." Accepted — the showcase is stateless-by-design.
- **Payment method:** Render's docs do not demand a card for free web services; their own 2026 article and most third-party sources state free deploys need no payment information; one third-party source disputes it. **Not fully settled from docs alone — settled definitively at SL7's first real step: if account creation asks for a card, SHOW-2's kill criterion fires and SL7 ships the documented "later" (R4).**

## Sources

- https://render.com/docs/free (fetched 2026-07-28)
- https://render.com/articles/platforms-with-a-real-free-tier-for-developers-in-2026
- https://dashdashhard.com/posts/ultimate-guide-to-renders-free-tier/ (third-party, concurring)
- https://www.srvrlss.io/provider/render/ (third-party, dissenting on card requirement)

## What this means for the plan

SL7 proceeds as planned when its turn comes: step 1 is account creation under Sri's control (no card = continue; card demanded = stop, document "later"). Nothing about SL0–SL6 changes either way.

## Slice-exit record

- **Re-verified 2026-07-31 (SL7 D-SL7-9 step 0, pre-build): no delta.** render.com/docs/free re-read; all five receipt facts unchanged (750 free instance hours/workspace/month with suspension on exhaustion · spin-down after 15 min without inbound traffic, wake on HTTP request/WebSocket taking about one minute behind Render's own loading page · ephemeral filesystem, changes lost on redeploy/restart/spin-down · Render may restart a Free web service at any time). No card requirement documented for free web services; the page mentions a payment method only as an option for overages ("If you haven't added a payment method, Render instead suspends all of your Free services"). Kill criterion stays armed at the account-creation moment; Phase A build proceeds.
