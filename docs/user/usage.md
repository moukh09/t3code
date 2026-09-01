# Review usage

The Usage page combines Codex, Claude Code, Grok Build, and GitHub Copilot activity from your
connected environments. It reads the providers' local session history and shows API-equivalent
token cost, processed tokens, cache savings, provider shares, and model breakdowns. Subscription
billing is separate from the raw token cost shown here.

Grok Build totals come from persisted session updates. Interactive turns that never wrote a
completed-turn record will not appear.

GitHub Copilot CLI sessions report exact per-model totals when they shut down. Sessions driven
through Copilot's ACP server do not currently write that summary, so T3 Code uses their per-turn
checkpoints instead. Those checkpoints include input, cache-read, and cache-write tokens, but not
output or reasoning tokens. ACP usage appears after each completed turn.

Use **Past 24h** for an hourly chart covering the exact rolling 24-hour period. The **7 days**,
**30 days**, and **90 days** ranges use daily resolution. Cost and token toggles update both the
headline and chart, and refreshing rescans every connected environment.
