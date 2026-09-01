# GitHub Copilot

T3 Code connects to GitHub Copilot CLI through its Agent Client Protocol server.

## Set up

1. Install GitHub Copilot CLI on the machine running the T3 Code server.
2. Run `copilot login` in a terminal and complete authentication.
3. Open **Settings → Providers**, enable **GitHub Copilot**, and confirm the binary path is `copilot`.

T3 Code discovers the models and reasoning levels advertised by the installed CLI. You can set a
full path to the executable or add launch arguments when the CLI is installed outside `PATH`.
Commands advertised by Copilot's ACP server also appear in the composer slash menu.

Copilot CLI sessions appear on the Usage page with per-model token totals. Regular CLI sessions
report exact totals when they exit. Sessions driven through Copilot's ACP server appear after each
completed turn using their input and cache checkpoints; Copilot does not currently persist output
or reasoning totals for those sessions.

## Windows and WSL

The Windows desktop app uses the Copilot CLI installed on Windows when its server runs natively.
If the desktop app hosts its server inside WSL, install Copilot CLI separately inside that WSL
distribution.

## Current limitations

GitHub Copilot's ACP server is in preview. Some CLI releases do not send permission requests even
when approval-required mode is selected. T3 Code sends the requested permission mode to Copilot,
but the CLI controls whether an approval prompt appears.
