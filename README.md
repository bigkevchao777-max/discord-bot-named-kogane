# discord-bot-named-kogane

## Deploy on Render (Background Worker)

This repo is configured for Render using `render.yaml`.

### 1) Push this repo to GitHub

```bash
git add .
git commit -m "Add bot code and Render deploy config"
git push origin main
```

### 2) Deploy on Render

1. Open Render dashboard.
2. New -> Blueprint.
3. Select this GitHub repo.
4. Render will read `render.yaml` and create a Worker service.

### 3) Set required environment variables

In Render service settings, add:

- `DISCORD_TOKEN` = your bot token
- `DISCORD_GUILD_ID` = your Discord server ID (optional, but used by this bot for fast slash sync)

### 4) Verify logs

Look for:

- `Logged in as ...`
- slash command sync output

If deployment fails, confirm `requirements.txt` installed successfully.
