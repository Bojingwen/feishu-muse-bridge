# feishu-muse-bridge

[简体中文](README.md) | English | [日本語](README.ja.md)

Bring **Muse himself (Meta's personal AI assistant)** into Feishu (Lark): @ the bot in a group chat, or just DM it, and you are talking to your own Muse. There is **no third-party LLM** anywhere in the chain — every reply is written by Muse himself, with his memory and tools.

> This repository doubles as a Muse skill package: give the repo URL to your Muse and he can deploy his own bridge by following `SKILL.md`.

## Architecture

```
Feishu group / DM --(@bot in group; DM direct)--> feishubridge daemon
  (Go, Feishu long-connection WebSocket — NO public IP / callback URL needed)
  --> bridge-data/inbox/<message_id>.json   (+ a "Muse is thinking…"
                                             placeholder posted in the chat)
  --> a Muse hook polls the inbox every 10s and wakes Muse himself
  --> Muse reads the message + history, writes the reply (and any
      attachments) to bridge-data/outbox/
  --> the daemon replaces the placeholder with the full reply and
      sends the attachments (image / video / file)
```

Replies take tens of seconds (poll + thinking time) and arrive as one whole message (the placeholder is edited into the answer), not token-by-token streaming.

## Features

- **Zero third-party models**: the chain is only Feishu, the local bridge, and Muse himself
- **No public endpoint**: Feishu long-connection mode — no domain, public IP, or tunneling needed
- **Groups answer on @ only, DMs answer directly**; a plain-text `@botname` (with no mentions array) is recognised too
- **Attachments out**: images, video, and files; video is remuxed to faststart with a first-frame cover (without it, some Feishu clients show the video as unplayable)
- **Anti-loop**: the bot never triggers on its own messages
- **Persisted placeholders**: after a daemon restart, a "thinking…" placeholder is still correctly replaced by the reply
- **Self-healing watchdog**: the wake-up hook checks the bridge's whole supervision chain on every 10s poll — a systemd unit wiped by a machine replacement is reinstalled, a stopped service is started, a dead-inside connection is restarted, and the user gets a one-line report after a repair

## Feishu-side setup

Before the bridge can work, set up the app on the Feishu Open Platform. Every step is required:

1. **Create the app**: sign in to the [Feishu Open Platform](https://open.feishu.cn), go to Developer Console → Create enterprise self-built app, and create it with a name and icon.
2. **Enable the bot capability**: under "Add app capabilities", enable "Bot".
3. **Get the credentials**: under "Credentials & basic info" you will find the **App ID** and **App Secret**. At deploy time these go into the bridge's `config/bridge.json` (stored locally only — never send them to anyone, and never commit them to any repository).
4. **Grant permissions** (add each one under "Permission management"):
   - `im:message` — send and receive messages (the core permission; without it nothing arrives)
   - `im:message.p2p_msg:readonly` — receive private-chat messages (without it you get "group works, DM doesn't")
   - `im:resource` — send images, video, and files
5. **Configure event subscription** (on the "Event subscription" page):
   - Receiving mode: **"Use long connection to receive events"** (this is what removes the need for a public IP or callback URL)
   - Add the event: **Receive message `im.message.receive_v1`**
6. **Publish a version**: under "Version management & release", create a new version and publish it. **Permission and event changes do not take effect until published** — this is the most common cause of "the bot doesn't respond", and every later config change needs a new release.
7. **Add the bot to your group**: in the target group's Settings → Group bots, add this bot. In groups, only messages **@-ing the bot** are answered; in DMs just send a message.

## Local deployment

Once the Feishu side is ready, give this repository's URL to your Muse and let him follow [`SKILL.md`](SKILL.md) step by step (build the bridge, write the config, install the wake-up hook, install the systemd service). All you need to provide is the App ID / App Secret, plus one test message at the end.

## Known limits

Feishu long connections are push-only: **events that occur while the client is offline are not queued and never redelivered**. That is Feishu's rule and the bridge cannot change it — what the watchdog does is compress recovery to seconds, so most failures heal before you ever notice them.

## Security note

This repository contains **no credentials, no chat history, and no personal data**: the config template holds placeholders only. At deploy time, write your own App Secret only into the local `config/bridge.json` (permissions 600), and never commit it to any repository.

## License

This project is open-sourced under the **GNU General Public License v3.0 (GPL-3.0)**: you may freely use, modify, and distribute it, but any distribution (including modified versions) must likewise be open-sourced under GPL-3.0 and keep the copyright notice. See [LICENSE](LICENSE) for the full text.

Copyright (C) 2026 Bojingwen
