# Slack progress-card evidence (2026-09-10)

Screenshots supporting four openclaw/openclaw pull requests from this fork:
the progress-line id fix, the command-detail fix, the relay proxy fix and the
Slack reasoning-cards feature.

Each directory holds one run against a real Slack workspace, captured from the
Slack web client and cropped to a single thread reply with the plan block
expanded. `final2`, `rollover2` and `first-run` are synthetic-driver runs
(scripted agent events through the real Slack dispatcher and a real
`@slack/web-api` client); `real-model` is a gateway run from source with a
live model over Socket Mode. See the `MANIFEST.md` in each directory for the
scenario behind each image.

No workspace, channel, bot or user identifiers appear in the images. The
captions inside some images name the commit the run was built from. This
branch carries no source code and is not meant to be merged.
