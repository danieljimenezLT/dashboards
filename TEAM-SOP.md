# SWEAT440 Dashboards — Team SOP

## Staging Workflow

**Purpose**
- `staging` branch — where work-in-progress changes get committed and previewed before they go live.
- `main` branch — production. Only updated via merge from `staging`, never edited directly.

**Day-to-day loop**
1. `git checkout staging && git pull`
2. Make your edits, commit, push to `staging`.
3. Test at https://dev-leadteam.github.io/dashboards/staging/
4. Once approved, open a Pull Request on GitHub from `staging` → `main` (GitHub shows a "Compare & pull request" button automatically after the push).
5. Review the diff, click "Merge pull request".
6. `main` auto-deploys to https://dev-leadteam.github.io/dashboards/

> **Never commit directly to `main`** — always go through `staging` + a PR.
