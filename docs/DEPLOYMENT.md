# Automated deployment (GitHub Actions)

`.github/workflows/deploy.yml` replaces the manual "SSH in, `git pull`, rebuild, restart" routine. It:

- **Triggers only on a push to `master`** — no other branch, no pull requests.
- **Never runs automatically.** Every trigger sits in "Waiting for review" until an authorized reviewer approves it from the Actions tab, via a GitHub Environment protection rule.
- Runs `git pull origin master`, then `docker compose build` + `docker compose up -d` for all four services (`shopify-pipeline`, `events-summary-worker`, `variant-wise-events-worker`, `checkout-login-status-worker`) directly on the EC2 host.
- Additionally fires one immediate one-off run of the `checkout-login-status-worker`'s job (`docker compose exec ... run_worker()`), since that worker's own schedule is once a day (00:05 IST) — without this, a change to it would silently wait up to 24h to actually take effect. This runs as a second process inside the just-restarted container, alongside (not instead of) its normal long-running scheduler.

This depends on two one-time setup steps in the repo/EC2 host that this workflow file alone can't do — a workflow file can declare *that* it needs an approval gate and a self-hosted runner, but the gate's reviewers and the runner's registration are account/instance-level setup done through the GitHub UI and the EC2 shell.

## 1. Register the EC2 instance as a self-hosted runner

The workflow runs the deploy commands *on* the EC2 box itself (no SSH key needed, nothing exposed to the internet) — but that requires GitHub's Actions runner agent to be installed and running there first.

1. In the repo: **Settings → Actions → Runners → New self-hosted runner**, choose Linux, and copy the commands GitHub shows you (they embed a one-time registration token — always use the ones from the UI, not a copy-pasted example).
2. SSH into the EC2 instance and run those commands under a directory of your choosing, e.g. `~/actions-runner`.
3. Install it as a persistent service rather than running it in a terminal you'll close:
   ```bash
   sudo ./svc.sh install
   sudo ./svc.sh start
   ```
4. Confirm it shows up as **Idle** under Settings → Actions → Runners before relying on it.

The runner needs permission to run `git`/`docker compose` in the deploy directory — run it as the same user (e.g. `ubuntu`) that already owns the existing pipeline checkout, not root.

## 2. Add the required-reviewer approval gate

1. **Settings → Environments → New environment**, name it `production` (must match `environment: production` in `deploy.yml`).
2. Under **Deployment protection rules**, enable **Required reviewers** and add yourself (and anyone else who should be able to approve a deploy).
3. Leave **Deployment branches** restricted to `master` too, as a second independent guard beyond the workflow's own `on.push.branches` filter.

With this in place, every push to `master` creates a workflow run that pauses before the `deploy` job starts; an authorized reviewer approves it from the run's page in the Actions tab, and only then do the pull/rebuild/restart steps execute.

## 3. Set the deploy path (if it's not the default)

The workflow assumes the existing pipeline checkout lives at `~/pipelines/aws-pipeline-ec2` on the runner. If it's somewhere else, set a repository variable instead of editing the workflow:

**Settings → Secrets and variables → Actions → Variables → New repository variable**, name `DEPLOY_PATH`, value the absolute path (e.g. `/home/ubuntu/pipelines/aws-pipeline-ec2`).

## Manual fallback

The old manual steps still work as a fallback if the runner or approval gate is ever unavailable:

```bash
cd ~/pipelines/aws-pipeline-ec2
git pull origin master
docker compose build shopify-pipeline events-summary-worker variant-wise-events-worker checkout-login-status-worker
docker compose up -d shopify-pipeline events-summary-worker variant-wise-events-worker checkout-login-status-worker
docker compose exec -T checkout-login-status-worker \
  python -c "from workers.checkout_login_status_worker import run_worker; run_worker()"
```
