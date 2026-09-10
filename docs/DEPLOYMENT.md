# Automated deployment (GitHub Actions)

`.github/workflows/deploy.yml` replaces the manual "SSH in, `git pull`, rebuild, restart" routine. It:

- **Triggers only on a push to `master`** — no other branch, no pull requests.
- **Never runs automatically.** Every trigger sits in "Waiting for review" until an authorized reviewer approves it from the Actions tab, via a GitHub Environment protection rule.
- Runs on a **GitHub-hosted runner** and reaches EC2 over SSH — nothing needs to be installed or kept running on the EC2 box itself, unlike a self-hosted runner.
- Over that SSH connection: `git pull origin master`, then `docker compose build` + `docker compose up -d` for all four services (`shopify-pipeline`, `events-summary-worker`, `variant-wise-events-worker`, `checkout-login-status-worker`), then one immediate one-off run of `checkout-login-status-worker`'s job (its own schedule is once a day, so a deploy wouldn't otherwise take effect until the next 00:05 IST tick).

This depends on two one-time setup steps that this workflow file alone can't do — an SSH key pair and repo secrets, and the approval gate — both done through the GitHub UI and the EC2 shell, once, up front.

## 1. Create a dedicated SSH deploy key and add the repo secrets

Use a key generated specifically for this, not your personal SSH key — it's the credential GitHub Actions will hold.

1. On your own machine (not EC2): `ssh-keygen -t ed25519 -C "github-actions-deploy" -f deploy_key -N ""` — produces `deploy_key` (private) and `deploy_key.pub` (public).
2. Copy the public key onto EC2's authorized keys, for the same user that owns the existing pipeline checkout (e.g. `ubuntu`):
   ```bash
   ssh-copy-id -i deploy_key.pub ubuntu@<your-ec2-host>
   # or manually: append deploy_key.pub's contents to ~/.ssh/authorized_keys on the EC2 box
   ```
3. In the repo: **Settings → Secrets and variables → Actions → Secrets → New repository secret**, add:
   - `EC2_HOST` — the EC2 instance's public IP or DNS name.
   - `EC2_USER` — the SSH user (e.g. `ubuntu`).
   - `EC2_SSH_KEY` — the full contents of the **private** key file (`deploy_key`), including the `-----BEGIN ... -----END-----` lines.
   - `EC2_SSH_PORT` — optional, only if SSH runs on a non-default port; the workflow falls back to `22`.
4. Delete the local `deploy_key`/`deploy_key.pub` files once they're safely in EC2's `authorized_keys` and the GitHub secret — don't leave the private key sitting on disk anywhere else.

**Security note**: this requires EC2's security group to allow inbound SSH (port 22, or whatever `EC2_SSH_PORT` is) from GitHub-hosted runners' IP ranges, which are broad and change over time — in practice this usually means opening port 22 to `0.0.0.0/0` and relying on key-based auth (no password auth) for security, rather than IP-allowlisting a fixed range. If that's not acceptable for this instance, the alternative is a self-hosted runner installed directly on EC2 (no inbound port needed) — ask if you want to switch back to that.

## 2. Add the required-reviewer approval gate

1. **Settings → Environments → New environment**, name it `production` (must match `environment: production` in `deploy.yml`).
2. Under **Deployment protection rules**, enable **Required reviewers** and add yourself (and anyone else who should be able to approve a deploy).
3. Leave **Deployment branches** restricted to `master` too, as a second independent guard beyond the workflow's own `on.push.branches` filter.

With this in place, every push to `master` creates a workflow run that pauses before the `deploy` job starts; an authorized reviewer approves it from the run's page in the Actions tab, and only then does the SSH/deploy step execute.

## 3. Set the deploy path (if it's not the default)

The workflow assumes the existing pipeline checkout lives at `~/pipelines/aws-pipeline-ec2` on the EC2 host. If it's somewhere else, set a repository variable instead of editing the workflow:

**Settings → Secrets and variables → Actions → Variables → New repository variable**, name `DEPLOY_PATH`, value the absolute path (e.g. `/home/ubuntu/pipelines/aws-pipeline-ec2`).

## Manual fallback

The old manual steps still work as a fallback if SSH access or the approval gate is ever unavailable:

```bash
cd ~/pipelines/aws-pipeline-ec2
git pull origin master
docker compose build shopify-pipeline events-summary-worker variant-wise-events-worker checkout-login-status-worker
docker compose up -d shopify-pipeline events-summary-worker variant-wise-events-worker checkout-login-status-worker
docker compose exec -T checkout-login-status-worker \
  python -c "from workers.checkout_login_status_worker import run_worker; run_worker()"
```
