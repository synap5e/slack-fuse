# k8s admin scripts

Operator-side helpers for running one-off admin jobs against the cluster
slack-fuse-server. Not Flux-reconciled — invoked directly via `kubectl apply`.

## `backfill-job.sh <channel-id> [flags]`

Runs one `slack-fuse-server backfill <channel-id>` Job. Defaults to
`--source legacy-cache`, which mounts an NFS share of the operator's
`~/.cache/slack-fuse/messages/` into the pod at
`/app/.cache/slack-fuse/messages/` (the path
`LegacyCacheBackfiller` expects). NFS is ~70× faster than slack-api for
backfilling — a 20k-message channel drains in ~3min vs ~2-3hr.

### One-time NFS setup on the operator PC

```
# /etc/exports
/home/$USER/.cache/slack-fuse/messages 10.0.0.6(ro,sync,no_subtree_check,insecure,all_squash,anonuid=10001,anongid=0)

systemctl enable --now nfs-server.service
exportfs -ra

# UFW (or equivalent firewall)
ufw allow proto tcp from 10.0.0.6 to any port 2049 comment "slack-fuse legacy-cache NFS"
ufw allow proto tcp from 10.0.0.6 to any port 111  comment "slack-fuse NFS rpcbind"
ufw allow proto udp from 10.0.0.6 to any port 111  comment "slack-fuse NFS rpcbind"
ufw reload
```

`10.0.0.6` is the k8s-homelab node IP — replace with your own. NFS export
is scoped to that single IP so other LAN hosts can't read the cache.

### Override the NFS source

```
SLACK_FUSE_LEGACY_NFS_SERVER=10.0.100.1 \
SLACK_FUSE_LEGACY_NFS_PATH=/srv/slack-fuse-cache \
scripts/k8s/backfill-job.sh C123ABC
```

### Examples

```
scripts/k8s/backfill-job.sh C077358GZLM                 # default: legacy-cache via NFS
scripts/k8s/backfill-job.sh C077358GZLM --watch         # also stream logs
scripts/k8s/backfill-job.sh C077358GZLM --source slack-api
scripts/k8s/backfill-job.sh C077358GZLM --allow-large
```

### Bulk

There's no built-in iterator — submit one Job per channel from a wrapper:

```
for ch in $(ls ~/.cache/slack-fuse/messages/); do
  scripts/k8s/backfill-job.sh "$ch"
  sleep 30
done
```

Job names are unique per submission (timestamped) so concurrent submissions
don't collide. Each Job auto-cleans after 24h
(`ttlSecondsAfterFinished: 86400`).

### Pinning the image

`backfill-job.sh` hardcodes the slack-fuse-server image tag+digest at the
top. Override via:

```
SLACK_FUSE_IMAGE=ghcr.io/synap5e/slack-fuse:sha-NEW@sha256:NEWDIGEST scripts/k8s/backfill-job.sh ...
```

Keep it in sync with what's actually deployed in
`synap5f/k8s-homelab/apps/apps/slack-fuse/deployment.yaml` — backfill and
the running server share the same `events` schema, so the two images must
agree on it.
