"""POC B: renderer-split byte-equivalence proof.

Candidate two-pass split of ``slack_fuse.mrkdwn.convert`` into a pure
``convert_structural`` pass (chunk-time) and a ``resolve_mentions`` pass
(read-time), per the server-split RFC's late-mention-resolution design.

Not production code. Lives only on the POC branch to validate the design.
"""

from __future__ import annotations
