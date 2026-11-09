---
name: aether-issue-pr
description: Develop and review changes in ycx-git/Aether, including PRs, CI failures and protected-file changes. Issues are optional in this personal fork.
---

# Fork collaboration

## Scope

- Default repository: `ycx-git/Aether`; default base: `dev`. Specify the repository explicitly, for example `gh pr ... --repo ycx-git/Aether` or `gh api repos/ycx-git/Aether/...`. Honor an explicitly selected repository instead; do not publish to upstream merely because inherited files mention it.
- Follow the user's authorized scope for edits, commits, pushes and PRs. A read-only request does not authorize writing. Reuse an existing task PR rather than opening duplicates.
- Issues are optional. Link a real issue when supplied or requested; otherwise describe the work directly. Do not invent an issue or create one to satisfy an inherited rule.
- Keep development branches after merging unless the user explicitly requests cleanup.

## Review and submit

1. Read applicable instructions and inspect status, remotes, branch tracking, staged changes and existing commits. Preserve unrelated user work.
2. Fetch the selected base and inspect the complete `origin/dev...HEAD` diff, including earlier commits, helpers and tests. Check merge compatibility with the actual base, not an assumed `main` branch.
3. Verify affected behavior, permissions, cancellation and recovery paths as appropriate. Run meaningful checks and distinguish code failures from environment failures. Do not disable tests or weaken assertions to make CI green.
4. Review staged code and documentation before committing. Use the configured models and the pre-push checks. A failed non-fast-forward push requires inspecting the divergence, not forcing it.
5. Before creating a PR, check its repository, head, base and existing PRs. Use the existing template, mark inapplicable issue fields, and report the concrete behavior change, validation and material limits concisely. Use a body file for multiline text.
6. Record the PR URL and head SHA. Inspect the actual check runs; an accepted request or a running check is not a completed success.

## CI governance

Inspect the failing run and its event base/head SHAs. For `cicd-guard`, read the workflow and protected-path/maintainer lists from that run's base revision. A path-policy failure differs from a test failure; a review approval does not itself override a path guard.

Do not modify guard lists, skip checks, hide protected changes or claim success merely to bypass a failure. A user-authorized fork-governance change must be explicit, reviewed and reported as such. Keep pending checks and remaining uncertainty visible.
