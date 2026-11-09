# Background agent tasks

The Aether agent engine exposes four coordinator tools:

- `task_start`: starts owned background sessions and returns their IDs without waiting for model completion. Reusing a request ID with identical work returns the existing jobs; changed work is rejected.
- `task_status`: reads job metadata and live status.
- `task_collect`: reads completed results independently; acknowledge only the exact task ID and event sequence already incorporated.
- `task_cancel`: cancels selected owned tasks and their workers, including pending retry timers.

Only a root coordinator can start background tasks. Its workers may use ordinary `task` delegation subject to existing permissions. Model and permission routing are shared with the ordinary task tool. Research policy, team size and model selection remain project configuration.

## Results and coordination

Results are persisted in `.aether/async-tasks/<owner-session-id>.json`. After a terminal state is committed, `task.completed` is published on the normal event bus/SSE stream. It carries owner, child, task and event IDs, status and sequence; prompts and result bodies are excluded. Events can repeat. Consumers deduplicate by event ID and use the ledger to recover missed events.

A controller or plugin may notify an idle coordinator of available results and let it refill work. A busy coordinator must not receive a duplicate normal prompt. The event itself does not start a model or enforce an exploration count. This patch does not replace the native Codex engine's own agent tools or provide a desktop task dashboard.

## Stop and restart

Normal coordinator completion leaves background tasks running. Explicit Stop pauses its task ledger, cancels delegated descendants, clears retries and preserves independent user forks. Stop waits briefly for tracked runs to settle and reports an error if shutdown cannot be confirmed. Results remain available. A ledger can be explicitly rearmed with its current pause token; rearming does not restart old jobs. After a process restart, unconfirmed jobs are marked interrupted rather than blindly resubmitted.

Read metadata with `GET /session/:sessionID/async_tasks`. Rearm with `POST /session/:sessionID/async_tasks/resume` and `{ "expected_pause_token": "..." }`. Run isolated checks from `packages/opencode` with `bun test test/session/async-task.test.ts`.
