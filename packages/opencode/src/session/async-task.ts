import z from "zod"
import { Bus } from "../bus"
import { BusEvent } from "../bus/bus-event"
import { Log } from "../util/log"
import path from "path"
import fs from "fs/promises"
import { randomUUID, createHash } from "crypto"
import { Instance } from "../project/instance"
import { Lock } from "../util/lock"
import { Session } from "."
import { SessionID, MessageID } from "./schema"
import { SessionStatus } from "./status"
import { MessageV2 } from "./message-v2"
import type { Tool } from "../tool/tool"
import { NotFoundError } from "../storage/db"

export type Spec = {
  description: string
  prompt: string
  subagent_type: string
  category: "exploration" | "support"
  direction: string
}

export type Job = Spec & {
  id: string
  owner_session_id: string
  child_session_id: string
  message_id: string
  request_key: string
  request_hash: string
  status: "starting" | "running" | "retrying" | "completed" | "failed" | "cancelled" | "interrupted" | "uncertain"
  model: { providerID: string; modelID: string } | null
  variant: string | null
  boot: string
  claimed?: boolean
  event_seq: number
  acknowledged: boolean
  created_at: number
  updated_at: number
  result?: string
  error?: string
}

export type State = {
  schema_version: 1
  owner_session_id: string
  revision: number
  event_cursor: number
  paused: boolean
  pause_token: string | null
  jobs: Job[]
}

const boot = randomUUID()
const terminal = new Set(["completed", "failed", "cancelled", "interrupted", "uncertain"])
const watching = new Set<string>()

export class Store {
  readonly file: string
  constructor(
    readonly directory: string,
    readonly owner: string,
  ) {
    if (!/^ses_[A-Za-z0-9_-]+$/.test(owner)) throw new Error("Invalid async task owner")
    this.file = path.join(directory, ".aether", "async-tasks", owner + ".json")
  }

  async read(): Promise<State> {
    const raw = await fs.readFile(this.file, "utf8").catch((err: NodeJS.ErrnoException) => {
      if (err.code === "ENOENT") return undefined
      throw err
    })
    if (raw === undefined)
      return {
        schema_version: 1,
        owner_session_id: this.owner,
        revision: 0,
        event_cursor: 0,
        paused: false,
        pause_token: null,
        jobs: [],
      }
    const state = JSON.parse(raw) as State
    if (state.schema_version !== 1 || state.owner_session_id !== this.owner || !Array.isArray(state.jobs))
      throw new Error("Async task ledger identity is invalid")
    return state
  }

  async update<T>(fn: (state: State) => T): Promise<T> {
    using _ = await Lock.write(this.file)
    const state = await this.read()
    const result = fn(state)
    state.revision++
    await fs.mkdir(path.dirname(this.file), { recursive: true })
    const temp = this.file + "." + randomUUID() + ".tmp"
    try {
      const handle = await fs.open(temp, "wx")
      try {
        await handle.writeFile(JSON.stringify(state, null, 2) + "\n", "utf8")
        await handle.sync()
      } finally {
        await handle.close()
      }
      for (let attempt = 0; ; attempt++) {
        try {
          await fs.rename(temp, this.file)
          break
        } catch (err) {
          if (
            !(err instanceof Error) ||
            !["EPERM", "EACCES", "EBUSY"].includes((err as NodeJS.ErrnoException).code ?? "") ||
            attempt >= 10
          )
            throw err
          await new Promise((resolve) => setTimeout(resolve, 50))
        }
      }
    } finally {
      await fs.unlink(temp).catch(() => {})
    }
    return structuredClone(result)
  }

  async plan(specs: Spec[], key: string) {
    const hash = createHash("sha256").update(JSON.stringify(specs)).digest("hex")
    return this.update((state) => {
      if (state.paused) throw new Error("Async tasks are paused; explicit controller resume is required")
      const existing = state.jobs.filter((job) => job.request_key === key)
      if (existing.length) {
        if (existing.some((job) => job.request_hash !== hash))
          throw new Error("Async request id was reused with different tasks")
        return existing
      }
      const jobs = specs.map((spec) => ({
        ...spec,
        id: "job_" + randomUUID(),
        owner_session_id: this.owner,
        child_session_id: SessionID.descending(),
        message_id: MessageID.ascending(),
        request_key: key,
        request_hash: hash,
        status: "starting" as const,
        model: null,
        variant: null,
        boot,
        event_seq: 0,
        acknowledged: false,
        created_at: Date.now(),
        updated_at: Date.now(),
      }))
      state.jobs.push(...jobs)
      return jobs
    })
  }

  async complete(
    id: string,
    status: "completed" | "failed" | "interrupted" | "uncertain",
    payload: { result?: string; error?: string },
  ) {
    return this.update((state) => {
      const job = state.jobs.find((job) => job.id === id)
      if (!job) throw new Error("Async task is not owned by this session")
      if (terminal.has(job.status)) return job
      Object.assign(job, payload, {
        status,
        updated_at: Date.now(),
        event_seq: ++state.event_cursor,
        acknowledged: false,
      })
      return job
    })
  }

  async ack(items: { id: string; event_seq: number }[]) {
    return this.update((state) => {
      for (const item of items) {
        const job = state.jobs.find((job) => job.id === item.id)
        if (!job || job.event_seq !== item.event_seq || !terminal.has(job.status))
          throw new Error("Async result acknowledgement does not match an owned terminal event")
        job.acknowledged = true
      }
      return state
    })
  }

  async pause() {
    return this.update((state) => {
      state.paused = true
      state.pause_token = randomUUID()
      for (const job of state.jobs) {
        if (terminal.has(job.status)) continue
        job.status = "cancelled"
        job.updated_at = Date.now()
        job.event_seq = ++state.event_cursor
        job.acknowledged = false
      }
      return state
    })
  }

  async resume(token: string | null) {
    return this.update((state) => {
      if (state.pause_token !== token) throw new Error("Async pause changed before resume")
      state.paused = false
      state.pause_token = null
      return state
    })
  }
}

export namespace AsyncTasks {
  const log = Log.create({ service: "async.tasks" })
  const runs = new Map<string, { owner: string; work: Promise<void> }>()
  export const Event = {
    Completed: BusEvent.define(
      "task.completed",
      z.object({
        sessionID: SessionID.zod,
        childSessionID: SessionID.zod,
        taskID: z.string(),
        eventID: z.string(),
        sequence: z.number().int().min(1),
        status: z.enum(["completed", "failed", "cancelled", "interrupted", "uncertain"]),
      }),
    ),
  }

  // The ledger is authoritative. Notifications contain references only, may be
  // delivered more than once, and are emitted after durable state is committed.
  async function notify(job: Job) {
    if (!terminal.has(job.status)) return
    await Bus.publish(Event.Completed, {
      sessionID: SessionID.make(job.owner_session_id),
      childSessionID: SessionID.make(job.child_session_id),
      taskID: job.id,
      eventID: `${job.id}:${job.event_seq}`,
      sequence: job.event_seq,
      status: job.status as "completed" | "failed" | "cancelled" | "interrupted" | "uncertain",
    }).catch((error) => log.warn("completion notification failed; result retained in ledger", { error }))
  }

  async function complete(
    owner: string,
    id: string,
    state: "completed" | "failed" | "interrupted" | "uncertain",
    payload: { result?: string; error?: string },
  ) {
    const job = await store(owner).complete(id, state, payload)
    await notify(job)
    return job
  }
  export const Metadata = z
    .object({
      schema_version: z.literal(1),
      owner_session_id: SessionID.zod,
      root_session_id: SessionID.zod,
      revision: z.number().int().nonnegative(),
      event_cursor: z.number().int().nonnegative(),
      paused: z.boolean(),
      pause_token: z.string().nullable(),
      needs_attention: z.boolean(),
      jobs: z.array(
        z
          .object({
            id: z.string(),
            owner_session_id: SessionID.zod,
            child_session_id: SessionID.zod,
            description: z.string(),
            subagent_type: z.string(),
            category: z.enum(["exploration", "support"]),
            direction: z.string(),
            status: z.enum([
              "starting",
              "running",
              "retrying",
              "completed",
              "failed",
              "cancelled",
              "interrupted",
              "uncertain",
              "cancelling",
            ]),
            model: z.object({ providerID: z.string(), modelID: z.string() }).nullable(),
            variant: z.string().nullable(),
            event_seq: z.number().int().nonnegative(),
            acknowledged: z.boolean(),
            child_exists: z.boolean(),
          })
          .passthrough(),
      ),
    })
    .meta({ ref: "AsyncTaskMetadata" })

  const store = (owner: string) => new Store(Instance.directory, owner)

  async function child(id: string) {
    return Session.get(SessionID.make(id)).catch((error) => {
      if (!NotFoundError.isInstance(error)) throw error
      return undefined
    })
  }

  export async function claim(owner: string, id: string, fn: () => AbortSignal | undefined) {
    const held: { signal?: AbortSignal } = {}
    await store(owner).update((state) => {
      const job = state.jobs.find((job) => job.id === id)
      if (state.paused || !job || terminal.has(job.status))
        throw new Error("Async task was cancelled before its busy claim")
      // This callback only claims in-memory session ownership. No permission
      // prompt or model request is awaited while holding the ledger lock.
      held.signal = fn()
      return true
    })
    return held.signal
  }

  async function observe(owner: string, id: string) {
    const ledger = store(owner)
    const job = (await ledger.read()).jobs.find((job) => job.id === id)
    if (!job || terminal.has(job.status)) return true
    const session = await child(job.child_session_id)
    if (!session) {
      if (job.boot !== boot)
        await complete(owner, id, "interrupted", {
          error: "Backend restarted before child dispatch was confirmed; not resubmitted",
        })
      return job.boot !== boot
    }
    const status = await SessionStatus.get(session.id)
    if (status.type !== "idle") return false
    const messages = await Session.messages({ sessionID: session.id, limit: 40 })
    const message = messages
      .filter((message) => message.info.role === "assistant" && message.info.parentID === job.message_id)
      .toSorted((a, b) => b.info.time.created - a.info.time.created)[0]
    if (
      message?.info.role === "assistant" &&
      message.info.time.completed &&
      message.info.finish === "stop" &&
      !message.info.error
    ) {
      const result = message.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text)
        .join("\n")
      await complete(owner, id, "completed", { result })
      return true
    }
    if (message?.info.role === "assistant" && message.info.time.completed && message.info.error) {
      await complete(owner, id, "failed", {
        error: message.info.error.name,
        result: message.parts
          .filter((part) => part.type === "text")
          .map((part) => part.text)
          .join("\n"),
      })
      return true
    }
    if (job.boot !== boot) {
      await complete(owner, id, "interrupted", {
        error: "Backend restarted without a confirmed terminal response; not resubmitted",
      })
      return true
    }
    return false
  }

  function watch(owner: string, id: string) {
    const key = owner + "/" + id
    if (watching.has(key)) return
    watching.add(key)
    const poll = Instance.bind(async () => {
      try {
        if (await observe(owner, id)) {
          watching.delete(key)
          return
        }
      } catch {
        watching.delete(key)
        return
      }
      const timer = setTimeout(poll, 1000)
      timer.unref()
    })
    void poll()
  }

  export async function start(specs: Spec[], ctx: Tool.Context, request?: string) {
    if (ctx.abort.aborted) throw new Error("Task dispatch was cancelled")
    const owner = await Session.get(ctx.sessionID)
    if (owner.parentID)
      throw new Error(
        "Only the main coordinator may start asynchronous tasks; subagents use task for their bounded workers",
      )
    const ledger = store(ctx.sessionID)
    for (const job of (await ledger.read()).jobs) {
      const live = await SessionStatus.get(SessionID.make(job.child_session_id))
      if (live.type === "retry" || (terminal.has(job.status) && live.type !== "idle"))
        throw new Error("Cancel the waiting or unexpectedly active owned task before launching replacements: " + job.id)
    }
    const key = request ?? ctx.messageID + "/" + ctx.callID
    const jobs = await ledger.plan(specs, key)
    await Promise.all(
      jobs.map(async (job) => {
        // Only this process's new, unclaimed record can be dispatched. Replayed
        // tool calls return existing identities instead of sending a second prompt.
        const claimed = await ledger.update((state) => {
          const current = state.jobs.find((item) => item.id === job.id)!
          if (state.paused || current.status !== "starting" || current.boot !== boot || current.claimed) return false
          current.claimed = true
          current.updated_at = Date.now()
          return true
        })
        if (!claimed) {
          watch(ctx.sessionID, job.id)
          return
        }
        try {
          const { prepare } = await import("../tool/task")
          const prepared = await prepare(job, ctx, SessionID.make(job.child_session_id))
          const { SessionPrompt } = await import("./prompt")
          await ledger.update((state) => {
            const current = state.jobs.find((item) => item.id === job.id)!
            if (state.paused || current.status === "cancelled" || ctx.abort.aborted)
              throw new Error("Async dispatch cancelled during preparation")
            current.model = prepared.model
            current.variant = prepared.agent.variant ?? null
          })
          // noReply persists the predetermined message exactly once. It does not
          // start a model. Background execution has its own lifetime, not the
          // parent turn's AbortSignal (normal parent finish also aborts that signal).
          await SessionPrompt.prompt({
            sessionID: prepared.session.id,
            messageID: MessageID.make(job.message_id),
            model: prepared.model,
            agent: prepared.agent.name,
            variant: prepared.agent.variant,
            noReply: true,
            parts: [{ type: "text", text: job.prompt, metadata: { async_task_id: job.id } }],
          })
          const current = await ledger.read()
          if (
            current.paused ||
            current.jobs.find((item) => item.id === job.id)?.status === "cancelled" ||
            ctx.abort.aborted
          ) {
            await SessionPrompt.cancel(prepared.session.id)
            return
          }
          await ledger.update((state) => {
            const current = state.jobs.find((item) => item.id === job.id)!
            if (state.paused || current.status === "cancelled" || ctx.abort.aborted)
              throw new Error("Async dispatch cancelled before launch")
            current.status = "running"
            current.updated_at = Date.now()
          })
          const run = Instance.bind(async () => {
            try {
              const result = await SessionPrompt.loop({
                sessionID: prepared.session.id,
                async_owner: ctx.sessionID,
                async_job: job.id,
              })
              if (result.info.role === "assistant" && result.info.error) {
                await SessionPrompt.cancel(prepared.session.id)
                await complete(ctx.sessionID, job.id, "failed", {
                  error: result.info.error.name,
                  result: result.parts
                    .filter((part) => part.type === "text")
                    .map((part) => part.text)
                    .join("\n"),
                })
              } else if (
                result.info.role === "assistant" &&
                result.info.finish === "stop" &&
                result.info.time.completed
              ) {
                // This returned result belongs to the run we launched, including
                // synthetic parent messages introduced by automatic compaction.
                await complete(ctx.sessionID, job.id, "completed", {
                  result: result.parts
                    .filter((part) => part.type === "text")
                    .map((part) => part.text)
                    .join("\n"),
                })
              } else {
                await SessionPrompt.cancel(prepared.session.id)
                await complete(ctx.sessionID, job.id, "failed", {
                  error: "Child ended without a confirmed complete result",
                })
              }
            } catch (err) {
              await SessionPrompt.cancel(prepared.session.id).catch(() => {})
              await complete(ctx.sessionID, job.id, "failed", {
                error: err instanceof Error ? err.message : "Async child failed",
              })
            } finally {
              watch(ctx.sessionID, job.id)
            }
          })
          const work = run()
          runs.set(job.id, { owner: ctx.sessionID, work })
          void work
            .finally(() => runs.delete(job.id))
            .catch((error) => {
              log.error("background task state update failed", { owner: ctx.sessionID, task: job.id, error })
            })
          watch(ctx.sessionID, job.id)
        } catch (err) {
          const { SessionPrompt } = await import("./prompt")
          // Preparation has not launched a model. A rejected permission or an
          // unknown role is a failed job, even when no child was created. If the
          // child exists, clear any partial prompt/retry before reporting failure.
          try {
            const session = await child(job.child_session_id)
            if (session) await SessionPrompt.cancel(session.id)
          } catch (cleanup) {
            await complete(ctx.sessionID, job.id, "uncertain", {
              error: `${err instanceof Error ? err.message : "Async dispatch failed"}; cleanup not confirmed: ${cleanup instanceof Error ? cleanup.message : String(cleanup)}`,
            })
            return
          }
          await complete(ctx.sessionID, job.id, "failed", {
            error: err instanceof Error ? err.message : "Async dispatch failed; not resubmitted",
          })
        }
      }),
    )
    return status(ctx.sessionID)
  }

  export async function status(owner: string) {
    const ledger = store(owner)
    const state = await ledger.read()
    const jobs = await Promise.all(
      state.jobs.map(async (job) => {
        if (!terminal.has(job.status)) watch(owner, job.id)
        const session = await child(job.child_session_id)
        const live = await SessionStatus.get(SessionID.make(job.child_session_id))
        const { prompt, result, error, request_hash, boot: lease, ...meta } = job
        return {
          ...meta,
          child_exists: !!session,
          role: job.subagent_type,
          session_id: job.child_session_id,
          parent_session_id: owner,
          category: job.category,
          purpose: job.category,
          direction_id: job.direction,
          status:
            terminal.has(job.status) && live.type !== "idle"
              ? "cancelling"
              : live.type === "retry"
                ? "retrying"
                : job.status,
          ...(live?.type === "retry" ? { retry: live } : {}),
        }
      }),
    )
    return {
      schema_version: 1 as const,
      owner_session_id: owner,
      root_session_id: owner,
      revision: state.revision,
      event_cursor: state.event_cursor,
      paused: state.paused,
      pause_token: state.pause_token,
      needs_attention: jobs.some((job) => ["failed", "interrupted", "uncertain", "cancelling"].includes(job.status)),
      jobs,
    }
  }

  export async function collect(owner: string, ack: { id: string; event_seq: number }[] = []) {
    const ledger = store(owner)
    if (ack.length) await ledger.ack(ack)
    const state = await ledger.read()
    return {
      owner_session_id: owner,
      event_cursor: state.event_cursor,
      results: state.jobs
        .filter((job) => terminal.has(job.status) && !job.acknowledged)
        .map((job) => ({
          id: job.id,
          event_seq: job.event_seq,
          status: job.status,
          description: job.description,
          direction: job.direction,
          child_session_id: job.child_session_id,
          error: job.error,
          result: job.result?.slice(0, 12000),
          result_truncated: (job.result?.length ?? 0) > 12000,
          ledger_path: ledger.file,
        })),
    }
  }

  export async function cancel(owner: string, ids: string[]) {
    const ledger = store(owner)
    const before = await ledger.read()
    const eligible: string[] = []
    for (const id of ids) {
      const job = before.jobs.find((job) => job.id === id)
      if (!job) throw new Error("Cannot cancel a task owned by another session")
      const live = await SessionStatus.get(SessionID.make(job.child_session_id))
      if (terminal.has(job.status) && live.type === "idle") continue
      if (!(await child(job.child_session_id))) continue
      const rows = await Session.messages({ sessionID: SessionID.make(job.child_session_id), limit: 40 })
      const latest = rows.filter((row) => row.info.role === "user").at(-1)
      const own = latest?.parts.some((part) => part.type === "text" && part.metadata?.async_task_id === job.id)
      const continuation =
        latest?.parts.length &&
        latest.parts.every(
          (part) =>
            part.type === "text" &&
            part.synthetic === true &&
            part.text.trim() ===
              "Continue if you have next steps, or stop and ask for clarification if you are unsure how to proceed.",
        )
      if (latest && latest.info.id !== job.message_id && !own && !continuation)
        throw new Error("Child conversation has a newer request; refusing to cancel another task generation")
      eligible.push(id)
    }
    const jobs = await ledger.update((state) =>
      ids.map((id) => {
        const job = state.jobs.find((job) => job.id === id)
        if (!job) throw new Error("Cannot cancel a task owned by another session")
        if (!terminal.has(job.status)) {
          job.status = "cancelled"
          job.updated_at = Date.now()
          job.event_seq = ++state.event_cursor
          job.acknowledged = false
        }
        return job
      }),
    )
    const { SessionPrompt } = await import("./prompt")
    await Promise.all(
      jobs
        .filter((job) => eligible.includes(job.id))
        .map((job) => SessionPrompt.cancel(SessionID.make(job.child_session_id))),
    )
    await drain(owner, ids)
    await Promise.all(jobs.map(notify))
    return status(owner)
  }

  // Stop must not report success while an owned run can still write a late
  // result. Keep this wait bounded so a broken provider cannot hang Stop.
  export async function drain(owner: string, ids?: string[]) {
    const pending = [...runs].filter(([id, run]) => run.owner === owner && (!ids || ids.includes(id)))
    if (!pending.length) return
    const timer = new AbortController()
    const timeout = setTimeout(() => timer.abort(), 3000)
    try {
      await Promise.race([
        Promise.all(pending.map(([, run]) => run.work)),
        new Promise<never>((_, reject) => {
          timer.signal.addEventListener(
            "abort",
            () => reject(new Error("Owned background tasks have not finished stopping")),
            { once: true },
          )
        }),
      ])
    } finally {
      clearTimeout(timeout)
    }
  }

  export async function pause(owner: string) {
    const state = await store(owner).pause()
    await Promise.all(state.jobs.filter((job) => job.status === "cancelled").map(notify))
  }

  export async function resume(owner: string, token: string | null) {
    await store(owner).resume(token)
    return status(owner)
  }
}
