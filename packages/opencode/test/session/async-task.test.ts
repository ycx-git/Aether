import { describe, expect, test } from "bun:test"
import { Bus } from "../../src/bus"
import { Global } from "../../src/global"
import * as fs from "fs/promises"
import path from "path"
import { Store, AsyncTasks } from "../../src/session/async-task"
import { Instance } from "../../src/project/instance"
import { Session } from "../../src/session"
import { SessionPrompt } from "../../src/session/prompt"
import { SessionStatus } from "../../src/session/status"
import { SessionWatchdog } from "../../src/session/watchdog"
import { SessionID, MessageID } from "../../src/session/schema"
import { ProviderID, ModelID } from "../../src/provider/schema"
import { Agent } from "../../src/agent/agent"
import { Permission } from "../../src/permission"
import { Memory } from "../../src/memory"
import type { Tool } from "../../src/tool/tool"
import { serve } from "../lib/server"
import { tmpdir } from "../fixture/fixture"

const owner = "ses_async_offline_owner"

function spec(n = 0) {
  return {
    description: "Direction " + n,
    prompt: "Investigate synthetic question " + n,
    subagent_type: "theorist",
    category: "exploration" as const,
    direction: "mechanism-" + n,
  }
}

describe("async task durable registry", () => {
  test("plans stable child and message identities before dispatch", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const jobs = await store.plan([spec(1), spec(2)], "call-a")
    expect(jobs).toHaveLength(2)
    expect(new Set(jobs.map((job) => job.id)).size).toBe(2)
    expect(new Set(jobs.map((job) => job.child_session_id)).size).toBe(2)
    expect(new Set(jobs.map((job) => job.message_id)).size).toBe(2)
    expect(jobs.every((job) => job.child_session_id.startsWith("ses_"))).toBe(true)
    expect(jobs.every((job) => job.message_id.startsWith("msg_"))).toBe(true)
    const reopened = await new Store(dir.path, owner).read()
    expect(reopened.jobs.map((job) => job.id)).toEqual(jobs.map((job) => job.id))
    expect(reopened.jobs.map((job) => job.message_id)).toEqual(jobs.map((job) => job.message_id))
  })

  test("replaying one request returns the original jobs without minting more", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const first = await store.plan([spec(1), spec(2)], "same-call")
    const second = await new Store(dir.path, owner).plan([spec(1), spec(2)], "same-call")
    expect(second.map((job) => job.id)).toEqual(first.map((job) => job.id))
    expect((await store.read()).jobs).toHaveLength(2)
  })

  test("reusing an idempotency key with changed work is rejected", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    await store.plan([spec(1)], "same-call")
    await expect(store.plan([spec(2)], "same-call")).rejects.toThrow()
    expect((await store.read()).jobs[0]?.direction).toBe("mechanism-1")
  })

  test("concurrent independent plans retain every job in readable JSON", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    await Promise.all(Array.from({ length: 12 }, (_, n) => new Store(dir.path, owner).plan([spec(n)], "call-" + n)))
    const saved = await store.read()
    expect(saved.jobs).toHaveLength(12)
    expect(new Set(saved.jobs.map((job) => job.id)).size).toBe(12)
    expect(new Set(saved.jobs.map((job) => job.direction)).size).toBe(12)
    expect(saved.revision).toBeGreaterThanOrEqual(12)
  })

  test("an aborted update cannot publish a partly changed registry", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    await store.plan([spec(1)], "call")
    const before = await store.read()
    await expect(
      store.update((state) => {
        state.jobs[0]!.description = "uncommitted"
        throw new Error("synthetic failure before commit")
      }),
    ).rejects.toThrow("synthetic failure")
    expect(await store.read()).toEqual(before)
  })

  test("complete persists a stable event and payload across a new Store", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const [job] = await store.plan([spec(1)], "call")
    const finished = await store.complete(job!.id, "completed", { result: "Synthetic decisive result" })
    expect(finished.status).toBe("completed")
    expect(finished.event_seq).toBeGreaterThan(0)
    const reopened = await new Store(dir.path, owner).read()
    const saved = reopened.jobs.find((item) => item.id === job!.id)!
    expect(saved.result).toBe("Synthetic decisive result")
    expect(saved.event_seq).toBe(finished.event_seq)
    expect(saved.acknowledged).toBe(false)
  })

  test("terminal replay cannot create a second completion event", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const [job] = await store.plan([spec(1)], "call")
    const first = await store.complete(job!.id, "completed", { result: "first" })
    const second = await store.complete(job!.id, "completed", { result: "duplicate" })
    expect(second.event_seq).toBe(first.event_seq)
    expect(second.result).toBe("first")
    expect((await store.read()).event_cursor).toBe(first.event_seq)
  })

  test("ack consumes only the exact terminal event", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const jobs = await store.plan([spec(1), spec(2)], "call")
    const a = await store.complete(jobs[0]!.id, "completed", { result: "A" })
    const b = await store.complete(jobs[1]!.id, "failed", { error: "Synthetic failure" })
    await store.ack([{ id: a.id, event_seq: a.event_seq }])
    const reopened = await new Store(dir.path, owner).read()
    expect(reopened.jobs.find((job) => job.id === a.id)?.acknowledged).toBe(true)
    expect(reopened.jobs.find((job) => job.id === b.id)?.acknowledged).toBe(false)
    expect(reopened.jobs.find((job) => job.id === b.id)?.error).toBe("Synthetic failure")
  })

  test("wrong event sequence does not consume a result", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const [job] = await store.plan([spec(1)], "call")
    const done = await store.complete(job!.id, "completed", { result: "keep me" })
    await store.ack([{ id: done.id, event_seq: done.event_seq + 1 }]).catch(() => undefined)
    expect((await store.read()).jobs[0]?.acknowledged).toBe(false)
  })

  test("foreign owner cannot acknowledge another owner's job", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const [job] = await store.plan([spec(1)], "call")
    const done = await store.complete(job!.id, "completed", { result: "private owner result" })
    const foreign = new Store(dir.path, "ses_async_other_owner")
    await foreign.ack([{ id: done.id, event_seq: done.event_seq }]).catch(() => undefined)
    expect((await store.read()).jobs[0]?.acknowledged).toBe(false)
    expect((await foreign.read()).jobs).toHaveLength(0)
  })

  test("explicit pause blocks new work and preserves its token on reread", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    await store.plan([spec(1)], "call")
    const paused = await store.pause()
    expect(paused.paused).toBe(true)
    expect(paused.pause_token).toBeTruthy()
    await expect(store.plan([spec(2)], "new-call")).rejects.toThrow()
    const reopened = await new Store(dir.path, owner).read()
    expect(reopened.paused).toBe(true)
    expect(reopened.pause_token).toBe(paused.pause_token)
  })

  test("a cancelled task cannot be revived by a late success", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const [job] = await store.plan([spec(1)], "call")
    await store.pause()
    const done = await store.complete(job!.id, "completed", { result: "late output" })
    expect(done.status).toBe("cancelled")
    expect(done.result).not.toBe("late output")
  })

  test("resume requires the exact current pause token and never requeues old jobs", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const [job] = await store.plan([spec(1)], "old-call")
    const paused = await store.pause()
    await expect(store.resume("wrong-token")).rejects.toThrow()
    expect((await store.read()).paused).toBe(true)
    const resumed = await store.resume(paused.pause_token!)
    expect(resumed.paused).toBe(false)
    expect(resumed.jobs.find((item) => item.id === job!.id)?.status).toBe("cancelled")
    const next = await store.plan([spec(2)], "new-call")
    expect(next[0]?.id).not.toBe(job!.id)
  })

  test("a second stop invalidates an earlier resume token", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const first = await store.pause()
    const second = await store.pause()
    expect(second.pause_token).not.toBe(first.pause_token)
    await expect(store.resume(first.pause_token!)).rejects.toThrow()
    expect((await store.read()).pause_token).toBe(second.pause_token)
  })

  test("new Store reads interrupted state without dispatching or changing it", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    const [job] = await store.plan([spec(1)], "call")
    await store.complete(job!.id, "interrupted", { error: "Previous process ended" })
    const before = await store.read()
    const reopened = await new Store(dir.path, owner).read()
    expect(reopened).toEqual(before)
    expect(reopened.jobs[0]?.status).toBe("interrupted")
    expect(reopened.jobs[0]?.message_id).toBe(job!.message_id)
  })

  test("corrupt persisted JSON fails visibly rather than replacing jobs with an empty registry", async () => {
    await using dir = await tmpdir()
    const store = new Store(dir.path, owner)
    await store.plan([spec(1)], "call")
    const file = path.join(dir.path, ".aether", "async-tasks", owner + ".json")
    await fs.writeFile(file, "{broken JSON", "utf8")
    await expect(new Store(dir.path, owner).read()).rejects.toThrow()
    expect(await fs.readFile(file, "utf8")).toBe("{broken JSON")
  })
})

const pid = ProviderID.make("alibaba-cn")
const mid = ModelID.make("glm-5.2")

function stream() {
  const chunk = (delta: Record<string, unknown>, finish?: string) => ({
    id: "chatcmpl-async-local",
    object: "chat.completion.chunk",
    choices: [{ delta, ...(finish ? { finish_reason: finish } : {}) }],
    ...(finish ? { usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } } : {}),
  })
  return new Response(
    "data: " +
      JSON.stringify(chunk({ role: "assistant", content: "Synthetic result" })) +
      "\n\n" +
      "data: " +
      JSON.stringify(chunk({}, "stop")) +
      "\n\n" +
      "data: [DONE]\n\n",
    { headers: { "Content-Type": "text/event-stream" } },
  )
}

async function fixture(
  response = stream,
  access: Record<string, "allow" | "deny"> = { "*": "deny", theorist: "allow" },
) {
  const gates: Array<() => void> = []
  const server = await serve({
    port: 0,
    async fetch(req) {
      await req.json()
      await new Promise<void>((resolve) => gates.push(resolve))
      return response()
    },
  })
  const dir = await tmpdir({
    config: {
      model: pid + "/" + mid,
      enabled_providers: [pid],
      memory: { enabled: false },
      agent: {
        build: { permission: { "*": "allow", task: access } },
        theorist: {
          mode: "subagent",
          model: pid + "/" + mid,
          prompt: "Return a concise synthetic answer.",
          permission: { "*": "deny" },
        },
      },
      provider: {
        [pid]: {
          npm: "@ai-sdk/openai-compatible",
          models: {
            [mid]: {
              name: "GLM-5.2",
              tool_call: true,
              temperature: true,
              limit: { context: 1000000, output: 4096 },
              modalities: { input: ["text"], output: ["text"] },
            },
          },
          options: { apiKey: "synthetic-local-key", baseURL: server.url.origin + "/v1" },
        },
      },
    },
  })
  return {
    path: dir.path,
    gates,
    async [Symbol.asyncDispose]() {
      gates.forEach((release) => release())
      await Memory.stop()
      await server.stop(true)
      await dir[Symbol.asyncDispose]()
    },
  }
}

async function poll(check: () => Promise<boolean>) {
  const deadline = Date.now() + 15000
  while (Date.now() < deadline) {
    if (await check()) return
    await Bun.sleep(25)
  }
  throw new Error("Synthetic async-task condition was not reached")
}

async function context(directory: string, engine?: Session.Info["engine"]) {
  const session = await Session.create({ title: "Synthetic async owner", engine })
  const parent = MessageID.ascending()
  await Session.updateMessage({
    id: parent,
    sessionID: session.id,
    role: "user",
    time: { created: Date.now() },
    agent: "build",
    model: { providerID: pid, modelID: mid },
  })
  const message = MessageID.ascending()
  await Session.updateMessage({
    id: message,
    sessionID: session.id,
    parentID: parent,
    role: "assistant",
    agent: "build",
    mode: "build",
    modelID: mid,
    providerID: pid,
    time: { created: Date.now() },
    path: { cwd: directory, root: directory },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  })
  const role = (await Agent.get("build"))!
  const controller = new AbortController()
  const ctx: Tool.Context = {
    sessionID: session.id,
    messageID: message,
    callID: "synthetic-call",
    agent: "build",
    abort: controller.signal,
    messages: [],
    metadata() {},
    ask: (request) => Permission.ask({ ...request, sessionID: session.id, ruleset: role.permission }),
  }
  return { session, controller, ctx }
}

describe("async task independent lifetimes", () => {
  test("a broken native queue cannot prevent delegated children from stopping", async () => {
    await using dir = await fixture()
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path, { type: "codex", permission: "user" })
        const file = path.join(Global.Path.data, "native-queue", Instance.project.id, root.session.id + ".json")
        try {
          const started = await AsyncTasks.start([spec(1)], root.ctx, "native-cancel")
          await poll(async () => dir.gates.length === 1)
          await fs.mkdir(path.dirname(file), { recursive: true })
          await fs.writeFile(file, "{invalid", "utf8")
          await expect(SessionPrompt.cancel(root.session.id)).rejects.toBeInstanceOf(AggregateError)
          await poll(
            async () => (await SessionStatus.get(SessionID.make(started.jobs[0]!.child_session_id))).type === "idle",
          )
          const stopped = await AsyncTasks.status(root.session.id)
          expect(stopped.paused).toBe(true)
          expect(stopped.jobs[0]!.status).toBe("cancelled")
          expect((await SessionStatus.get(root.session.id)).type).toBe("idle")
        } finally {
          await fs.unlink(file).catch((error: NodeJS.ErrnoException) => {
            if (error.code !== "ENOENT") throw error
          })
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)

  test("completion events allow replenishment while a slow sibling remains busy", async () => {
    await using dir = await fixture()
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path)
        const events: Array<{ taskID: string; eventID: string; sequence: number }> = []
        const off = Bus.subscribe(AsyncTasks.Event.Completed, (event) => events.push(event.properties))
        try {
          const started = await AsyncTasks.start([spec(1), spec(2)], root.ctx, "event-initial")
          expect(() => AsyncTasks.Metadata.parse(started)).not.toThrow()
          await poll(async () => dir.gates.length === 2)
          root.controller.abort() // A normal finished owner turn leaves workers alive.
          dir.gates[0]!()
          await poll(async () => events.length > 0)
          const first = (await AsyncTasks.collect(root.session.id)).results[0]!
          expect(events[0]!.taskID).toBe(first.id)
          expect(events[0]!.eventID).toBe(`${first.id}:${first.event_seq}`)
          expect(Object.keys(events[0]!).sort()).toEqual([
            "childSessionID",
            "eventID",
            "sequence",
            "sessionID",
            "status",
            "taskID",
          ])
          expect((await AsyncTasks.collect(root.session.id)).results[0]!.id).toBe(first.id)
          expect(
            (await AsyncTasks.collect(root.session.id, [{ id: first.id, event_seq: first.event_seq }])).results,
          ).toHaveLength(0)

          const slow = started.jobs.find((job) => job.id !== first.id)!
          const refill = await AsyncTasks.start(
            [spec(3)],
            { ...root.ctx, callID: "refill", abort: new AbortController().signal },
            "event-refill",
          )
          await poll(async () => dir.gates.length === 3)
          expect((await SessionStatus.get(SessionID.make(slow.child_session_id))).type).toBe("busy")
          expect(refill.jobs.filter((job) => job.status === "running")).toHaveLength(2)
          dir.gates[2]!()
          await poll(async () => (await AsyncTasks.collect(root.session.id)).results.length === 1)
          expect((await AsyncTasks.collect(root.session.id)).results[0]!.id).not.toBe(slow.id)
          expect((await SessionStatus.get(SessionID.make(slow.child_session_id))).type).toBe("busy")
          dir.gates[1]!()
          await poll(async () => (await AsyncTasks.collect(root.session.id)).results.length === 2)
        } finally {
          off()
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)

  test("dispatch returns before completion, replay is idempotent, and one result is collectable without its sibling", async () => {
    await using dir = await fixture()
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path)
        try {
          const started = await AsyncTasks.start([spec(1), spec(2)], root.ctx, "synthetic-request")
          expect(started.jobs).toHaveLength(2)
          expect(started.jobs.every((job) => job.status === "running")).toBe(true)
          expect((await AsyncTasks.collect(root.session.id)).results).toHaveLength(0)
          await poll(async () => dir.gates.length === 2)
          const replay = await AsyncTasks.start([spec(1), spec(2)], root.ctx, "synthetic-request")
          expect(replay.jobs.map((job) => job.id)).toEqual(started.jobs.map((job) => job.id))
          expect(dir.gates).toHaveLength(2)
          // Normal parent finish aborts the turn signal. This must not cancel
          // already-dispatched background sessions.
          root.controller.abort()
          await Bun.sleep(50)
          expect((await AsyncTasks.status(root.session.id)).jobs.every((job) => job.status !== "cancelled")).toBe(true)
          dir.gates[0]!()
          await poll(async () => (await AsyncTasks.collect(root.session.id)).results.length === 1)
          const partial = await AsyncTasks.collect(root.session.id)
          expect(partial.results[0]?.status).toBe("completed")
          expect(partial.results[0]?.result).toContain("Synthetic result")
          expect(
            (await AsyncTasks.status(root.session.id)).jobs.filter((job) => job.status === "running"),
          ).toHaveLength(1)
          dir.gates[1]!()
          await poll(async () => (await AsyncTasks.collect(root.session.id)).results.length === 2)
        } finally {
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)

  test("explicit Stop cancels owned background work even when the parent is idle", async () => {
    await using dir = await fixture()
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path)
        try {
          const started = await AsyncTasks.start([spec(1)], root.ctx, "synthetic-stop")
          await poll(async () => dir.gates.length === 1)
          await SessionPrompt.cancel(root.session.id)
          const stopping = await AsyncTasks.status(root.session.id)
          expect(stopping.paused).toBe(true)
          expect(["cancelled", "cancelling"]).toContain(
            stopping.jobs.find((job) => job.id === started.jobs[0]!.id)!.status,
          )
          await poll(async () =>
            (await AsyncTasks.status(root.session.id)).jobs.every((job) => job.status === "cancelled"),
          )
          const stopped = await AsyncTasks.status(root.session.id)
          expect(stopped.jobs.find((job) => job.id === started.jobs[0]?.id)?.status).toBe("cancelled")
          expect((await SessionStatus.list()).has(SessionID.make(started.jobs[0]!.child_session_id))).toBe(false)
          await expect(AsyncTasks.start([spec(2)], root.ctx, "after-stop")).rejects.toThrow()
          dir.gates[0]!()
          await Bun.sleep(50)
          expect((await AsyncTasks.status(root.session.id)).jobs[0]?.status).toBe("cancelled")
        } finally {
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)
  test("Stop during preparation prevents a delayed permission reply from launching a child", async () => {
    await using dir = await fixture()
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path)
        let entered = false
        let release!: () => void
        const gate = new Promise<void>((resolve) => {
          release = resolve
        })
        const ask = root.ctx.ask
        root.ctx.ask = async (request) => {
          entered = true
          await gate
          await ask(request)
        }
        try {
          const dispatch = AsyncTasks.start([spec(1)], root.ctx, "synthetic-prepare-stop")
          await poll(async () => entered)
          await SessionPrompt.cancel(root.session.id)
          release()
          const stopped = await dispatch
          expect(stopped.paused).toBe(true)
          expect(stopped.jobs[0]?.status).toBe("cancelled")
          expect(dir.gates).toHaveLength(0)
          expect((await SessionStatus.list()).size).toBe(0)
        } finally {
          release()
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)

  test("a terminal provider error clears a pending watchdog and leaves no active child", async () => {
    await using dir = await fixture(
      () =>
        new Response(
          JSON.stringify({
            error: {
              message: "Synthetic invalid request",
              type: "invalid_request_error",
              code: "synthetic_invalid_request",
            },
          }),
          { status: 400, headers: { "Content-Type": "application/json" } },
        ),
    )
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path)
        try {
          const started = await AsyncTasks.start([spec(1)], root.ctx, "synthetic-terminal-error")
          const job = started.jobs[0]!
          const child = SessionID.make(job.child_session_id)
          await poll(async () => dir.gates.length === 1)
          await SessionWatchdog.defer({
            sessionID: child,
            messageID: MessageID.make(job.message_id),
            message: "Synthetic pending retry before terminal response",
          })
          expect(await SessionWatchdog.pending(child)).toBe(true)
          dir.gates[0]!()
          await poll(async () => (await AsyncTasks.collect(root.session.id)).results.length === 1)
          const collected = await AsyncTasks.collect(root.session.id)
          expect(collected.results[0]?.status).toBe("failed")
          expect(collected.results[0]?.error).toBeTruthy()
          await poll(async () => (await SessionStatus.get(child)).type === "idle")
          expect(await SessionWatchdog.pending(child)).toBe(false)
          const metadata = await AsyncTasks.status(root.session.id)
          expect(metadata.jobs[0]?.status).toBe("failed")
          expect(metadata.jobs[0]).not.toHaveProperty("error")
          expect(metadata.jobs[0]).not.toHaveProperty("prompt")
          expect(metadata.jobs[0]).not.toHaveProperty("result")
          expect(() => AsyncTasks.Metadata.parse(metadata)).not.toThrow()
        } finally {
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)
  test("one unknown role cannot block an independent valid sibling", async () => {
    await using dir = await fixture(stream, { "*": "allow" })
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path)
        try {
          const state = await AsyncTasks.start(
            [{ ...spec(1), subagent_type: "synthetic-nonexistent-role" }, spec(2)],
            root.ctx,
            "synthetic-independent-failure",
          )
          expect(state.jobs).toHaveLength(2)
          expect(state.jobs.find((job) => job.direction === "mechanism-1")?.status).toBe("failed")
          expect(state.jobs.find((job) => job.direction === "mechanism-2")?.status).toBe("running")
          await poll(async () => dir.gates.length === 1)
          const partial = await AsyncTasks.collect(root.session.id)
          expect(partial.results).toHaveLength(1)
          expect(partial.results[0]?.status).toBe("failed")
          expect(partial.results[0]?.error).toContain("Unknown agent type")
          dir.gates[0]!()
          await poll(async () => (await AsyncTasks.collect(root.session.id)).results.length === 2)
          expect((await AsyncTasks.collect(root.session.id)).results.some((job) => job.status === "completed")).toBe(
            true,
          )
        } finally {
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)

  test("root Stop clears both delegation levels while preserving forks and unrelated sessions", async () => {
    await using dir = await fixture()
    await Instance.provide({
      directory: dir.path,
      fn: async () => {
        const root = await context(dir.path)
        const child = await Session.create({ parentID: root.session.id, title: "Synthetic theory team" })
        const worker = await Session.create({ parentID: child.id, title: "Synthetic bounded worker" })
        const fork = await Session.fork({ sessionID: root.session.id })
        const unrelated = await Session.create({ title: "Unrelated work" })
        try {
          for (const item of [child, worker, fork, unrelated]) await SessionStatus.set(item.id, { type: "busy" })
          await SessionPrompt.cancel(root.session.id)
          expect(await SessionStatus.get(child.id)).toEqual({ type: "idle" })
          expect(await SessionStatus.get(worker.id)).toEqual({ type: "idle" })
          expect(await SessionStatus.get(fork.id)).toEqual({ type: "busy" })
          expect(await SessionStatus.get(unrelated.id)).toEqual({ type: "busy" })
          expect((await AsyncTasks.status(root.session.id)).paused).toBe(true)
          expect((await AsyncTasks.status(child.id)).paused).toBe(true)
          expect((await AsyncTasks.status(worker.id)).paused).toBe(true)
          expect((await AsyncTasks.status(fork.id)).paused).toBe(false)
          expect((await AsyncTasks.status(unrelated.id)).paused).toBe(false)
          expect(dir.gates).toHaveLength(0)
        } finally {
          await SessionStatus.set(fork.id, { type: "idle" })
          await SessionStatus.set(unrelated.id, { type: "idle" })
          await SessionPrompt.cancel(root.session.id)
        }
      },
    })
  }, 30000)
})
