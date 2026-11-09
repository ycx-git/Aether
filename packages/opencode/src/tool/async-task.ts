import z from "zod"
import { Tool } from "./tool"
import { AsyncTasks } from "../session/async-task"

const job = z.object({
  description: z.string().min(1).max(240),
  prompt: z.string().min(1),
  subagent_type: z.string().min(1),
  category: z.enum(["exploration", "support"]),
  direction: z
    .string()
    .min(1)
    .max(300)
    .describe("The distinct research mechanism/question; support work is not a discovery direction"),
})

export const TaskStartTool = Tool.define("task_start", {
  description:
    "Main coordinator only. Start independent background tasks and return their durable ids immediately; do not wait for results. Uses the same agent permissions/models as task. Poll task_status or task_collect, acknowledge collected events only after using them, and refill exploration directions as tasks finish. Before replacing a retrying job, cancel it so an old retry cannot silently revive. Subagents use ordinary task for their bounded workers.",
  parameters: z.object({ tasks: z.array(job).min(1).max(25), request_id: z.string().min(1).max(160).optional() }),
  async execute(params, ctx) {
    const result = await AsyncTasks.start(params.tasks, ctx, params.request_id)
    return {
      title: "Background tasks started",
      metadata: { owner: ctx.sessionID, jobs: result.jobs.map((job) => job.id) },
      output: JSON.stringify(result),
    }
  },
})

export const TaskCollectTool = Tool.define("task_collect", {
  description:
    "Return each completed owned background task independently, without waiting for other tasks. Results remain available until explicitly acknowledged by matching id/event_seq. Acknowledge only results already received and incorporated; repeating collection is safe. Large results include their durable ledger path.",
  parameters: z.object({
    acknowledge: z.array(z.object({ id: z.string(), event_seq: z.number().int().min(1) })).optional(),
  }),
  async execute(params, ctx) {
    const result = await AsyncTasks.collect(ctx.sessionID, params.acknowledge)
    return {
      title: "Available background task results",
      metadata: { event_cursor: result.event_cursor, count: result.results.length },
      output: JSON.stringify(result),
    }
  },
})

export const TaskStatusTool = Tool.define("task_status", {
  description:
    "Read metadata for this coordinator's background tasks. Retrying, cancelled, failed and interrupted jobs do not count as effective concurrent exploration. No prompts are submitted or resumed.",
  parameters: z.object({}),
  async execute(_params, ctx) {
    const result = await AsyncTasks.status(ctx.sessionID)
    return {
      title: "Background task status",
      metadata: { event_cursor: result.event_cursor },
      output: JSON.stringify(result),
    }
  },
})

export const TaskCancelTool = Tool.define("task_cancel", {
  description:
    "Cancel only this coordinator's selected background jobs and their bounded workers, clearing retry timers. Other jobs continue. Use before replacing waiting tasks; preserved results are not deleted.",
  parameters: z.object({ ids: z.array(z.string()).min(1).max(25) }),
  async execute(params, ctx) {
    const result = await AsyncTasks.cancel(ctx.sessionID, params.ids)
    return {
      title: "Selected background tasks cancelled",
      metadata: { ids: params.ids },
      output: JSON.stringify(result),
    }
  },
})
