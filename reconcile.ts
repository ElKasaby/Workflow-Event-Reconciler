// ASSUMPTIONS
// 1. Events may arrive out of order.
// 2. Deduplication key: (workflow_id, source_service, sequence).
// 3. Conflict window applies to transitions with same `from` state.
// 4. GAP timestamp equals the next event timestamp.

// Language: TypeScript (Node.js >=18)
// How to run:
// ts-node reconciler.ts <input_file> <conflict_window_seconds> <dedup_strategy>

import { createReadStream } from "fs";
import { createInterface } from "readline";

interface Event {
  event_id: string;
  workflow_id: string;
  timestamp: string;
  timestamp_ms: number;
  source_service: string;
  sequence: number;
  transition: {
    from: string;
    to: string;
  };
  metadata?: Record<string, unknown>;
  duplicates_removed?: number;
}

interface Tombstone {
  event_id: string;
  workflow_id: string;
  status: "SUPERSEDED";
  superseded_by: string;
  original_transition: { from: string; to: string };
}

interface Gap {
  type: "GAP";
  workflow_id: string;
  expected_from: string;
  actual_from: string;
  between_events: [string, string];
  timestamp: string;
}

interface Summary {
  type: "SUMMARY";
  workflow_id: string;
  no_ops_removed: number;
  total_events_processed: number;
  total_conflicts_resolved: number;
  total_duplicates_removed: number;
  total_malformed_skipped: number;
  transition_chain: string[];
  gaps_detected: number;
}

type OutputRecord = Event | Tombstone | Gap | Summary;

const EVENT_ID_REGEX = /^evt_[a-zA-Z0-9]+$/;

function isValidEvent(obj: unknown): obj is Event {
  if (!obj || typeof obj !== "object") return false;

  const e = obj as Record<string, unknown>;

  if (typeof e.event_id !== "string") return false;
  if (!EVENT_ID_REGEX.test(e.event_id)) return false;

  if (typeof e.workflow_id !== "string") return false;

  if (typeof e.timestamp !== "string") return false;
  const ts = Date.parse(e.timestamp);
  if (Number.isNaN(ts)) return false;

  if (typeof e.source_service !== "string") return false;

  if (
    typeof e.sequence !== "number" ||
    !Number.isInteger(e.sequence) ||
    e.sequence < 1
  )
    return false;

  if (!e.transition || typeof e.transition !== "object") return false;

  const t = e.transition as Record<string, unknown>;

  if (typeof t.from !== "string" || t.from.length === 0) return false;
  if (typeof t.to !== "string" || t.to.length === 0) return false;

  (obj as Event).timestamp_ms = ts;

  return true;
}

function chooseDedup(
  events: Event[],
  strategy: "sequence" | "timestamp",
): Event {
  if (strategy === "sequence") {
    return events.sort((a, b) => a.event_id.localeCompare(b.event_id))[0]!;
  }

  return events.sort((a, b) => {
    if (a.timestamp_ms !== b.timestamp_ms)
      return a.timestamp_ms - b.timestamp_ms;
    return a.event_id.localeCompare(b.event_id);
  })[0]!;
}

function chooseWinner(events: Event[]): Event {
  return events.sort((a, b) => {
    if (a.sequence !== b.sequence) return b.sequence - a.sequence;
    if (a.timestamp_ms !== b.timestamp_ms)
      return b.timestamp_ms - a.timestamp_ms;
    return a.source_service.localeCompare(b.source_service);
  })[0]!;
}

function buildTransitionChain(events: Event[]): string[] {
  if (events.length === 0) return [];

  const chain: string[] = [events[0]!.transition.from];

  for (const e of events) {
    chain.push(e.transition.to);
  }

  return chain;
}

function processWorkflow(
  events: Event[],
  conflictWindow: number,
  strategy: "sequence" | "timestamp",
  workflow_id: string,
) {
  const duplicateGroups = new Map<string, Event[]>();

  for (const e of events) {
    const key = `${e.workflow_id}:${e.source_service}:${e.sequence}`;
    if (!duplicateGroups.has(key)) duplicateGroups.set(key, []);
    duplicateGroups.get(key)!.push(e);
  }

  const deduped: Event[] = [];
  let duplicatesRemoved = 0;

  for (const group of duplicateGroups.values()) {
    const chosen = chooseDedup(group, strategy);

    const removed = group.length - 1;

    if (removed > 0) {
      chosen.duplicates_removed = removed;
      duplicatesRemoved += removed;
    }

    deduped.push(chosen);
  }

  const noOpsRemoved = deduped.filter(
    (e) => e.transition.from === e.transition.to,
  ).length;

  const filtered = deduped.filter((e) => e.transition.from !== e.transition.to);

  const byFrom = new Map<string, Event[]>();

  for (const e of filtered) {
    if (!byFrom.has(e.transition.from)) byFrom.set(e.transition.from, []);
    byFrom.get(e.transition.from)!.push(e);
  }

  const survivors = new Set<string>(filtered.map((e) => e.event_id));

  const tombstones: Tombstone[] = [];

  let conflictsResolved = 0;

  for (const group of byFrom.values()) {
    group.sort((a, b) => a.timestamp_ms - b.timestamp_ms);

    for (let i = 0; i < group.length; i++) {
      const base = group[i]!;

      const cluster: Event[] = [base];

      for (let j = i + 1; j < group.length; j++) {
        const candidate = group[j]!;

        const diff = Math.abs(candidate.timestamp_ms - base.timestamp_ms);

        if (diff > conflictWindow * 1000) break;

        if (
          candidate.transition.to !== base.transition.to &&
          candidate.source_service !== base.source_service
        ) {
          cluster.push(candidate);
        }
      }

      if (cluster.length > 1) {
        const winner = chooseWinner(cluster);

        for (const e of cluster) {
          if (e.event_id !== winner.event_id && survivors.has(e.event_id)) {
            survivors.delete(e.event_id);

            tombstones.push({
              event_id: e.event_id,
              workflow_id,
              status: "SUPERSEDED",
              superseded_by: winner.event_id,
              original_transition: e.transition,
            });

            conflictsResolved++;
          }
        }
      }
    }
  }

  const finalEvents = filtered
    .filter((e) => survivors.has(e.event_id))
    .sort((a, b) => a.timestamp_ms - b.timestamp_ms);

  const gaps: Gap[] = [];

  for (let i = 0; i < finalEvents.length - 1; i++) {
    const a = finalEvents[i]!;
    const b = finalEvents[i + 1]!;

    if (a.transition.to !== b.transition.from) {
      const gapTime = b.timestamp;

      gaps.push({
        type: "GAP",
        workflow_id,
        expected_from: a.transition.to,
        actual_from: b.transition.from,
        between_events: [a.event_id, b.event_id],
        timestamp: gapTime,
      });
    }
  }

  return {
    keptEvents: finalEvents,
    tombstones,
    gaps,
    duplicatesRemoved,
    noOpsRemoved,
    conflictsResolved,
  };
}

async function* reconcile_events(
  file_path: string,
  conflict_window_seconds: number,
  dedup_strategy: "sequence" | "timestamp",
): AsyncGenerator<OutputRecord> {
  const workflows = new Map<string, Event[]>();
  const malformedCounts = new Map<string, number>();

  const rl = createInterface({
    input: createReadStream(file_path),
  });

  for await (const line of rl) {
    try {
      const parsed = JSON.parse(line.trim());

      if (!isValidEvent(parsed)) {
        const wf = parsed?.workflow_id ?? "__global__";

        malformedCounts.set(wf, (malformedCounts.get(wf) ?? 0) + 1);

        malformedCounts.set(
          "__global__",
          (malformedCounts.get("__global__") ?? 0) + 1,
        );

        continue;
      }

      const wf = parsed.workflow_id;

      if (!workflows.has(wf)) workflows.set(wf, []);

      workflows.get(wf)!.push(parsed);
    } catch {
      malformedCounts.set(
        "__global__",
        (malformedCounts.get("__global__") ?? 0) + 1,
      );
      continue;
    }
  }

  const orderedWorkflows = [...workflows.keys()].sort();

  for (const wf of orderedWorkflows) {
    const events = workflows.get(wf)!;

    const {
      keptEvents,
      tombstones,
      gaps,
      duplicatesRemoved,
      noOpsRemoved,
      conflictsResolved,
    } = processWorkflow(events, conflict_window_seconds, dedup_strategy, wf);

    const orderedTimeline: OutputRecord[] = [];

    const tombstonesByWinner = new Map<string, Tombstone[]>();

    for (const t of tombstones) {
      if (!tombstonesByWinner.has(t.superseded_by))
        tombstonesByWinner.set(t.superseded_by, []);

      tombstonesByWinner.get(t.superseded_by)!.push(t);
    }

    for (const e of keptEvents) {
      orderedTimeline.push(e);

      const t = tombstonesByWinner.get(e.event_id);
      if (t) orderedTimeline.push(...t);
    }

    const merged = [...orderedTimeline, ...gaps].sort((a, b) => {
      const ta =
        "timestamp" in a ? Date.parse(a.timestamp) : Number.MAX_SAFE_INTEGER;
      const tb =
        "timestamp" in b ? Date.parse(b.timestamp) : Number.MAX_SAFE_INTEGER;
      return ta - tb;
    });

    for (const record of merged) {
      if ("timestamp_ms" in record) {
        delete (record as any).timestamp_ms;
      }
      yield record;
    }

    const summary: Summary = {
      type: "SUMMARY",
      workflow_id: wf,
      no_ops_removed: noOpsRemoved,
      total_events_processed: events.length,
      total_conflicts_resolved: conflictsResolved,
      total_duplicates_removed: duplicatesRemoved,
      total_malformed_skipped: malformedCounts.get(wf) ?? 0,
      transition_chain: buildTransitionChain(keptEvents),
      gaps_detected: gaps.length,
    };

    yield summary;
  }

  if (malformedCounts.has("__global__")) {
    yield {
      type: "SUMMARY",
      workflow_id: "__global__",
      no_ops_removed: 0,
      total_events_processed: 0,
      total_conflicts_resolved: 0,
      total_duplicates_removed: 0,
      total_malformed_skipped: malformedCounts.get("__global__")!,
      transition_chain: [],
      gaps_detected: 0,
    };
  }
}

export { reconcile_events };
