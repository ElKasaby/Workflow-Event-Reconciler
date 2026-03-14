*Workflow Event Reconciler*

This project implements a workflow event reconciliation system that processes raw workflow events produced by distributed services and reconstructs a clean, ordered, and reliable state transition history.

The system handles common distributed system issues such as duplicate events, out-of-order events, and conflicting transitions.

*Features*

Parse workflow events from a JSON stream

Remove duplicate events

Handle out-of-order event delivery

Detect conflicting transitions

Reconstruct a consistent workflow history

Ignore corrupted or invalid events safely

*Assumptions*

Events may arrive in any order

Duplicate events may occur due to at-least-once delivery

Conflicts happen when two events share the same form state but lead to different states

The dataset can fit into memory for this exercise

*How to Run*
npm install
node --loader ts-node/esm run.ts test_events_120.ndjson 10 sequence > my_output.jsonl
