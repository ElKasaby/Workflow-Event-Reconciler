import { reconcile_events } from "./reconcile.js";

async function main() {
  const generator = reconcile_events(
    "./events.ndjson",
    5,
    "sequence"
  );

  for await (const record of generator) {
    console.log(JSON.stringify(record, null, 2));
  }
}

main();
