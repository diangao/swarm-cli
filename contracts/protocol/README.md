# Protocol compatibility fixtures

`fixtures/positive.json` is the V0 compatibility registry. It includes all four target shapes, first/launch-fenced/replay deliveries, a lease renewal with stable fence identity, launch commands with and without a wake, every receipt kind, both legal side-effect fence shapes, and both minor-downgrade directions.

`fixtures/seeded-controls.json` names the eight mandatory implementation defects and their stable rejection categories. Tests generate the intentionally invalid wire bytes in memory so duplicate keys, malformed UTF-8, oversized payloads, and unsafe nesting never masquerade as valid JSON files.

The boundary fixtures are separate: the positive file imports the public package, while the negative seed deliberately imports two forbidden layers and is evaluated only by the negative-mode boundary command.
