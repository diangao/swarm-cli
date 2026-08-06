// This committed defect is read only by `check:boundaries:negative`.
import { readFile } from "node:fs";
import type { StorageRecord } from "@swarm/storage";

export type SeededBoundaryViolation = StorageRecord;
void readFile;
