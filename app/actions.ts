"use server";

import { revalidatePath } from "next/cache";
import { getDb } from "../lib/db";
import { syncResultsFromBdlCore } from "../lib/sync";

export type ActionState = {
  ok: boolean;
  message: string | null;
};

const defaultState: ActionState = { ok: true, message: null };

function normalizeCurrency(value: string) {
  const cleaned = value.replace(/[^0-9.-]/g, "");
  const parsed = Number(cleaned);
  if (Number.isNaN(parsed)) return null;
  return Math.round(parsed);
}

export async function addPick(_: ActionState, formData: FormData): Promise<ActionState> {
  const userId = Number(formData.get("userId"));
  const tournamentId = Number(formData.get("tournamentId"));
  const golferId = Number(formData.get("golferId"));

  if (!userId || !tournamentId || !golferId) {
    return { ok: false, message: "Pick requires user, tournament, and golfer." };
  }

  const db = getDb();
  const existingPick = db
    .prepare("SELECT id FROM picks WHERE user_id = ? AND tournament_id = ?")
    .get(userId, tournamentId) as { id?: number } | undefined;
  if (existingPick?.id) {
    return { ok: false, message: "That user already has a pick for this tournament." };
  }

  const usedGolfer = db
    .prepare("SELECT id FROM picks WHERE user_id = ? AND golfer_id = ?")
    .get(userId, golferId) as { id?: number } | undefined;
  if (usedGolfer?.id) {
    return { ok: false, message: "That user already used this golfer." };
  }

  db.prepare(
    "INSERT INTO picks (user_id, tournament_id, golfer_id, created_at) VALUES (?, ?, ?, ?)"
  ).run(userId, tournamentId, golferId, new Date().toISOString());

  revalidatePath("/");
  return { ok: true, message: "Pick saved." };
}

export async function addResult(_: ActionState, formData: FormData): Promise<ActionState> {
  const tournamentId = Number(formData.get("resultTournamentId"));
  const golferId = Number(formData.get("resultGolferId"));
  const purseRaw = String(formData.get("purse") || "");
  const positionRaw = String(formData.get("position") || "");
  const purse = normalizeCurrency(purseRaw);
  const position = positionRaw ? Number(positionRaw) : null;

  if (!tournamentId || !golferId || purse === null) {
    return { ok: false, message: "Result requires tournament, golfer, and purse." };
  }

  const db = getDb();
  const existing = db
    .prepare("SELECT id FROM results WHERE tournament_id = ? AND golfer_id = ?")
    .get(tournamentId, golferId) as { id?: number } | undefined;

  if (existing?.id) {
    db.prepare("UPDATE results SET purse = ?, position = ? WHERE id = ?").run(
      purse,
      position,
      existing.id
    );
  } else {
    db.prepare("INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)")
      .run(tournamentId, golferId, purse, position);
  }

  revalidatePath("/");
  return { ok: true, message: "Result saved." };
}

export async function addGolfer(_: ActionState, formData: FormData): Promise<ActionState> {
  const name = String(formData.get("golferName") || "").trim();
  const rank = Number(formData.get("golferRank"));
  const points = Number(formData.get("golferPoints"));

  if (!name) {
    return { ok: false, message: "Golfer name is required." };
  }

  const db = getDb();
  try {
    db.prepare("INSERT INTO golfers (name, fedex_rank, fedex_points) VALUES (?, ?, ?)")
      .run(name, Number.isNaN(rank) ? null : rank, Number.isNaN(points) ? null : points);
  } catch (error) {
    return { ok: false, message: "That golfer already exists." };
  }

  revalidatePath("/");
  return { ok: true, message: "Golfer added." };
}

export async function bulkAddGolfers(_: ActionState, formData: FormData): Promise<ActionState> {
  const raw = String(formData.get("golferBulk") || "").trim();
  if (!raw) {
    return { ok: false, message: "Paste at least one golfer." };
  }

  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length === 0) {
    return { ok: false, message: "Paste at least one golfer." };
  }

  const db = getDb();
  const insert = db.prepare(
    "INSERT OR IGNORE INTO golfers (name, fedex_rank, fedex_points) VALUES (?, ?, ?)"
  );

  const insertMany = db.transaction(() => {
    lines.forEach((line) => {
      const parts = line.split(",").map((part) => part.trim());
      const name = parts[0];
      if (!name) return;
      const rank = parts[1] ? Number(parts[1]) : null;
      const points = parts[2] ? Number(parts[2]) : null;
      insert.run(name, Number.isNaN(rank) ? null : rank, Number.isNaN(points) ? null : points);
    });
  });

  insertMany();
  revalidatePath("/");
  revalidatePath("/admin");
  return { ok: true, message: `Imported ${lines.length} golfer lines.` };
}

export async function bulkAddResults(_: ActionState, formData: FormData): Promise<ActionState> {
  const raw = String(formData.get("resultBulk") || "").trim();
  if (!raw) {
    return { ok: false, message: "Paste at least one result line." };
  }

  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length === 0) {
    return { ok: false, message: "Paste at least one result line." };
  }

  const db = getDb();
  const tournamentByName = db.prepare("SELECT id FROM tournaments WHERE name = ?");
  const golferByName = db.prepare("SELECT id FROM golfers WHERE name = ?");
  const upsert = db.prepare(
    "INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)\n     ON CONFLICT(tournament_id, golfer_id) DO UPDATE SET purse = excluded.purse, position = excluded.position"
  );

  const insertMany = db.transaction(() => {
    lines.forEach((line) => {
      const parts = line.split(",").map((part) => part.trim());
      const tournamentName = parts[0];
      const golferName = parts[1];
      const purse = parts[2] ? Number(parts[2].replace(/[^0-9.-]/g, "")) : NaN;
      const position = parts[3] ? Number(parts[3]) : null;
      if (!tournamentName || !golferName || Number.isNaN(purse)) return;

      const tournament = tournamentByName.get(tournamentName) as { id?: number } | undefined;
      const golfer = golferByName.get(golferName) as { id?: number } | undefined;
      if (!tournament?.id || !golfer?.id) return;

      upsert.run(tournament.id, golfer.id, Math.round(purse), Number.isNaN(position as number) ? null : position);
    });
  });

  insertMany();
  revalidatePath("/");
  revalidatePath("/admin");
  return { ok: true, message: `Imported ${lines.length} result lines.` };
}

export async function syncResultsFromBdl(): Promise<ActionState> {
  try {
    const { updatedResults } = await syncResultsFromBdlCore();

    revalidatePath("/");
    revalidatePath("/admin");
    return { ok: true, message: `Synced ${updatedResults} results from BallDontLie.` };
  } catch (error) {
    return { ok: false, message: (error as Error).message };
  }
}

export async function addTournament(_: ActionState, formData: FormData): Promise<ActionState> {
  const name = String(formData.get("tournamentName") || "").trim();
  const startDate = String(formData.get("tournamentStart") || "").trim();
  const endDate = String(formData.get("tournamentEnd") || "").trim();
  const isMajor = formData.get("tournamentMajor") === "on";
  const isSignature = formData.get("tournamentSignature") === "on";

  if (!name || !startDate || !endDate) {
    return { ok: false, message: "Tournament needs name, start date, and end date." };
  }

  const db = getDb();
  db.prepare(
    "INSERT INTO tournaments (name, start_date, end_date, is_major, is_signature, season) VALUES (?, ?, ?, ?, ?, 2026)"
  ).run(name, startDate, endDate, isMajor ? 1 : 0, isSignature ? 1 : 0);

  revalidatePath("/");
  return { ok: true, message: "Tournament added." };
}

export { defaultState };
