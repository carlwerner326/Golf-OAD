import { getDb } from "./db";
import { BdlTournament, BdlTournamentResult, fetchAll } from "./bdl";

function normalizeName(value: string) {
  return value
    .toLowerCase()
    .replace(/presented by.*$/i, "")
    .replace(/sponsored by.*$/i, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export async function syncResultsFromBdlCore() {
  const db = getDb();
  const tournaments = db
    .prepare("SELECT id, name, start_date, end_date, bdl_id FROM tournaments WHERE season = 2026")
    .all() as { id: number; name: string; start_date: string; end_date: string; bdl_id: number | null }[];

  const bdlTournaments = await fetchAll<BdlTournament>("/tournaments", {
    season: 2026
  });

  const tournamentByNormalized = new Map(
    bdlTournaments.map((tournament) => [normalizeName(tournament.name), tournament])
  );

  const updateTournament = db.prepare("UPDATE tournaments SET bdl_id = ? WHERE id = ?");
  tournaments.forEach((tournament) => {
    if (tournament.bdl_id) return;
    const normalized = normalizeName(tournament.name);
    const match = tournamentByNormalized.get(normalized);
    if (match) {
      updateTournament.run(match.id, tournament.id);
    }
  });

  const updatedTournaments = db
    .prepare("SELECT id, name, bdl_id FROM tournaments WHERE season = 2026 AND bdl_id IS NOT NULL")
    .all() as { id: number; name: string; bdl_id: number }[];

  const golfers = db
    .prepare("SELECT id, name, bdl_id FROM golfers WHERE active = 1")
    .all() as { id: number; name: string; bdl_id: number | null }[];
  const golferByName = new Map(golfers.map((golfer) => [normalizeName(golfer.name), golfer]));

  const golferUpdate = db.prepare("UPDATE golfers SET bdl_id = ? WHERE id = ?");

  const resultsUpsert = db.prepare(
    "INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)\n     ON CONFLICT(tournament_id, golfer_id) DO UPDATE SET purse = excluded.purse, position = excluded.position"
  );

  let updatedResults = 0;
  for (const tournament of updatedTournaments) {
    const bdlResults = await fetchAll<BdlTournamentResult>("/tournament_results", {
      tournament_ids: tournament.bdl_id
    });

    bdlResults.forEach((result) => {
      const displayName = normalizeName(result.player.display_name);
      const golfer = golferByName.get(displayName);
      if (!golfer) return;
      if (!golfer.bdl_id) golferUpdate.run(result.player.id, golfer.id);

      const purse = result.earnings ?? 0;
      const position = result.position_numeric ?? null;
      resultsUpsert.run(tournament.id, golfer.id, Math.round(purse), position);
      updatedResults += 1;
    });
  }

  return { updatedResults };
}
