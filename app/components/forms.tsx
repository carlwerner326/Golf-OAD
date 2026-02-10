"use client";

import { useMemo, useState } from "react";
import { useFormState } from "react-dom";
import {
  addGolfer,
  addPick,
  addResult,
  addTournament,
  bulkAddGolfers,
  bulkAddResults,
  defaultState
} from "../actions";

export type User = { id: number; name: string };
export type Golfer = { id: number; name: string; fedex_rank: number | null; fedex_points: number | null };
export type Tournament = {
  id: number;
  name: string;
  start_date: string;
  end_date: string;
  is_major: number;
  is_signature: number;
};

export function PickForm({
  users,
  golfers,
  tournaments,
  usedGolfersByUser
}: {
  users: User[];
  golfers: Golfer[];
  tournaments: Tournament[];
  usedGolfersByUser: Record<number, number[]>;
}) {
  const [state, formAction] = useFormState(addPick, defaultState);
  const [userId, setUserId] = useState(users[0]?.id ?? 0);

  const availableGolfers = useMemo(() => {
    const used = new Set(usedGolfersByUser[userId] || []);
    return golfers.filter((golfer) => !used.has(golfer.id));
  }, [golfers, usedGolfersByUser, userId]);

  return (
    <form className="form" action={formAction}>
      <label>Pool member</label>
      <select name="userId" value={userId} onChange={(event) => setUserId(Number(event.target.value))}>
        {users.map((user) => (
          <option key={user.id} value={user.id}>
            {user.name}
          </option>
        ))}
      </select>

      <label>Tournament</label>
      <select name="tournamentId" required>
        {tournaments.map((tournament) => (
          <option key={tournament.id} value={tournament.id}>
            {tournament.name} ({tournament.start_date})
          </option>
        ))}
      </select>

      <label>Golfer</label>
      <select name="golferId" required>
        {availableGolfers.map((golfer) => (
          <option key={golfer.id} value={golfer.id}>
            {golfer.name} {golfer.fedex_rank ? `(#${golfer.fedex_rank})` : ""}
          </option>
        ))}
      </select>

      <button type="submit">Save Pick</button>
      {state.message && <p className="helper">{state.message}</p>}
    </form>
  );
}

export function ResultForm({ tournaments, golfers }: { tournaments: Tournament[]; golfers: Golfer[] }) {
  const [state, formAction] = useFormState(addResult, defaultState);

  return (
    <form className="form" action={formAction}>
      <label>Tournament</label>
      <select name="resultTournamentId" required>
        {tournaments.map((tournament) => (
          <option key={tournament.id} value={tournament.id}>
            {tournament.name}
          </option>
        ))}
      </select>

      <label>Golfer</label>
      <select name="resultGolferId" required>
        {golfers.map((golfer) => (
          <option key={golfer.id} value={golfer.id}>
            {golfer.name}
          </option>
        ))}
      </select>

      <label>Purse Winnings (USD)</label>
      <input name="purse" placeholder="e.g. 900000" />

      <label>Finish Position (optional)</label>
      <input name="position" type="number" placeholder="e.g. 1" />

      <button type="submit" className="secondary">
        Save Result
      </button>
      {state.message && <p className="helper">{state.message}</p>}
    </form>
  );
}

export function AddGolferForm() {
  const [state, formAction] = useFormState(addGolfer, defaultState);

  return (
    <form className="form" action={formAction}>
      <label>Golfer Name</label>
      <input name="golferName" placeholder="Add a golfer" />

      <label>FedExCup Rank (optional)</label>
      <input name="golferRank" type="number" />

      <label>FedExCup Points (optional)</label>
      <input name="golferPoints" type="number" />

      <button type="submit">Add Golfer</button>
      {state.message && <p className="helper">{state.message}</p>}
    </form>
  );
}

export function AddTournamentForm() {
  const [state, formAction] = useFormState(addTournament, defaultState);

  return (
    <form className="form" action={formAction}>
      <label>Tournament Name</label>
      <input name="tournamentName" placeholder="Add a tournament" />

      <label>Start Date</label>
      <input name="tournamentStart" type="date" />

      <label>End Date</label>
      <input name="tournamentEnd" type="date" />

      <label>
        <input name="tournamentMajor" type="checkbox" /> Major
      </label>

      <label>
        <input name="tournamentSignature" type="checkbox" /> Signature
      </label>

      <button type="submit">Add Tournament</button>
      {state.message && <p className="helper">{state.message}</p>}
    </form>
  );
}

export function BulkGolferForm() {
  const [state, formAction] = useFormState(bulkAddGolfers, defaultState);

  return (
    <form className="form" action={formAction}>
      <label>Bulk Import Golfers</label>
      <textarea
        name="golferBulk"
        rows={8}
        placeholder="Name, Rank, Points&#10;Example:&#10;Scottie Scheffler,1,999&#10;Rory McIlroy,2,850"
      />
      <p className="helper">Format: Name, Rank, Points (Rank/Points optional)</p>
      <button type="submit">Import Golfers</button>
      {state.message && <p className="helper">{state.message}</p>}
    </form>
  );
}

export function BulkResultForm() {
  const [state, formAction] = useFormState(bulkAddResults, defaultState);

  return (
    <form className="form" action={formAction}>
      <label>Bulk Import Results</label>
      <textarea
        name="resultBulk"
        rows={8}
        placeholder="Tournament Name, Golfer Name, Purse, Position&#10;Example:&#10;WM Phoenix Open,Si Woo Kim,439680,1"
      />
      <p className="helper">Format: Tournament Name, Golfer Name, Purse, Position (Position optional)</p>
      <button type="submit" className="secondary">
        Import Results
      </button>
      {state.message && <p className="helper">{state.message}</p>}
    </form>
  );
}
