import { getDb } from "../../lib/db";
import {
  AddGolferForm,
  AddTournamentForm,
  BulkGolferForm,
  BulkResultForm,
  Golfer,
  PickForm,
  ResultForm,
  Tournament,
  User
} from "../components/forms";
import { syncResultsFromBdl } from "../actions";

export default function AdminPage() {
  const db = getDb();
  const golfers = db
    .prepare(
      "SELECT id, name, fedex_rank, fedex_points FROM golfers WHERE active = 1 ORDER BY fedex_rank IS NULL, fedex_rank ASC, name ASC"
    )
    .all() as Golfer[];
  const tournaments = db
    .prepare(
      "SELECT id, name, start_date, end_date, is_major, is_signature FROM tournaments ORDER BY start_date"
    )
    .all() as Tournament[];

  const users = db.prepare("SELECT id, name FROM users ORDER BY name").all() as User[];
  const usedGolfersByUser = Object.fromEntries(
    users.map((user) => {
      const used = db
        .prepare("SELECT golfer_id FROM picks WHERE user_id = ?")
        .all(user.id) as { golfer_id: number }[];
      return [user.id, used.map((row) => row.golfer_id)];
    })
  );

  return (
    <>
      <header>
        <h1>Admin</h1>
        <p>Admin owner: Carl. All pool changes should be made here.</p>
      </header>

      <section className="section">
        <h2>Weekly Picks</h2>
        <div className="split">
          <PickForm users={users} golfers={golfers} tournaments={tournaments} usedGolfersByUser={usedGolfersByUser} />
          <div className="notice">
            Picks lock per user per tournament, and golfers cannot be reused by the same user.
          </div>
        </div>
      </section>

      <section className="section">
        <h2>Results</h2>
        <div className="split">
          <ResultForm tournaments={tournaments} golfers={golfers} />
          <form className="form" action={syncResultsFromBdl}>
            <label>BallDontLie Sync</label>
            <p className="helper">
              Requires `BDL_API_KEY`. This will pull completed tournament results and update purse/finish positions.
            </p>
            <button type="submit" className="secondary">
              Sync Results Now
            </button>
          </form>
        </div>
      </section>

      <section className="section">
        <h2>Roster &amp; Tournaments</h2>
        <div className="split">
          <AddGolferForm />
          <AddTournamentForm />
        </div>
      </section>

      <section className="section">
        <h2>Bulk Import</h2>
        <div className="split">
          <BulkGolferForm />
          <BulkResultForm />
        </div>
      </section>

      <section className="section">
        <h2>Reference Lists</h2>
        <div className="split">
          <div>
            <h3>Tournaments</h3>
            <table className="table">
              <thead>
                <tr>
                  <th>Event</th>
                  <th>Dates</th>
                </tr>
              </thead>
              <tbody>
                {tournaments.map((tournament) => (
                  <tr key={tournament.id}>
                    <td>{tournament.name}</td>
                    <td>
                      {tournament.start_date} → {tournament.end_date}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div>
            <h3>Golfers</h3>
            <table className="table">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Name</th>
                </tr>
              </thead>
              <tbody>
                {golfers.map((golfer) => (
                  <tr key={golfer.id}>
                    <td>{golfer.fedex_rank ?? "—"}</td>
                    <td>{golfer.name}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>
    </>
  );
}
