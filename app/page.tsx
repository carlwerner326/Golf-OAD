import { getDb } from "../lib/db";
import { Golfer, Tournament, User } from "./components/forms";

function formatMoney(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  }).format(value);
}

function formatDateRange(start: string, end: string) {
  return `${start} → ${end}`;
}

function getStatus(start: string, end: string, today: string) {
  if (today < start) return "Upcoming";
  if (today > end) return "Completed";
  return "Live";
}

export default function Page() {
  const db = getDb();
  const users = db.prepare("SELECT id, name FROM users ORDER BY name").all() as User[];
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

  const picks = db
    .prepare(
      `SELECT picks.id, users.name as user_name, golfers.name as golfer_name, tournaments.name as tournament_name
       FROM picks
       JOIN users ON users.id = picks.user_id
       JOIN golfers ON golfers.id = picks.golfer_id
       JOIN tournaments ON tournaments.id = picks.tournament_id
       ORDER BY tournaments.start_date, users.name`
    )
    .all() as { id: number; user_name: string; golfer_name: string; tournament_name: string }[];

  const usedGolfersByUser = Object.fromEntries(
    users.map((user) => {
      const used = db
        .prepare("SELECT golfer_id FROM picks WHERE user_id = ?")
        .all(user.id) as { golfer_id: number }[];
      return [user.id, used.map((row) => row.golfer_id)];
    })
  );

  const leaderboard = db
    .prepare(
      `SELECT users.id, users.name,
        COALESCE(SUM(results.purse), 0) as total,
        SUM(CASE WHEN results.position = 1 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN results.position IS NOT NULL AND results.position <= 5 THEN 1 ELSE 0 END) as top5,
        SUM(CASE WHEN results.position IS NOT NULL AND results.position <= 10 THEN 1 ELSE 0 END) as top10
       FROM users
       LEFT JOIN picks ON picks.user_id = users.id
       LEFT JOIN results ON results.tournament_id = picks.tournament_id AND results.golfer_id = picks.golfer_id
       GROUP BY users.id
       ORDER BY total DESC, users.name ASC`
    )
    .all() as { id: number; name: string; total: number; wins: number; top5: number; top10: number }[];

  const results = db
    .prepare(
      `SELECT tournaments.name as tournament_name, golfers.name as golfer_name, results.purse, results.position
       FROM results
       JOIN tournaments ON tournaments.id = results.tournament_id
       JOIN golfers ON golfers.id = results.golfer_id
       ORDER BY tournaments.start_date, results.purse DESC`
    )
    .all() as { tournament_name: string; golfer_name: string; purse: number; position: number | null }[];

  const today = new Date().toISOString().slice(0, 10);
  const currentTournament = tournaments.find(
    (tournament) => today >= tournament.start_date && today <= tournament.end_date
  );

  return (
    <>
      <header>
        <h1>Golf One &amp; Done Pool</h1>
        <p>Season-long picks with cumulative purse winnings. Week 1 started at WM Phoenix Open.</p>
        <p>
          Read-only view. Admin tools: <a href="/admin">manage pool data</a>
        </p>
      </header>

      <section className="section">
        <h2>Pool Dashboard</h2>
        <div className="grid grid-2">
          <div>
            <h3>Leaderboard</h3>
            <table className="table">
              <thead>
                <tr>
                  <th>Player</th>
                  <th>Total Purse</th>
                  <th>Wins</th>
                  <th>Top 5</th>
                  <th>Top 10</th>
                </tr>
              </thead>
              <tbody>
                {leaderboard.map((row) => (
                  <tr key={row.id}>
                    <td>{row.name}</td>
                    <td>{formatMoney(row.total)}</td>
                    <td>{row.wins}</td>
                    <td>{row.top5}</td>
                    <td>{row.top10}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div>
            <h3>Current Tournament</h3>
            {currentTournament ? (
              <div className="notice">
                <strong>{currentTournament.name}</strong>
                <div>{formatDateRange(currentTournament.start_date, currentTournament.end_date)}</div>
                <div className="badge">Live</div>
              </div>
            ) : (
              <div className="notice">No tournament in progress today ({today}).</div>
            )}
          </div>
        </div>
      </section>

      <section className="section">
        <h2>Weekly Picks</h2>
        <div>
          <h3>All Picks</h3>
          <table className="table">
            <thead>
              <tr>
                <th>Tournament</th>
                <th>Player</th>
                <th>Golfer</th>
              </tr>
            </thead>
            <tbody>
              {picks.map((pick) => (
                <tr key={pick.id}>
                  <td>{pick.tournament_name}</td>
                  <td>{pick.user_name}</td>
                  <td>{pick.golfer_name}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="section">
        <h2>Tournament Management</h2>
        <div>
          <table className="table">
            <thead>
              <tr>
                <th>Event</th>
                <th>Dates</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {tournaments.map((tournament) => (
                <tr key={tournament.id}>
                  <td>
                    {tournament.name}
                    {tournament.is_major ? <span className="badge major">Major</span> : null}
                    {tournament.is_signature ? <span className="badge signature">Signature</span> : null}
                  </td>
                  <td>{formatDateRange(tournament.start_date, tournament.end_date)}</td>
                  <td>{getStatus(tournament.start_date, tournament.end_date, today)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="section">
        <h2>Player Roster</h2>
        <div>
          <table className="table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Golfer</th>
                <th>FedExCup Pts</th>
              </tr>
            </thead>
            <tbody>
              {golfers.map((golfer) => (
                <tr key={golfer.id}>
                  <td>{golfer.fedex_rank ?? "—"}</td>
                  <td>{golfer.name}</td>
                  <td>{golfer.fedex_points ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="section">
        <h2>Results &amp; Leaderboard</h2>
        <div>
            <table className="table">
              <thead>
                <tr>
                  <th>Tournament</th>
                  <th>Golfer</th>
                  <th>Purse</th>
                  <th>Finish</th>
                </tr>
              </thead>
              <tbody>
                {results.map((result, index) => (
                  <tr key={`${result.tournament_name}-${result.golfer_name}-${index}`}>
                    <td>{result.tournament_name}</td>
                    <td>{result.golfer_name}</td>
                    <td>{formatMoney(result.purse)}</td>
                    <td>{result.position ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
        </div>
      </section>
    </>
  );
}
