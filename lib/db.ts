import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import { seedGolfers, seedTournaments, seedUsers } from "./seed-data";

let db: Database.Database | null = null;

function ensureDir(dir: string) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function initDb(database: Database.Database) {
  database.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS golfers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      fedex_rank INTEGER,
      fedex_points INTEGER,
      active INTEGER NOT NULL DEFAULT 1,
      bdl_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS tournaments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      start_date TEXT NOT NULL,
      end_date TEXT NOT NULL,
      is_major INTEGER NOT NULL DEFAULT 0,
      is_signature INTEGER NOT NULL DEFAULT 0,
      season INTEGER NOT NULL DEFAULT 2026,
      bdl_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS picks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      tournament_id INTEGER NOT NULL,
      golfer_id INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY(tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
      FOREIGN KEY(golfer_id) REFERENCES golfers(id) ON DELETE CASCADE,
      UNIQUE(user_id, tournament_id),
      UNIQUE(user_id, golfer_id)
    );

    CREATE TABLE IF NOT EXISTS results (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tournament_id INTEGER NOT NULL,
      golfer_id INTEGER NOT NULL,
      purse INTEGER NOT NULL,
      position INTEGER,
      FOREIGN KEY(tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
      FOREIGN KEY(golfer_id) REFERENCES golfers(id) ON DELETE CASCADE,
      UNIQUE(tournament_id, golfer_id)
    );
  `);

  const addColumnIfMissing = (table: string, column: string, type: string) => {
    const columns = database.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[];
    if (columns.some((col) => col.name === column)) return;
    database.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`);
  };

  addColumnIfMissing("golfers", "bdl_id", "INTEGER");
  addColumnIfMissing("tournaments", "bdl_id", "INTEGER");
  addColumnIfMissing("results", "position", "INTEGER");

  const userCount = database.prepare("SELECT COUNT(*) as count FROM users").get() as { count: number };
  if (userCount.count === 0) {
    const insertUser = database.prepare("INSERT INTO users (name) VALUES (?)");
    const insertUsers = database.transaction(() => {
      seedUsers.forEach((name) => insertUser.run(name));
    });
    insertUsers();
  }

  const golferCount = database.prepare("SELECT COUNT(*) as count FROM golfers").get() as { count: number };
  if (golferCount.count === 0) {
    const insertGolfer = database.prepare(
      "INSERT INTO golfers (name, fedex_rank, fedex_points) VALUES (?, ?, ?)"
    );
    const insertGolfers = database.transaction(() => {
      seedGolfers.forEach((golfer) => {
        insertGolfer.run(golfer.name, golfer.fedexRank, golfer.fedexPoints);
      });
    });
    insertGolfers();
  }

  const tournamentCount = database
    .prepare("SELECT COUNT(*) as count FROM tournaments")
    .get() as { count: number };
  if (tournamentCount.count === 0) {
    const insertTournament = database.prepare(
      `INSERT INTO tournaments (name, start_date, end_date, is_major, is_signature, season)
       VALUES (?, ?, ?, ?, ?, 2026)`
    );
    const insertTournaments = database.transaction(() => {
      seedTournaments.forEach((tournament) => {
        insertTournament.run(
          tournament.name,
          tournament.startDate,
          tournament.endDate,
          tournament.isMajor ? 1 : 0,
          tournament.isSignature ? 1 : 0
        );
      });
    });
    insertTournaments();
  }

  const picksCount = database.prepare("SELECT COUNT(*) as count FROM picks").get() as { count: number };
  if (picksCount.count === 0) {
    const tournament = database
      .prepare("SELECT id FROM tournaments WHERE name = ?")
      .get("WM Phoenix Open") as { id?: number } | undefined;

    if (tournament?.id) {
      const golferByName = database.prepare("SELECT id FROM golfers WHERE name = ?");
      const userByName = database.prepare("SELECT id FROM users WHERE name = ?");

      const golfers = {
        siWoo: golferByName.get("Si Woo Kim") as { id?: number } | undefined,
        maverick: golferByName.get("Maverick McNealy") as { id?: number } | undefined,
        cameron: golferByName.get("Cameron Young") as { id?: number } | undefined,
        sahith: golferByName.get("Sahith Theegala") as { id?: number } | undefined
      };

      const users = {
        jacob: userByName.get("Jacob") as { id?: number } | undefined,
        carl: userByName.get("Carl") as { id?: number } | undefined,
        cade: userByName.get("Cade") as { id?: number } | undefined,
        aj: userByName.get("AJ") as { id?: number } | undefined,
        jordan: userByName.get("Jordan") as { id?: number } | undefined,
        vossy: userByName.get("Vossy") as { id?: number } | undefined
      };

      const insertResult = database.prepare(
        "INSERT OR IGNORE INTO results (tournament_id, golfer_id, purse) VALUES (?, ?, ?)"
      );

      if (golfers.siWoo?.id) insertResult.run(tournament.id, golfers.siWoo.id, 439680);
      if (golfers.maverick?.id) insertResult.run(tournament.id, golfers.maverick.id, 188000);
      if (golfers.cameron?.id) insertResult.run(tournament.id, golfers.cameron.id, 34080);
      if (golfers.sahith?.id) insertResult.run(tournament.id, golfers.sahith.id, 122720);

      const insertPick = database.prepare(
        "INSERT OR IGNORE INTO picks (user_id, tournament_id, golfer_id, created_at) VALUES (?, ?, ?, ?)"
      );
      const now = new Date().toISOString();

      if (users.jacob?.id && golfers.siWoo?.id) {
        insertPick.run(users.jacob.id, tournament.id, golfers.siWoo.id, now);
      }
      if (users.carl?.id && golfers.maverick?.id) {
        insertPick.run(users.carl.id, tournament.id, golfers.maverick.id, now);
      }
      if (users.cade?.id && golfers.maverick?.id) {
        insertPick.run(users.cade.id, tournament.id, golfers.maverick.id, now);
      }
      if (users.aj?.id && golfers.cameron?.id) {
        insertPick.run(users.aj.id, tournament.id, golfers.cameron.id, now);
      }
      if (users.jordan?.id && golfers.sahith?.id) {
        insertPick.run(users.jordan.id, tournament.id, golfers.sahith.id, now);
      }
      if (users.vossy?.id && golfers.cameron?.id) {
        insertPick.run(users.vossy.id, tournament.id, golfers.cameron.id, now);
      }
    }
  }
}

export function getDb() {
  if (db) return db;
  const dataDir = path.join(process.cwd(), "data");
  ensureDir(dataDir);
  const dbPath = path.join(dataDir, "golf.db");
  const database = new Database(dbPath);
  initDb(database);
  db = database;
  return database;
}
