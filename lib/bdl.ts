const BDL_BASE = "https://api.balldontlie.io/pga/v1";

export type BdlTournament = {
  id: number;
  name: string;
  start_date: string;
  end_date: string;
  season: number;
  status: string;
};

export type BdlPlayer = {
  id: number;
  display_name: string;
};

export type BdlTournamentResult = {
  tournament_id: number;
  player: BdlPlayer;
  earnings: number | null;
  position_numeric: number | null;
};

type BdlResponse<T> = {
  data: T[];
  meta?: { next_cursor?: string | null };
};

function getApiKey() {
  const apiKey = process.env.BDL_API_KEY;
  if (!apiKey) {
    throw new Error("Missing BDL_API_KEY environment variable.");
  }
  return apiKey;
}

export async function bdlFetch<T>(path: string, params?: Record<string, string | number | undefined>) {
  const apiKey = getApiKey();
  const url = new URL(`${BDL_BASE}${path}`);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined) return;
      url.searchParams.set(key, String(value));
    });
  }

  const response = await fetch(url.toString(), {
    headers: {
      Authorization: apiKey
    },
    cache: "no-store"
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(`BDL API error (${response.status}): ${message}`);
  }

  return (await response.json()) as BdlResponse<T>;
}

export async function fetchAll<T>(path: string, params?: Record<string, string | number | undefined>) {
  const all: T[] = [];
  let cursor: string | null | undefined = undefined;

  do {
    const response = await bdlFetch<T>(path, { ...params, cursor: cursor ?? undefined, per_page: 100 });
    all.push(...response.data);
    cursor = response.meta?.next_cursor;
  } while (cursor);

  return all;
}
