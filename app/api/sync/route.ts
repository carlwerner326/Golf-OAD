import { NextResponse } from "next/server";
import { syncResultsFromBdlCore } from "../../../lib/sync";

export async function POST(request: Request) {
  const token = process.env.SYNC_TOKEN;
  if (token) {
    const auth = request.headers.get("x-sync-token");
    if (auth !== token) {
      return NextResponse.json({ ok: false, message: "Unauthorized" }, { status: 401 });
    }
  }

  try {
    const { updatedResults } = await syncResultsFromBdlCore();
    return NextResponse.json({ ok: true, updatedResults });
  } catch (error) {
    return NextResponse.json({ ok: false, message: (error as Error).message }, { status: 500 });
  }
}
