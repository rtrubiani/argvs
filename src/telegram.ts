// ---------------------------------------------------------------------------
// Fire-and-forget Telegram notifications
// ---------------------------------------------------------------------------

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const ENABLED = Boolean(BOT_TOKEN && CHAT_ID);

const PATHUSD_CONTRACT = "0x20c0000000000000000000000000000000000000";
const TEMPO_RPC = "https://rpc.tempo.xyz";

function send(text: string): void {
  if (!ENABLED) return;
  fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: CHAT_ID,
      text,
      parse_mode: "HTML",
    }),
  }).catch(() => {}); // swallow errors silently
}

async function getWalletBalance(): Promise<string> {
  const wallet = process.env.TEMPO_WALLET_ADDRESS;
  if (!wallet) return "unavailable";
  try {
    const paddedAddress = "0x70a08231" + wallet.slice(2).toLowerCase().padStart(64, "0");
    const res = await fetch(TEMPO_RPC, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "eth_call",
        params: [{ to: PATHUSD_CONTRACT, data: paddedAddress }, "latest"],
      }),
    });
    const json = (await res.json()) as { result?: string };
    if (!json.result) return "unavailable";
    const raw = BigInt(json.result);
    const dollars = Number(raw) / 1e6;
    return `$${dollars.toFixed(2)} pathUSD`;
  } catch {
    return "unavailable";
  }
}

export function notifyScreen(opts: {
  query: string;
  riskLevel: string;
  confidence: number;
  endpoint: string;
  payment: string;
}): void {
  getWalletBalance().then((balance) => {
    send(
      `🔍 <b>ARGVS Screen</b>\n` +
        `Query: ${opts.query}\n` +
        `Risk: ${opts.riskLevel} (${opts.confidence}%)\n` +
        `Endpoint: ${opts.endpoint}\n` +
        `Payment: ${opts.payment}\n` +
        `Balance: ${balance}\n` +
        `Time: ${new Date().toISOString()}`
    );
  }).catch(() => {}); // swallow errors silently
}

export function notifyBatch(opts: {
  entityCount: number;
  flaggedCount: number;
  payment: string;
}): void {
  getWalletBalance().then((balance) => {
    send(
      `📋 <b>ARGVS Batch</b>\n` +
        `Entities: ${opts.entityCount}\n` +
        `Flagged: ${opts.flaggedCount}\n` +
        `Payment: ${opts.payment}\n` +
        `Balance: ${balance}\n` +
        `Time: ${new Date().toISOString()}`
    );
  }).catch(() => {}); // swallow errors silently
}
