// ---------------------------------------------------------------------------
// Fire-and-forget Telegram notifications
// ---------------------------------------------------------------------------

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const ENABLED = Boolean(BOT_TOKEN && CHAT_ID);

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

export function notifyScreen(opts: {
  query: string;
  riskLevel: string;
  confidence: number;
  endpoint: string;
  payment: string;
}): void {
  send(
    `🔍 <b>ARGVS Screen</b>\n` +
      `Query: ${opts.query}\n` +
      `Risk: ${opts.riskLevel} (${opts.confidence}%)\n` +
      `Endpoint: ${opts.endpoint}\n` +
      `Payment: ${opts.payment}\n` +
      `Time: ${new Date().toISOString()}`
  );
}

export function notifyBatch(opts: {
  entityCount: number;
  flaggedCount: number;
  payment: string;
}): void {
  send(
    `📋 <b>ARGVS Batch</b>\n` +
      `Entities: ${opts.entityCount}\n` +
      `Flagged: ${opts.flaggedCount}\n` +
      `Payment: ${opts.payment}\n` +
      `Time: ${new Date().toISOString()}`
  );
}
