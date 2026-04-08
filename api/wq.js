export default async function handler(req, res) {
  const SITE = "USGS-01618100";

  function parseCsvLine(line) {
    const out = [];
    let cur = "";
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      const next = line[i + 1];

      if (ch === '"' && inQuotes && next === '"') {
        cur += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = !inQuotes;
      } else if (ch === "," && !inQuotes) {
        out.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }

    out.push(cur);
    return out;
  }

  function parseCsv(text) {
    const lines = String(text || "").trim().split(/\r?\n/).filter(Boolean);
    if (!lines.length) return { headers: [], rows: [] };

    const headers = parseCsvLine(lines[0]).map(h => h.trim());

    const rows = lines.slice(1).map(line => {
      const cols = parseCsvLine(line);
      const row = {};
      headers.forEach((h, i) => {
        row[h] = cols[i] ?? "";
      });
      return row;
    });

    return { headers, rows };
  }

  try {
    const url =
      `https://www.waterqualitydata.us/wqx3/Result/search?siteid=${SITE}&mimeType=csv&dataProfile=fullPhysChem`;

    const upstream = await fetch(url, { cache: "no-store" });

    if (!upstream.ok) {
      return res.status(502).json({ error: `Upstream request failed (${upstream.status})` });
    }

    const csvText = await upstream.text();
    const { headers, rows } = parseCsv(csvText);

    return res.status(200).json({
      site: SITE,
      rowCount: rows.length,
      headerCount: headers.length,
      headers: headers,
      firstRow: rows[0] || null
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown server error"
    });
  }
}
