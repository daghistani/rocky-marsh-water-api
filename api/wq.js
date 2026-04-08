export default async function handler(req, res) {
  const SITE = "USGS-01618100";

  // Drop obvious admin / non-display / metadata-style fields
  const EXCLUDE_EXACT = new Set([
    "NWIS lot number",
    "Sample location",
    "Height, gage",
    "Barometric pressure"
  ]);

  const EXCLUDE_CONTAINS = [
    "lot number",
    "gage",
    "sample location",
    "barometric pressure"
  ];

  function shouldKeepCharacteristic(name) {
    if (!name) return false;

    const normalized = String(name).trim();
    const lower = normalized.toLowerCase();

    if (EXCLUDE_EXACT.has(normalized)) return false;
    if (EXCLUDE_CONTAINS.some(term => lower.includes(term))) return false;

    return true;
  }

  function inferGroup(name) {
    const n = String(name || "").toLowerCase();

    if (
      n.includes("nitrate") ||
      n.includes("nitrite") ||
      n.includes("nitrogen") ||
      n.includes("phosphorus") ||
      n.includes("orthophosphate") ||
      n.includes("ammonia")
    ) return "nutrient";

    if (
      n.includes("sediment") ||
      n.includes("turbidity")
    ) return "sediment";

    if (
      n.includes("temperature") ||
      n.includes("conductance") ||
      n === "ph" ||
      n.includes("dissolved oxygen") ||
      n.includes("oxygen")
    ) return "physical";

    return "other";
  }

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
    if (!lines.length) return [];

    const headers = parseCsvLine(lines[0]).map(h => h.trim());

    return lines.slice(1).map(line => {
      const cols = parseCsvLine(line);
      const row = {};
      headers.forEach((h, i) => {
        row[h] = cols[i] ?? "";
      });
      return row;
    });
  }

  function toNumber(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(String(v).replace(/,/g, "").trim());
    return Number.isFinite(n) ? n : null;
  }

  try {
    const url =
      `https://www.waterqualitydata.us/wqx3/Result/search?siteid=${SITE}&mimeType=csv&dataProfile=fullPhysChem`;

    const upstream = await fetch(url, { cache: "no-store" });

    if (!upstream.ok) {
      return res.status(502).json({ error: `Upstream failed (${upstream.status})` });
    }

    const csvText = await upstream.text();
    const rows = parseCsv(csvText);

    const cleaned = rows.map(r => {
      const characteristic = (r["Result_Characteristic"] || "").trim();
      if (!shouldKeepCharacteristic(characteristic)) return null;

      const value = toNumber(r["Result_Measure"]);
      if (value === null) return null;

      const rawDate = r["Activity_StartDate"];
      const date = new Date(rawDate);
      if (isNaN(date)) return null;

      return {
        characteristic,
        group: inferGroup(characteristic),
        value,
        unit: (r["Result_MeasureUnit"] || "").trim(),
        date: date.toISOString()
      };
    }).filter(Boolean);

    const grouped = {};
    for (const row of cleaned) {
      if (!grouped[row.characteristic]) grouped[row.characteristic] = [];
      grouped[row.characteristic].push(row);
    }

    const parameters = Object.entries(grouped).map(([characteristic, values]) => {
      values.sort((a, b) => new Date(b.date) - new Date(a.date));

      const latest = values[0];
      const group = latest.group;

      return {
        characteristic,
        group,
        latest,
        count: values.length,
        series: values
      };
    });

    parameters.sort((a, b) => new Date(b.latest.date) - new Date(a.latest.date));

    res.status(200).json({
      site: SITE,
      sampleCount: cleaned.length,
      parameterCount: parameters.length,
      parameters
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
