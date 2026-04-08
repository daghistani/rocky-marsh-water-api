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

  function pick(row, keys) {
    for (const key of keys) {
      if (row[key] !== undefined && row[key] !== null && row[key] !== "") {
        return row[key];
      }
    }
    return "";
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
      return res.status(502).json({ error: `Upstream request failed (${upstream.status})` });
    }

    const csvText = await upstream.text();
    const rows = parseCsv(csvText);

    const cleaned = rows.map((r) => {
      const characteristic = pick(r, [
        "CharacteristicName",
        "Characteristic Name",
        "USGSPCodeName",
        "ObservedPropertyName"
      ]);

      const rawValue = pick(r, [
        "ResultMeasureValue",
        "Result Measure Value",
        "ResultValue",
        "MeasureValue"
      ]);

      const value = toNumber(rawValue);
      if (value === null) return null;

      const rawDate = pick(r, [
        "ActivityStartDate",
        "Activity Start Date",
        "ActivityStartDateTime",
        "ResultDate"
      ]);

      const date = new Date(rawDate);
      if (Number.isNaN(date.getTime())) return null;

      const unit = pick(r, [
        "ResultMeasure/MeasureUnitCode",
        "Result Measure/Measure Unit Code",
        "MeasureUnitCode",
        "ResultUnit"
      ]);

      return {
        characteristic: characteristic || "Unknown",
        value,
        unit,
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
      return {
        characteristic,
        latest: values[0],
        count: values.length
      };
    });

    parameters.sort((a, b) => new Date(b.latest.date) - new Date(a.latest.date));

    return res.status(200).json({
      site: SITE,
      rowCount: rows.length,
      sampleCount: cleaned.length,
      parameterCount: parameters.length,
      parameters
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown server error"
    });
  }
}
