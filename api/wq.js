export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0, s-maxage=0"
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");

  try {
    const site = "USGS-01618100";
    const sourceUrl =
      "https://www.waterqualitydata.us/wqx3/Result/search" +
      `?siteid=${encodeURIComponent(site)}` +
      "&dataProfile=basicPhysChem" +
      "&mimeType=csv" +
      "&count=no" +
      `&_=${Date.now()}`;

    const upstream = await fetch(sourceUrl, {
      headers: { Accept: "text/csv" },
      cache: "no-store"
    });

    if (!upstream.ok) {
      throw new Error(`WQX3 request failed: ${upstream.status}`);
    }

    const csvText = await upstream.text();
    const rows = parseCsv(csvText);

    if (!rows.length) {
      throw new Error("No rows parsed from upstream CSV");
    }

    const cleaned = rows
      .map((row) => normalizeRow(row))
      .filter(Boolean);

    const parameterDefs = [
      {
        key: "temperature_water",
        displayCharacteristic: "Temperature, water",
        group: "physical",
        match: (r) => r.characteristicLower === "temperature, water",
        score: () => 100
      },
      {
        key: "dissolved_oxygen",
        displayCharacteristic: "Dissolved oxygen",
        group: "physical",
        match: (r) =>
          r.characteristicLower.includes("dissolved oxygen") ||
          r.characteristicLower.includes("oxygen, dissolved"),
        score: () => 100
      },
      {
        key: "specific_conductance",
        displayCharacteristic: "Specific conductance",
        group: "physical",
        match: (r) => r.characteristicLower.includes("specific conductance"),
        score: () => 100
      },
      {
        key: "ph",
        displayCharacteristic: "pH",
        group: "physical",
        match: (r) => r.characteristicLower === "ph",
        score: () => 100
      },
      {
        key: "phosphorus",
        displayCharacteristic: "Phosphorus",
        group: "nutrient",
        match: (r) => r.characteristicLower === "phosphorus",
        score: (r) =>
          containsScore(r.fraction, "unfiltered") +
          containsScore(r.methodSpec, "as p") +
          pcodeScore(r.pcode, ["00665"])
      },
      {
        key: "nitrate",
        displayCharacteristic: "Nitrate",
        group: "nutrient",
        match: (r) => r.characteristicLower === "nitrate",
        score: (r) =>
          containsScore(r.methodSpec, "as n") +
          pcodeScore(r.pcode, ["00618"])
      },
      {
        key: "nitrite",
        displayCharacteristic: "Nitrite",
        group: "nutrient",
        match: (r) => r.characteristicLower === "nitrite",
        score: (r) =>
          containsScore(r.methodSpec, "as n") +
          pcodeScore(r.pcode, ["00613"])
      },
      {
        key: "inorganic_nitrogen",
        displayCharacteristic: "Inorganic nitrogen (nitrate and nitrite)",
        group: "nutrient",
        match: (r) => r.characteristicLower.includes("inorganic nitrogen"),
        score: (r) =>
          containsScore(r.methodSpec, "as n") +
          pcodeScore(r.pcode, ["00630"])
      },
      {
        key: "total_nitrogen",
        displayCharacteristic:
          "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)",
        group: "nutrient",
        match: (r) => r.characteristicLower.includes("mixed forms"),
        score: (r) =>
          containsScore(r.fraction, "unfiltered") +
          containsScore(r.methodSpec, "as n") +
          pcodeScore(r.pcode, ["00625"])
      },
      {
        key: "turbidity",
        displayCharacteristic: "Turbidity",
        group: "sediment",
        match: (r) => r.characteristicLower.includes("turbidity"),
        score: () => 100
      },
      {
        key: "suspended_sediment_load",
        displayCharacteristic: "Suspended Sediment Load",
        group: "sediment",
        match: (r) => r.characteristicLower.includes("suspended sediment load"),
        score: () => 100
      }
    ];

    const parameters = [];

    for (const def of parameterDefs) {
      const matched = cleaned.filter(def.match);
      if (!matched.length) continue;

      const bestByDate = new Map();

      for (const row of matched) {
        const key = formatDateOnly(row.date);
        const existing = bestByDate.get(key);

        if (!existing || def.score(row) > def.score(existing)) {
          bestByDate.set(key, row);
        }
      }

      const chosenRows = Array.from(bestByDate.values())
        .sort((a, b) => b.date.getTime() - a.date.getTime());

      if (!chosenRows.length) continue;

      const series = chosenRows.map((r) => ({
        characteristic: def.displayCharacteristic,
        group: def.group,
        value: r.value,
        unit: r.unit,
        date: toIsoMidnightUtc(r.date)
      }));

      parameters.push({
        characteristic: def.displayCharacteristic,
        group: def.group,
        latest: series[0],
        count: series.length,
        series
      });
    }

    parameters.sort((a, b) => new Date(b.latest.date) - new Date(a.latest.date));

    res.status(200).json({
      site,
      generatedAt: new Date().toISOString(),
      sourceUrl,
      latestDate: parameters.length ? parameters[0].latest.date : null,
      parameterCount: parameters.length,
      parameters
    });
  } catch (err) {
    res.status(500).json({
      error: err.message || "Unknown error"
    });
  }
}

function normalizeRow(row) {
  const characteristic = String(row["Result_Characteristic"] || "").trim();
  if (!characteristic) return null;

  const value = Number(String(row["Result_Measure"] || "").trim());
  if (!Number.isFinite(value)) return null;

  const date = parseDateOnly(row["Activity_StartDate"]);
  if (!date) return null;

  const unit = String(row["Result_MeasureUnit"] || "").trim();
  const fraction = String(row["Result_SampleFraction"] || "").trim();
  const methodSpec = String(row["Result_MethodSpeciation"] || "").trim();
  const pcode = String(row["USGSpcode"] || "").trim();

  const characteristicLower = characteristic.toLowerCase().trim();

  if (
    characteristicLower.includes("lot number") ||
    characteristicLower.includes("sample location") ||
    characteristicLower.includes("gage height") ||
    characteristicLower.includes("streamflow") ||
    characteristicLower.includes("barometric pressure")
  ) {
    return null;
  }

  return {
    characteristic,
    characteristicLower,
    value,
    unit,
    date,
    fraction,
    methodSpec,
    pcode
  };
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    const next = text[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      continue;
    }

    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && next === "\n") i++;
      row.push(cell);
      cell = "";
      if (row.some((x) => String(x).trim() !== "")) rows.push(row);
      row = [];
      continue;
    }

    cell += ch;
  }

  if (cell.length || row.length) {
    row.push(cell);
    if (row.some((x) => String(x).trim() !== "")) rows.push(row);
  }

  if (!rows.length) return [];

  const headers = rows[0].map((h) => String(h || "").trim());
  return rows.slice(1).map((vals) => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = vals[i] ?? "";
    }
    return obj;
  });
}

function parseDateOnly(value) {
  const m = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return null;
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
}

function formatDateOnly(date) {
  return date.toISOString().slice(0, 10);
}

function toIsoMidnightUtc(date) {
  return `${formatDateOnly(date)}T00:00:00.000Z`;
}

function containsScore(text, needle) {
  return String(text || "").toLowerCase().includes(String(needle).toLowerCase()) ? 100 : 0;
}

function pcodeScore(pcode, preferred) {
  return preferred.includes(String(pcode || "").trim()) ? 100 : 0;
}
