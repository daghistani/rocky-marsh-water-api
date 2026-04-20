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

    // Use WQP beta because it includes more recent USGS data additions.
    const url =
      "https://www.waterqualitydata.us/beta/data/Result/search" +
      `?siteid=${encodeURIComponent(site)}` +
      "&mimeType=csv" +
      "&sorted=no" +
      `&cacheBust=${Date.now()}`;

    const upstream = await fetch(url, {
      headers: {
        Accept: "text/csv"
      }
    });

    if (!upstream.ok) {
      throw new Error(`Upstream request failed: ${upstream.status}`);
    }

    const csv = await upstream.text();
    if (!csv || !csv.trim()) {
      throw new Error("Upstream returned empty CSV");
    }

    const rows = parseCsv(csv);
    if (!rows.length) {
      throw new Error("No rows parsed from upstream CSV");
    }

    const cleaned = rows
      .map(normalizeRow)
      .filter(Boolean);

    const grouped = buildCanonicalParameters(cleaned);

    const parameters = Object.values(grouped)
      .map((item) => {
        const series = item.rows
          .filter((r) => Number.isFinite(r.value) && r.date)
          .sort((a, b) => compareDateDesc(a.date, b.date))
          .map((r) => ({
            characteristic: item.displayCharacteristic,
            group: item.group,
            value: r.value,
            unit: r.unit || "",
            date: toIsoDateOnlyUtc(r.date)
          }));

        if (!series.length) return null;

        return {
          characteristic: item.displayCharacteristic,
          group: item.group,
          latest: series[0],
          count: series.length,
          series
        };
      })
      .filter(Boolean)
      .sort((a, b) => compareDateDesc(a.latest.date, b.latest.date));

    const latestDate = parameters.length ? parameters[0].latest.date : null;

    res.status(200).json({
      site,
      generatedAt: new Date().toISOString(),
      latestDate,
      parameterCount: parameters.length,
      parameters
    });
  } catch (err) {
    res.status(500).json({
      error: err.message || "Unknown error"
    });
  }
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

      if (row.some((x) => String(x).trim() !== "")) {
        rows.push(row);
      }
      row = [];
      continue;
    }

    cell += ch;
  }

  if (cell.length || row.length) {
    row.push(cell);
    if (row.some((x) => String(x).trim() !== "")) {
      rows.push(row);
    }
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

function normalizeRow(r) {
  const characteristic =
    pick(r, [
      "CharacteristicName",
      "characteristicName",
      "Characteristic Name"
    ]) || "";

  if (!characteristic) return null;

  const valueRaw = pick(r, [
    "ResultMeasureValue",
    "Result Measure Value",
    "MeasureValue",
    "value"
  ]);

  const value = Number(String(valueRaw).trim());
  if (!Number.isFinite(value)) return null;

  const unit =
    pick(r, [
      "ResultMeasure/MeasureUnitCode",
      "Result Measure/Measure Unit Code",
      "MeasureUnitCode",
      "unit"
    ]) || "";

  const dateRaw =
    pick(r, [
      "ActivityStartDate",
      "Activity Start Date",
      "date"
    ]) || "";

  const date = parseDateOnly(dateRaw);
  if (!date) return null;

  const fraction =
    pick(r, [
      "ResultSampleFractionText",
      "Result Sample Fraction Text",
      "SampleFractionText"
    ]) || "";

  const chemicalForm =
    pick(r, [
      "ResultChemicalFormText",
      "Result Chemical Form Text",
      "ChemicalFormText"
    ]) || "";

  const pcode =
    pick(r, [
      "USGSPCode",
      "USGS PCode",
      "PCode"
    ]) || "";

  const detectionCondition =
    pick(r, [
      "ResultDetectionConditionText",
      "Result Detection Condition Text"
    ]) || "";

  const resultStatus =
    pick(r, [
      "ResultStatusIdentifier",
      "Result Status Identifier"
    ]) || "";

  const comment =
    pick(r, [
      "ResultCommentText",
      "Result Comment Text"
    ]) || "";

  const lowerName = characteristic.toLowerCase();

  if (
    lowerName.includes("lot number") ||
    lowerName.includes("sample location") ||
    lowerName.includes("gage height") ||
    lowerName.includes("streamflow") ||
    lowerName.includes("barometric pressure")
  ) {
    return null;
  }

  if (
    detectionCondition &&
    /not detected|below detection|present above quantification limit/i.test(detectionCondition) &&
    !Number.isFinite(value)
  ) {
    return null;
  }

  return {
    raw: r,
    characteristic,
    characteristicLower: lowerName,
    value,
    unit: normalizeUnit(unit),
    date,
    fraction,
    chemicalForm,
    pcode,
    detectionCondition,
    resultStatus,
    comment
  };
}

function buildCanonicalParameters(rows) {
  const defs = [
    {
      key: "temperature_water",
      displayCharacteristic: "Temperature, water",
      group: "physical",
      test: (r) => r.characteristicLower === "temperature, water",
      prefer: (r) => 100
    },
    {
      key: "dissolved_oxygen",
      displayCharacteristic: "Dissolved oxygen",
      group: "physical",
      test: (r) =>
        r.characteristicLower.includes("dissolved oxygen") ||
        r.characteristicLower.includes("oxygen, dissolved"),
      prefer: (r) => 100
    },
    {
      key: "specific_conductance",
      displayCharacteristic: "Specific conductance",
      group: "physical",
      test: (r) => r.characteristicLower.includes("specific conductance"),
      prefer: (r) => 100
    },
    {
      key: "ph",
      displayCharacteristic: "pH",
      group: "physical",
      test: (r) => r.characteristicLower === "ph",
      prefer: (r) => 100
    },
    {
      key: "phosphorus",
      displayCharacteristic: "Phosphorus",
      group: "nutrient",
      test: (r) => r.characteristicLower === "phosphorus",
      prefer: (r) =>
        scoreContains(r.fraction, "unfiltered") +
        scoreContains(r.chemicalForm, "as p") +
        scorePcode(r.pcode, ["00665"]),
    },
    {
      key: "nitrate",
      displayCharacteristic: "Nitrate",
      group: "nutrient",
      test: (r) => r.characteristicLower === "nitrate",
      prefer: (r) =>
        scoreContains(r.chemicalForm, "as n") +
        scorePcode(r.pcode, ["00618"]),
    },
    {
      key: "nitrite",
      displayCharacteristic: "Nitrite",
      group: "nutrient",
      test: (r) => r.characteristicLower === "nitrite",
      prefer: (r) =>
        scoreContains(r.chemicalForm, "as n") +
        scorePcode(r.pcode, ["00613"]),
    },
    {
      key: "inorganic_nitrogen",
      displayCharacteristic: "Inorganic nitrogen (nitrate and nitrite)",
      group: "nutrient",
      test: (r) => r.characteristicLower.includes("inorganic nitrogen"),
      prefer: (r) =>
        scoreContains(r.chemicalForm, "as n") +
        scorePcode(r.pcode, ["00630"]),
    },
    {
      key: "total_nitrogen",
      displayCharacteristic:
        "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)",
      group: "nutrient",
      test: (r) => r.characteristicLower.includes("mixed forms"),
      prefer: (r) =>
        scoreContains(r.fraction, "unfiltered") +
        scoreContains(r.chemicalForm, "as n") +
        scorePcode(r.pcode, ["00625"]),
    },
    {
      key: "turbidity",
      displayCharacteristic: "Turbidity",
      group: "sediment",
      test: (r) => r.characteristicLower.includes("turbidity"),
      prefer: (r) => 100
    },
    {
      key: "suspended_sediment_load",
      displayCharacteristic: "Suspended Sediment Load",
      group: "sediment",
      test: (r) => r.characteristicLower.includes("suspended sediment load"),
      prefer: (r) => 100
    }
  ];

  const out = {};

  for (const def of defs) {
    const matched = rows.filter(def.test);
    if (!matched.length) continue;

    const byDate = new Map();

    for (const row of matched) {
      const dateKey = formatDateOnly(row.date);
      const current = byDate.get(dateKey);

      if (!current || def.prefer(row) > def.prefer(current)) {
        byDate.set(dateKey, row);
      }
    }

    const canonicalRows = Array.from(byDate.values()).sort((a, b) =>
      compareDateDesc(a.date, b.date)
    );

    if (!canonicalRows.length) continue;

    out[def.key] = {
      displayCharacteristic: def.displayCharacteristic,
      group: def.group,
      rows: canonicalRows
    };
  }

  return out;
}

function pick(obj, keys) {
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") {
      return obj[k];
    }
  }
  return "";
}

function normalizeUnit(unit) {
  const u = String(unit || "").trim();
  if (u === "uS/cm") return "uS/cm";
  return u;
}

function parseDateOnly(s) {
  const m = String(s || "").match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return null;
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
}

function formatDateOnly(d) {
  return d.toISOString().slice(0, 10);
}

function toIsoDateOnlyUtc(d) {
  return `${formatDateOnly(d)}T00:00:00.000Z`;
}

function compareDateDesc(a, b) {
  return new Date(b).getTime() - new Date(a).getTime();
}

function scoreContains(text, needle) {
  const t = String(text || "").toLowerCase();
  return t.includes(String(needle).toLowerCase()) ? 100 : 0;
}

function scorePcode(pcode, preferred) {
  return preferred.includes(String(pcode || "").trim()) ? 100 : 0;
}
