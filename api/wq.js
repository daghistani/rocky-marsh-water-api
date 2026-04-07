export default async function handler(req, res) {
  const SITE = "USGS-01618100";

  try {
    const url =
      `https://www.waterqualitydata.us/wqx3/Result/search?siteid=${SITE}&mimeType=json&dataProfile=fullPhysChem`;

    const upstream = await fetch(url, { cache: "no-store" });

    if (!upstream.ok) {
      return res.status(502).json({ error: `Upstream request failed (${upstream.status})` });
    }

    const data = await upstream.json();
    const results = Array.isArray(data?.Results) ? data.Results : [];

    const cleaned = results
      .map((r) => {
        const rawValue =
          r?.ResultMeasureValue ??
          r?.ResultValue ??
          r?.ResultMeasure?.ResultMeasureValue;

        const value = Number(rawValue);
        if (!Number.isFinite(value)) return null;

        const rawDate =
          r?.ActivityStartDate ??
          r?.ActivityStartDateTime ??
          r?.ResultDate;

        const date = new Date(rawDate);
        if (Number.isNaN(date.getTime())) return null;

        const unit =
          r?.ResultMeasure?.MeasureUnitCode ??
          r?.ResultMeasureMeasureUnitCode ??
          "";

        return {
          characteristic: r?.CharacteristicName || "Unknown",
          value,
          unit,
          date: date.toISOString()
        };
      })
      .filter(Boolean);

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
      parameterCount: parameters.length,
      sampleCount: cleaned.length,
      parameters
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown server error"
    });
  }
}
