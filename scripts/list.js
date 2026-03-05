  (function(){
    const status = document.getElementById("status");
    const q = document.getElementById("q");
    const tbody = document.querySelector("#tbl tbody");
    const ths = Array.from(document.querySelectorAll("th[data-key]"));

    const radiusInput = document.getElementById("radiusKm");
    const useGeoBtn = document.getElementById("useGeo");
    const geoStatus = document.getElementById("geoStatus");
    const placeInput = document.getElementById("place");
    const geocodeBtn = document.getElementById("geocodeBtn");
    const placeList = document.getElementById("placeSuggestions");
    const centerLat = document.getElementById("centerLat");
    const centerLon = document.getElementById("centerLon");
    const setCustomBtn = document.getElementById("setCustom");

    // Grade sliders (min/max)
    const aMin = document.getElementById("aMin");
    const aMax = document.getElementById("aMax");
    const mMin = document.getElementById("mMin");
    const mMax = document.getElementById("mMax");
    const wiMin = document.getElementById("wiMin");
    const wiMax = document.getElementById("wiMax");
    const rMin = document.getElementById("rMin");
    const rMax = document.getElementById("rMax");
    const sunMin = document.getElementById("sunMin");
    const sunMax = document.getElementById("sunMax");
    const elevMin = document.getElementById("elevMin");
    const elevMax = document.getElementById("elevMax");
    const aRangeTxt = document.getElementById("aRangeTxt");
    const mRangeTxt = document.getElementById("mRangeTxt");
    const wiRangeTxt = document.getElementById("wiRangeTxt");
    const rRangeTxt = document.getElementById("rRangeTxt");
    const sunRangeTxt = document.getElementById("sunRangeTxt");
    const elevRangeTxt = document.getElementById("elevRangeTxt");

    const modal = document.getElementById("modal");
    const modalImg = document.getElementById("modalImg");
    const modalTitle = document.getElementById("modalTitle");
    const closeModal = document.getElementById("closeModal");
    const openNewTab = document.getElementById("openNewTab");

    let rows = [];
    let sortKey = "climb_max_tomorrow";
    let sortAsc = false;

    let center = null;
    let centerLabel = "";
    let radiusKm = NaN;

    let fAmin = NaN, fAmax = NaN;
    let fMmin = NaN, fMmax = NaN;
    let fWImin = NaN, fWImax = NaN;
    let fRmin = NaN, fRmax = NaN;
    let fSunMin = NaN, fSunMax = NaN;
    let fElevMin = NaN, fElevMax = NaN;

    const RANGE = {
      A:  { min: 0.75, max: 4.25 },
      M:  { min: 0.75, max: 13.25 },
      WI: { min: 0.75, max: 7.25 },
      R:  { min: 0.75, max: 12.25 },
      SUN:{ min: 0.00, max: 12.00 },
      ELEV:{ min: 0, max: 4000 }
    };
    const API_BASE = "https://icefalls-api.carlos-wydra.workers.dev";
    const UPLOAD_LOOKUP_CONCURRENCY = 10;
    const UPLOAD_LOOKUP_INITIAL_LIMIT = 120;
    const UPLOAD_LOOKUP_SORT_LIMIT = 400;
    const UPLOAD_CACHE_KEY = "icefalls:last_upload_cache:v1";
    const UPLOAD_CACHE_MAX_AGE_MS = 12 * 60 * 60 * 1000;

    const uploadQueue = [];
    let uploadActive = 0;
    let rerenderTimer = null;
    let backgroundQueued = false;

    function num(x){
      if (x === null || x === undefined) return NaN;
      const n = Number(x);
      return isFinite(n) ? n : NaN;
    }
    function str(x){
      if (x === null || x === undefined) return "";
      return String(x);
    }

    function fmtGrade(v){
      const x = num(v);
      if (!isFinite(x)) return "";
      const base = Math.round(x);
      const diff = x - base;
      if (diff > 0.10) return String(base) + "+";
      if (diff < -0.10) return String(base) + "-";
      return String(base);
    }

    function clampMinMax(minEl, maxEl){
      if (!minEl || !maxEl) return [NaN, NaN];
      let a = Number(minEl.value);
      let b = Number(maxEl.value);
      if (!isFinite(a) || !isFinite(b)) return [NaN, NaN];
      if (a > b) { const t = a; a = b; b = t; minEl.value = a; maxEl.value = b; }
      return [a, b];
    }

    function updateRangeLabels(){
      const [a1,a2] = clampMinMax(aMin, aMax);
      const [m1,m2] = clampMinMax(mMin, mMax);
      const [w1,w2] = clampMinMax(wiMin, wiMax);
      const [r1,r2] = clampMinMax(rMin, rMax);
      const [s1,s2] = clampMinMax(sunMin, sunMax);
      const [e1,e2] = clampMinMax(elevMin, elevMax);
      if (aRangeTxt && isFinite(a1) && isFinite(a2)) aRangeTxt.textContent = `A${fmtGrade(a1)} – A${fmtGrade(a2)}`;
      if (mRangeTxt && isFinite(m1) && isFinite(m2)) mRangeTxt.textContent = `M${fmtGrade(m1)} – M${fmtGrade(m2)}`;
      if (wiRangeTxt && isFinite(w1) && isFinite(w2)) wiRangeTxt.textContent = `WI${fmtGrade(w1)} – WI${fmtGrade(w2)}`;
      if (rRangeTxt && isFinite(r1) && isFinite(r2)) rRangeTxt.textContent = `${fmtGrade(r1)} – ${fmtGrade(r2)}`;
      if (sunRangeTxt && isFinite(s1) && isFinite(s2)) sunRangeTxt.textContent = `${s1.toFixed(1)} – ${s2.toFixed(1)} h`;
      if (elevRangeTxt && isFinite(e1) && isFinite(e2)) elevRangeTxt.textContent = `${Math.round(e1)} – ${Math.round(e2)} m`;
    }

    function parseDifficulty(d){
      const s0 = str(d).toUpperCase();
      const s = s0.replace(/SCHWIERIGKEIT|DIFFICULTY|GRADE|GRAD/g, " ");
      const out = { a: NaN, m: NaN, wi: NaN, r: NaN };
      let m = null;

      function signed(base, sign){
        const n = Number(base);
        if (!isFinite(n)) return NaN;
        if (sign === "+") return n + 0.25;
        if (sign === "-") return n - 0.25;
        return n;
      }

      m = s.match(/(?:^|[^A-Z])A\s*(\d{1,2})\s*([+\-])?/);
      if (m) out.a = signed(m[1], m[2]);

      m = s.match(/(?:^|[^A-Z])M\s*(\d{1,2})\s*([+\-])?/);
      if (m) out.m = signed(m[1], m[2]);

      m = s.match(/(?:^|[^A-Z])WI\s*(\d{1,2})\s*([+\-])?/);
      if (m) out.wi = signed(m[1], m[2]);

      // Standalone rock grades (1..12) with optional +/-; take max if multiple
      const re = /(?:^|[^A-Z0-9])(1[0-2]|[1-9])\s*([+\-])?(?=\b|[^0-9])/g;
      let best = NaN;
      while ((m = re.exec(s)) !== null) {
        const v = signed(m[1], m[2]);
        if (isFinite(v)) best = isFinite(best) ? Math.max(best, v) : v;
      }
      out.r = best;

      return out;
    }

    function parseUploadDate(item){
      if (!item || typeof item !== "object") return NaN;
      const candidates = [item.shot_date, item.created_at, item.uploaded_at];
      for (const raw of candidates){
        if (!raw) continue;
        const d = new Date(raw);
        const ts = d.getTime();
        if (Number.isFinite(ts)) return ts;
      }
      return NaN;
    }

    function formatUploadDate(ts){
      if (!Number.isFinite(ts)) return "";
      const d = new Date(ts);
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${dd}.${mm}.${yyyy}`;
    }

    function haversineKm(lat1, lon1, lat2, lon2){
      const R = 6371;
      const toRad = (d) => (d * Math.PI / 180);
      const dLat = toRad(lat2 - lat1);
      const dLon = toRad(lon2 - lon1);
      const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
                Math.sin(dLon/2) * Math.sin(dLon/2);
      const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
      return R * c;
    }

    function computeRowDistance(r){
      if (!center) { r._dist_km = NaN; return NaN; }
      const lat = num(r.latitude);
      const lon = num(r.longitude);
      if (!isFinite(lat) || !isFinite(lon)) { r._dist_km = NaN; return NaN; }
      const d = haversineKm(center.lat, center.lon, lat, lon);
      r._dist_km = d;
      return d;
    }

    function matches(r, query){
      // Radius filter (optional)
      if (center && isFinite(radiusKm)) {
        const d = num(r._dist_km);
        if (!isFinite(d) || d > radiusKm) return false;
      }

      // Grade filters (optional)
      if (isFinite(fAmin) || isFinite(fAmax)) {
        const v = num(r._grade_a);
        if (!isFinite(v)) return false;
        if (isFinite(fAmin) && v < fAmin) return false;
        if (isFinite(fAmax) && v > fAmax) return false;
      }
      if (isFinite(fMmin) || isFinite(fMmax)) {
        const v = num(r._grade_m);
        if (!isFinite(v)) return false;
        if (isFinite(fMmin) && v < fMmin) return false;
        if (isFinite(fMmax) && v > fMmax) return false;
      }
      if (isFinite(fWImin) || isFinite(fWImax)) {
        const v = num(r._grade_wi);
        if (!isFinite(v)) return false;
        if (isFinite(fWImin) && v < fWImin) return false;
        if (isFinite(fWImax) && v > fWImax) return false;
      }
      if (isFinite(fRmin) || isFinite(fRmax)) {
        const v = num(r._grade_r);
        if (!isFinite(v)) return false;
        if (isFinite(fRmin) && v < fRmin) return false;
        if (isFinite(fRmax) && v > fRmax) return false;
      }
      if (isFinite(fSunMin) || isFinite(fSunMax)) {
        const v = num(r.sun_hours_tomorrow_h);
        if (!isFinite(v)) return false;
        if (isFinite(fSunMin) && v < fSunMin) return false;
        if (isFinite(fSunMax) && v > fSunMax) return false;
      }
      if (isFinite(fElevMin) || isFinite(fElevMax)) {
        const v = num(r.elev_m);
        if (!isFinite(v)) return false;
        if (isFinite(fElevMin) && v < fElevMin) return false;
        if (isFinite(fElevMax) && v > fElevMax) return false;
      }

      if(!query) return true;
      const t = query.toLowerCase();
      const blob = [
        r.name, r.difficulty,
        (isFinite(num(r._grade_a)) ? ("A" + fmtGrade(r._grade_a)) : ""),
        (isFinite(num(r._grade_m)) ? ("M" + fmtGrade(r._grade_m)) : ""),
        (isFinite(num(r._grade_wi)) ? ("WI" + fmtGrade(r._grade_wi)) : ""),
        (isFinite(num(r._grade_r)) ? fmtGrade(r._grade_r) : ""),
        r.aspect, r.station_id, r.source, r.approach, r.descent
      ].map(str).join(" | ").toLowerCase();
      return blob.includes(t);
    }

    function cmp(a,b){
      const va = a[sortKey];
      const vb = b[sortKey];
      const na = num(va), nb = num(vb);
      const aMiss = !isFinite(na);
      const bMiss = !isFinite(nb);
      if (aMiss && !bMiss) return 1;
      if (!aMiss && bMiss) return -1;
      if (!aMiss && !bMiss) return sortAsc ? (na-nb) : (nb-na);
      const sa = str(va).toLowerCase();
      const sb = str(vb).toLowerCase();
      if (sa < sb) return sortAsc ? -1 : 1;
      if (sa > sb) return sortAsc ? 1 : -1;
      return 0;
    }

    function openFullscreen(plotUrl, title){
      modalImg.src = plotUrl;
      modalTitle.textContent = title || "Diagramm";
      openNewTab.href = plotUrl;
      modal.style.display = "block";
    }
    function closeFullscreen(){
      modal.style.display = "none";
      modalImg.src = "";
    }

    if (closeModal) closeModal.addEventListener("click", closeFullscreen);
    if (modal) modal.addEventListener("click", function(e){ if (e.target === modal) closeFullscreen(); });
    document.addEventListener("keydown", function(e){ if (e.key === "Escape") closeFullscreen(); });

    function applySliderFilters(){
      // radius
      radiusKm = radiusInput ? Number(radiusInput.value) : NaN;
      if (!isFinite(radiusKm) || radiusKm <= 0) radiusKm = NaN;

      // grade ranges
      let a = clampMinMax(aMin, aMax);
      let m = clampMinMax(mMin, mMax);
      let w = clampMinMax(wiMin, wiMax);
      let r = clampMinMax(rMin, rMax);
      let s = clampMinMax(sunMin, sunMax);
      let e = clampMinMax(elevMin, elevMax);

      fAmin = (isFinite(a[0]) && a[0] > RANGE.A.min + 1e-9) ? a[0] : NaN;
      fAmax = (isFinite(a[1]) && a[1] < RANGE.A.max - 1e-9) ? a[1] : NaN;
      fMmin = (isFinite(m[0]) && m[0] > RANGE.M.min + 1e-9) ? m[0] : NaN;
      fMmax = (isFinite(m[1]) && m[1] < RANGE.M.max - 1e-9) ? m[1] : NaN;
      fWImin = (isFinite(w[0]) && w[0] > RANGE.WI.min + 1e-9) ? w[0] : NaN;
      fWImax = (isFinite(w[1]) && w[1] < RANGE.WI.max - 1e-9) ? w[1] : NaN;
      fRmin = (isFinite(r[0]) && r[0] > RANGE.R.min + 1e-9) ? r[0] : NaN;
      fRmax = (isFinite(r[1]) && r[1] < RANGE.R.max - 1e-9) ? r[1] : NaN;
      fSunMin = (isFinite(s[0]) && s[0] > RANGE.SUN.min + 1e-9) ? s[0] : NaN;
      fSunMax = (isFinite(s[1]) && s[1] < RANGE.SUN.max - 1e-9) ? s[1] : NaN;
      fElevMin = (isFinite(e[0]) && e[0] > RANGE.ELEV.min + 1e-9) ? e[0] : NaN;
      fElevMax = (isFinite(e[1]) && e[1] < RANGE.ELEV.max - 1e-9) ? e[1] : NaN;

      updateRangeLabels();
    }

    function render(){
      applySliderFilters();
      for (const r of rows) computeRowDistance(r);
      const query = q.value.trim();
      const view = rows.filter(r => matches(r, query)).sort(cmp);
      ensureUploadsForView(view);

      tbody.innerHTML = "";
      for(const r of view){
        const tr = document.createElement("tr");
        const topoLink = r.topo_url ? `<a href="${r.topo_url}" target="_blank" rel="noopener">Topo</a>` : `<span class="muted">&mdash;</span>`;
        const uidPad = String(r.uid).padStart(3,"0");
        const stationTxt = r.station_id ? ("Station: " + str(r.station_id) + (r.source ? (" (" + str(r.source) + ")") : "")) : "";
        const metaTxt = ["UID " + uidPad, stationTxt].filter(Boolean).join(" · ");
   const detailsUrl = `icefalls/uid_${uidPad}.html`;
   const detailsBtn = `<a class="btn" href="${detailsUrl}">Open</a>`;

        const aTxt  = isFinite(num(r._grade_a))  ? ("A" + fmtGrade(r._grade_a))  : "<span class=muted>&mdash;</span>";
        const mTxt  = isFinite(num(r._grade_m))  ? ("M" + fmtGrade(r._grade_m))  : "<span class=muted>&mdash;</span>";
        const wiTxt = isFinite(num(r._grade_wi)) ? ("WI" + fmtGrade(r._grade_wi)) : "<span class=muted>&mdash;</span>";
        const rTxt  = isFinite(num(r._grade_r))  ? fmtGrade(r._grade_r)  : "<span class=muted>&mdash;</span>";

        tr.innerHTML = `
          <td>
            <div><b>${str(r.name) || ("UID " + r.uid)}</b></div>
            <div class="muted">${metaTxt}</div>
          </td>
          <td>${str(r.difficulty) || "<span class=muted>&mdash;</span>"}</td>
          <td>${aTxt}</td>
          <td>${mTxt}</td>
          <td>${wiTxt}</td>
          <td>${rTxt}</td>
          <td>${isFinite(num(r.elev_m)) ? Math.round(num(r.elev_m)) : "<span class=muted>&mdash;</span>"}</td>
          <td>${isFinite(num(r._dist_km)) ? (num(r._dist_km).toFixed(1) + " km") : "<span class=muted>&mdash;</span>"}</td>
          <td>${str(r.sun_tomorrow_range_txt) || "<span class=muted>&mdash;</span>"}</td>
          <td>${isFinite(num(r.sun_hours_tomorrow_h)) ? (num(r.sun_hours_tomorrow_h).toFixed(1) + " h") : "<span class=muted>&mdash;</span>"}</td>
          <td>${r.thickness_tomorrow_07_txt || "<span class=muted>&mdash;</span>"}</td>
          <td>${(r.climb_max_tomorrow_txt || str(r.climb_max_time_local))
            ? `${r.climb_max_tomorrow_txt || "<span class=muted>&mdash;</span>"}<div class="muted">${str(r.climb_max_time_local) || "&mdash;"}</div>`
            : "<span class=muted>&mdash;</span>"}</td>
          <td>${r.last_upload_txt || "<span class=muted>&mdash;</span>"}</td>
          <td>${detailsBtn}</td>
          <td>${topoLink}</td>
        `;
        tbody.appendChild(tr);
      }

      Array.from(document.querySelectorAll("button[data-plot]"))
        .forEach(btn => btn.addEventListener("click", () => openFullscreen(btn.getAttribute("data-plot"), btn.getAttribute("data-title"))));

      const dirTxt = sortAsc ? "ASC" : "DESC";
      const radiusTxt = (center && isFinite(radiusKm)) ? (` | Radius: ${radiusKm} km around ${centerLabel || "Center"}`) : "";
      const centerOnlyTxt = (center && !isFinite(radiusKm)) ? (` | Center: ${centerLabel || "Center"} (radius off)`) : "";
      status.textContent = `Entries: ${view.length} / ${rows.length}${radiusTxt || centerOnlyTxt} | Sort: ${sortKey} ${dirTxt}`;
    }

    ths.forEach(th => {
      th.addEventListener("click", () => {
        const key = th.getAttribute("data-key");
        if (key === sortKey) sortAsc = !sortAsc; else { sortKey = key; sortAsc = true; }
        render();
      });
    });

    q.addEventListener("input", render);
    if (radiusInput) radiusInput.addEventListener("input", render);
    if (aMin) aMin.addEventListener("input", render);
    if (aMax) aMax.addEventListener("input", render);
    if (mMin) mMin.addEventListener("input", render);
    if (mMax) mMax.addEventListener("input", render);
    if (wiMin) wiMin.addEventListener("input", render);
    if (wiMax) wiMax.addEventListener("input", render);
    if (rMin) rMin.addEventListener("input", render);
    if (rMax) rMax.addEventListener("input", render);
    if (sunMin) sunMin.addEventListener("input", render);
    if (sunMax) sunMax.addEventListener("input", render);
    if (elevMin) elevMin.addEventListener("input", render);
    if (elevMax) elevMax.addEventListener("input", render);

    // Custom coordinate center
    if (setCustomBtn) setCustomBtn.addEventListener("click", () => {
      const lat = centerLat ? Number(centerLat.value) : NaN;
      const lon = centerLon ? Number(centerLon.value) : NaN;
      if (!isFinite(lat) || !isFinite(lon)) { if (geoStatus) geoStatus.textContent = "Invalid coordinates"; return; }
      center = { lat, lon };
      centerLabel = lat.toFixed(5) + "," + lon.toFixed(5);
      if (geoStatus) geoStatus.textContent = "Center set: " + centerLabel;
      render();
    });

    function geocodePlace(query){
      const q = (query || "").trim();
      if (!q) return Promise.reject(new Error("No place entered"));
      const url = "https://nominatim.openstreetmap.org/search?format=json&limit=1&q=" + encodeURIComponent(q) + "&countrycodes=at,de,it,ch";
      return fetch(url, { cache: "no-store", headers: { "Accept": "application/json" } })
        .then(r => { if (!r.ok) throw new Error("Geocoding HTTP " + r.status); return r.json(); })
        .then(arr => {
          if (!Array.isArray(arr) || arr.length === 0) throw new Error("Place not found");
          const hit = arr[0];
          const lat = Number(hit.lat);
          const lon = Number(hit.lon);
          const label = hit.display_name ? String(hit.display_name).split(",")[0] : q;
          if (!isFinite(lat) || !isFinite(lon)) throw new Error("Geocoding without coordinates");
          return { lat, lon, label };
        });
    }

    // Typeahead suggestions (Nominatim). Needs Internet; keep usage light (debounce + min length).
    let sugTimer = null;
    let lastSug = [];
    function clearSuggestions(){
      lastSug = [];
      if (!placeList) return;
      placeList.innerHTML = "";
    }
    function setSuggestions(items){
      lastSug = items || [];
      if (!placeList) return;
      placeList.innerHTML = "";
      for (const it of lastSug){
        const opt = document.createElement("option");
        opt.value = it.label;
        placeList.appendChild(opt);
      }
    }
    function fetchSuggestions(q){
      const qq = (q || "").trim();
      if (qq.length < 3) { clearSuggestions(); return; }
      const url = "https://nominatim.openstreetmap.org/search?format=json&limit=6&q=" + encodeURIComponent(qq) + "&countrycodes=at,de,it,ch";
      fetch(url, { cache: "no-store", headers: { "Accept": "application/json" } })
        .then(r => { if (!r.ok) throw new Error("Suggest HTTP " + r.status); return r.json(); })
        .then(arr => {
          if (!Array.isArray(arr) || arr.length === 0) { clearSuggestions(); return; }
          const items = arr.map(h => {
            const lat = Number(h.lat);
            const lon = Number(h.lon);
            const label = h.display_name ? String(h.display_name).split(",").slice(0,3).join(", ") : qq;
            return { label, lat, lon };
          }).filter(x => isFinite(x.lat) && isFinite(x.lon));
          setSuggestions(items);
        })
        .catch(_ => { /* silent */ });
    }
    if (placeInput) placeInput.addEventListener("input", () => {
      if (sugTimer) clearTimeout(sugTimer);
      const v = placeInput.value;
      sugTimer = setTimeout(() => fetchSuggestions(v), 350);
    });
    if (placeInput) placeInput.addEventListener("keydown", (e) => { if (e.key === "Enter" && geocodeBtn) geocodeBtn.click(); });
    if (geocodeBtn) geocodeBtn.addEventListener("click", () => {
      if (!placeInput) return;
      const v = (placeInput.value || "").trim();
      if (!v) { if (geoStatus) geoStatus.textContent = "No place entered"; return; }
      const hit = lastSug.find(s => String(s.label).toLowerCase() === v.toLowerCase());
      if (hit && isFinite(hit.lat) && isFinite(hit.lon)) {
        center = { lat: hit.lat, lon: hit.lon };
        centerLabel = hit.label;
        if (centerLat) centerLat.value = hit.lat.toFixed(6);
        if (centerLon) centerLon.value = hit.lon.toFixed(6);
        if (geoStatus) geoStatus.textContent = "Center set: " + centerLabel;
        render();
        return;
      }
      if (geoStatus) geoStatus.textContent = "Searching place ...";
      geocodePlace(v)
        .then(res => {
          center = { lat: res.lat, lon: res.lon };
          centerLabel = res.label;
          if (centerLat) centerLat.value = res.lat.toFixed(6);
          if (centerLon) centerLon.value = res.lon.toFixed(6);
          if (geoStatus) geoStatus.textContent = "Center set: " + centerLabel;
          render();
        })
        .catch(err => { if (geoStatus) geoStatus.textContent = "Place search failed: " + (err && err.message ? err.message : err); });
    });

    if (useGeoBtn) useGeoBtn.addEventListener("click", () => {
      if (!navigator.geolocation) { if (geoStatus) geoStatus.textContent = "Geolocation not available"; return; }
      if (window.isSecureContext !== true) { if (geoStatus) geoStatus.textContent = "GPS requires https or localhost (file:// is usually blocked)."; return; }
      if (geoStatus) geoStatus.textContent = "Getting location ...";
      navigator.geolocation.getCurrentPosition(
        (pos) => {
          const lat = pos.coords.latitude;
          const lon = pos.coords.longitude;
          if (centerLat) centerLat.value = lat.toFixed(6);
          if (centerLon) centerLon.value = lon.toFixed(6);
          center = { lat: lat, lon: lon };
          centerLabel = "Location";
          if (geoStatus) geoStatus.textContent = "Center: Location";
          render();
        },
        (err) => { if (geoStatus) geoStatus.textContent = "Location failed: " + (err && err.message ? err.message : err); },
        { enableHighAccuracy: false, timeout: 10000, maximumAge: 600000 }
      );
    });

    function enrichRows(){
      for (const r of rows){
        const g = parseDifficulty(r.difficulty);
        r._grade_a = g.a;
        r._grade_m = g.m;
        r._grade_wi = g.wi;
        r._grade_r = g.r;
        r._last_upload_ts = NaN;
        r.last_upload_txt = "";
        r._upload_lookup_started = false;
        r._last_upload_cached_at = NaN;
        r._last_upload_empty = false;
      }
    }

    function loadUploadCache(){
      try {
        const raw = localStorage.getItem(UPLOAD_CACHE_KEY);
        if (!raw) return;
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== "object") return;
        const entries = parsed.entries;
        if (!entries || typeof entries !== "object") return;
        const now = Date.now();
        for (const r of rows){
          if (!r || r.uid === null || r.uid === undefined) continue;
          const rec = entries[String(r.uid)];
          if (!rec || typeof rec !== "object") continue;
          const ts = Number(rec.ts);
          const cachedAt = Number(rec.cached_at);
          const hasUpload = rec.has_upload !== false;
          if (!Number.isFinite(cachedAt)) continue;
          if (hasUpload && !Number.isFinite(ts)) continue;
          if ((now - cachedAt) > UPLOAD_CACHE_MAX_AGE_MS) continue;
          r._last_upload_empty = !hasUpload;
          r._last_upload_ts = hasUpload ? ts : NaN;
          r.last_upload_txt = hasUpload ? formatUploadDate(ts) : "";
          r._last_upload_cached_at = cachedAt;
        }
      } catch (_) {
        // ignore cache parse/storage errors
      }
    }

    function saveUploadCache(){
      try {
        const entries = {};
        const now = Date.now();
        for (const r of rows){
          if (!r || r.uid === null || r.uid === undefined) continue;
          if (Number.isFinite(r._last_upload_ts)) {
            entries[String(r.uid)] = { ts: r._last_upload_ts, has_upload: true, cached_at: now };
            continue;
          }
          if (r._last_upload_empty) {
            entries[String(r.uid)] = { ts: 0, has_upload: false, cached_at: now };
          }
        }
        localStorage.setItem(UPLOAD_CACHE_KEY, JSON.stringify({ entries }));
      } catch (_) {
        // ignore cache write errors
      }
    }

    function queueBackgroundUploadLookups(view){
      if (backgroundQueued) return;
      backgroundQueued = true;
      const seen = new Set();
      const prioritized = Array.isArray(view) ? view : [];
      const rest = [];
      for (const r of prioritized){
        if (!r || r.uid === null || r.uid === undefined) continue;
        const k = String(r.uid);
        if (seen.has(k)) continue;
        seen.add(k);
        queueUploadLookup(r);
      }
      for (const r of rows){
        if (!r || r.uid === null || r.uid === undefined) continue;
        const k = String(r.uid);
        if (seen.has(k)) continue;
        seen.add(k);
        rest.push(r);
      }
      const schedule = () => {
        for (const r of rest) queueUploadLookup(r);
      };
      if (typeof requestIdleCallback === "function") {
        requestIdleCallback(schedule, { timeout: 1200 });
      } else {
        setTimeout(schedule, 250);
      }
    }

    function scheduleRender(){
      if (rerenderTimer !== null) return;
      rerenderTimer = setTimeout(() => {
        rerenderTimer = null;
        render();
      }, 80);
    }

    async function fetchLastUploadForRow(r){
      if (!r || r.uid === null || r.uid === undefined) return;
      try {
        const res = await fetch(`${API_BASE}/api/images?uid=${r.uid}`, { method: "GET" });
        if (!res.ok) return;
        const arr = await res.json();
        if (!Array.isArray(arr) || arr.length === 0) {
          r._last_upload_empty = true;
          r._last_upload_ts = NaN;
          r.last_upload_txt = "";
          r._last_upload_cached_at = Date.now();
          return;
        }
        let maxTs = NaN;
        for (const item of arr){
          const ts = parseUploadDate(item);
          if (!Number.isFinite(ts)) continue;
          if (!Number.isFinite(maxTs) || ts > maxTs) maxTs = ts;
        }
        if (Number.isFinite(maxTs)) {
          r._last_upload_empty = false;
          r._last_upload_ts = maxTs;
          r.last_upload_txt = formatUploadDate(maxTs);
        } else {
          r._last_upload_empty = true;
          r._last_upload_ts = NaN;
          r.last_upload_txt = "";
        }
        r._last_upload_cached_at = Date.now();
      } catch (_) {
        // ignore upload lookup errors
      }
    }

    function hasFreshUploadCache(r){
      if (!r || !Number.isFinite(r._last_upload_cached_at)) return false;
      return (Date.now() - r._last_upload_cached_at) <= UPLOAD_CACHE_MAX_AGE_MS;
    }

    function pumpUploadQueue(){
      while (uploadActive < UPLOAD_LOOKUP_CONCURRENCY && uploadQueue.length) {
        const r = uploadQueue.shift();
        uploadActive += 1;
        fetchLastUploadForRow(r)
          .finally(() => {
            uploadActive -= 1;
            saveUploadCache();
            scheduleRender();
            pumpUploadQueue();
          });
      }
    }

    function queueUploadLookup(r){
      if (!r || r.uid === null || r.uid === undefined) return;
      if (hasFreshUploadCache(r)) return;
      if (r._upload_lookup_started) return;
      r._upload_lookup_started = true;
      uploadQueue.push(r);
      pumpUploadQueue();
    }

    function ensureUploadsForView(view){
      if (!Array.isArray(view) || view.length === 0) return;
      const cap = (sortKey === "_last_upload_ts") ? UPLOAD_LOOKUP_SORT_LIMIT : UPLOAD_LOOKUP_INITIAL_LIMIT;
      for (let i = 0; i < view.length && i < cap; i += 1){
        queueUploadLookup(view[i]);
      }
      queueBackgroundUploadLookups(view);
    }

    function initSunSliderFromData(){
      // set SUN max to data-driven max (rounded up), keep within [2, 24]
      let mx = 0;
      for (const r of rows){
        const v = num(r.sun_hours_tomorrow_h);
        if (isFinite(v)) mx = Math.max(mx, v);
      }
      if (!isFinite(mx) || mx <= 0) mx = RANGE.SUN.max;
      const maxNice = Math.min(24, Math.max(2, Math.ceil(mx * 4) / 4));
      RANGE.SUN.max = maxNice;
      if (sunMin) { sunMin.max = String(maxNice); if (Number(sunMin.value) > maxNice) sunMin.value = String(maxNice); }
      if (sunMax) { sunMax.max = String(maxNice); sunMax.value = String(maxNice); }
      updateRangeLabels();
    }

    try {
      const b64el = document.getElementById("ICEFALL_DATA_B64");
      const b64 = (b64el && b64el.textContent) ? b64el.textContent.trim() : "";
      if (b64.length > 10) {
        const bin = atob(b64);
        const bytes = Uint8Array.from(bin, c => c.charCodeAt(0));
        const jsonText = new TextDecoder("utf-8").decode(bytes);
        rows = JSON.parse(jsonText);
        if (!Array.isArray(rows)) rows = [];
        enrichRows();
        initSunSliderFromData();
        loadUploadCache();
        status.textContent = `Data loaded (embedded): ${rows.length} entries`;
        render();
        return;
      }
    } catch(e) { console.error("Embedded Base64 parse failed", e); }

    status.textContent = "No embedded JSON found. Trying fetch() ...";
    const candidates = ["icefalls_table.json", "./icefalls_table.json", "../icefalls_table.json", "site/icefalls_table.json", "./site/icefalls_table.json", "../site/icefalls_table.json"];
    function fetchJsonFirstOk(urls){
      return urls.reduce((p, u) => p.catch(() => fetch(u, {cache: "no-store"}).then(r => {
        if (!r.ok) throw new Error(u + " -> HTTP " + r.status);
        return r.json().then(data => ({ data, url: u }));
      })), Promise.reject(new Error("no candidates tried")));
    }
    fetchJsonFirstOk(candidates)
      .then(res => {
        rows = (res && res.data) ? res.data : [];
        if (!Array.isArray(rows)) rows = [];
        enrichRows();
        initSunSliderFromData();
        loadUploadCache();
        status.textContent = `Data loaded: ${rows.length} entries (source: ${res.url})`;
        render();
      })
      .catch(err => { status.textContent = "Loading error: " + err; console.error(err); });
  })();
