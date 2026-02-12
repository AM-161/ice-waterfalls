function(el, x){
  var map = this;

  function num(x){ var n = Number(x); return isFinite(n) ? n : NaN; }

  function fmtGrade(v){
    var x = num(v);
    if (!isFinite(x)) return '';
    var base = Math.round(x);
    var diff = x - base;
    if (diff > 0.10) return String(base) + '+';
    if (diff < -0.10) return String(base) + '-';
    return String(base);
  }

  function signed(base, sign){
    var n = Number(base);
    if (!isFinite(n)) return NaN;
    if (sign === '+') return n + 0.25;
    if (sign === '-') return n - 0.25;
    return n;
  }

  function parseDifficulty(d){
    var s = String(d || '').toUpperCase();
    s = s.replace(/SCHWIERIGKEIT|DIFFICULTY|GRADE|GRAD/g, ' ');
    var out = { a: NaN, m: NaN, wi: NaN, r: NaN };

    var m = s.match(/(?:^|[^A-Z])A\s*(\d{1,2})\s*([+\-])?/);
    if (m) out.a = signed(m[1], m[2]);
    m = s.match(/(?:^|[^A-Z])M\s*(\d{1,2})\s*([+\-])?/);
    if (m) out.m = signed(m[1], m[2]);
    m = s.match(/(?:^|[^A-Z])WI\s*(\d{1,2})\s*([+\-])?/);
    if (m) out.wi = signed(m[1], m[2]);

    var re = /(?:^|[^A-Z0-9])(1[0-2]|[1-9])\s*([+\-])?(?=\b|[^0-9])/g;
    var best = NaN, mm;
    while ((mm = re.exec(s)) !== null) {
      var v = signed(mm[1], mm[2]);
      if (isFinite(v)) best = isFinite(best) ? Math.max(best, v) : v;
    }
    out.r = best;
    return out;
  }

  function clampMinMax(minEl, maxEl){
    if (!minEl || !maxEl) return [NaN, NaN];
    var a = Number(minEl.value), b = Number(maxEl.value);
    if (!isFinite(a) || !isFinite(b)) return [NaN, NaN];
    if (a > b) { var t=a; a=b; b=t; minEl.value=a; maxEl.value=b; }
    return [a,b];
  }

  function isDefaultRange(minEl, maxEl){
    if (!minEl || !maxEl) return true;
    var minVal=Number(minEl.value), maxVal=Number(maxEl.value);
    var minDef=Number(minEl.min),   maxDef=Number(maxEl.max);
    if (!isFinite(minVal) || !isFinite(maxVal)) return true;
    return Math.abs(minVal-minDef)<1e-6 && Math.abs(maxVal-maxDef)<1e-6;
  }

  function inRange(v, min, max, minEl, maxEl){
    if (!isFinite(min) && !isFinite(max)) return true;
    if (!isFinite(v)) return isDefaultRange(minEl, maxEl);
    if (isFinite(min) && v < min) return false;
    if (isFinite(max) && v > max) return false;
    return true;
  }

  function getLayerGroup(name){
    if (!map.layerManager) return null;
    if (typeof map.layerManager.getLayerGroup === 'function') {
      return map.layerManager.getLayerGroup(name);
    }
    if (map.layerManager._byGroup && map.layerManager._byGroup[name]) {
      return map.layerManager._byGroup[name];
    }
    return null;
  }

  var rawGroup = getLayerGroup('EisfälleRaw');
  if (!rawGroup || typeof rawGroup.getLayers !== 'function') {
    console.warn('EisfälleRaw group not found.');
    return;
  }

  var rawMarkers = (rawGroup.getLayers() || []).filter(function(l){
    return l && typeof l.getLatLng === 'function';
  });

  function readMetaFromPopup(layer){
    var meta = { name:'', uid:'', difficulty:'', sun:NaN, grades:{a:NaN,m:NaN,wi:NaN,r:NaN} };
    try{
      var popup = layer.getPopup ? layer.getPopup() : null;
      var content = popup && popup.getContent ? popup.getContent() : '';
      if (content){
        var wrapper = document.createElement('div');
        wrapper.innerHTML = content;
        var node = wrapper.querySelector('.map-meta');
        if (node && node.dataset){
          meta.name = node.dataset.name || '';
          meta.uid  = node.dataset.uid  || '';
          meta.difficulty = node.dataset.difficulty || '';
          meta.sun  = num(node.dataset.sun);
        }
      }
    }catch(e){}
    meta.grades = parseDifficulty(meta.difficulty);
    return meta;
  }

  rawMarkers.forEach(function(m){ m._mapMeta = readMetaFromPopup(m); });

  if (!L.markerClusterGroup) {
    console.error('leaflet.markercluster not loaded (L.markerClusterGroup missing)');
    return;
  }

  // create ONE stable cluster group
  var cluster = L.markerClusterGroup({
    showCoverageOnHover: false,
    spiderfyOnMaxZoom: true,
    zoomToBoundsOnClick: true,
    disableClusteringAtZoom: 12,
    iconCreateFunction: function(cluster){
      var count = cluster.getChildCount();
      return new L.DivIcon({
        html: '<div style="background:#ff4fa3;color:#fff;border-radius:999px;width:42px;height:42px;line-height:42px;text-align:center;font-weight:700;font-size:20px;border:2px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,0.35);">' + count + '</div>',
  className: 'marker-cluster-custom',
iconSize: new L.Point(42, 42)
});
}
});

cluster.addLayers(rawMarkers);

// disable raw group (prevents flicker / removal)
try{ if (typeof rawGroup.clearLayers === 'function') rawGroup.clearLayers(); } catch(e){}

// register as overlay "Eisfälle"
try{
  if (map.layerManager && typeof map.layerManager.addLayer === 'function') {
    map.layerManager.addLayer(cluster, 'markercluster', 'icefalls_cluster', 'Eisfälle', null, null);
    if (typeof map.layerManager.showGroup === 'function') map.layerManager.showGroup('Eisfälle');
  } else {
    cluster.addTo(map);
  }
}catch(e){
  try{ cluster.addTo(map); }catch(e2){}
}

window._icefallsCluster = cluster;
window._icefallsAll = rawMarkers.slice();

var input   = el.querySelector('#mapFilterInput');
var status  = el.querySelector('#mapFilterStatus');
var aMin    = el.querySelector('#mapAmin');
var aMax    = el.querySelector('#mapAmax');
var mMin    = el.querySelector('#mapMmin');
var mMax    = el.querySelector('#mapMmax');
var wiMin   = el.querySelector('#mapWImin');
var wiMax   = el.querySelector('#mapWImax');
var rMin    = el.querySelector('#mapRmin');
var rMax    = el.querySelector('#mapRmax');
var sunMin  = el.querySelector('#mapSunMin');
var sunMax  = el.querySelector('#mapSunMax');
var aRangeTxt   = el.querySelector('#mapARangeTxt');
var mRangeTxt   = el.querySelector('#mapMRangeTxt');
var wiRangeTxt  = el.querySelector('#mapWIRangeTxt');
var rRangeTxt   = el.querySelector('#mapRRangeTxt');
var sunRangeTxt = el.querySelector('#mapSunRangeTxt');
var resetBtn = el.querySelector('#mapFilterReset');

if (typeof L !== 'undefined') {
  var filterBox = el.querySelector('#map-filter');
  if (filterBox) { L.DomEvent.disableClickPropagation(filterBox); L.DomEvent.disableScrollPropagation(filterBox); }
}

function updateRangeLabels(){
  var a = clampMinMax(aMin,aMax);
  var m = clampMinMax(mMin,mMax);
  var w = clampMinMax(wiMin,wiMax);
  var r = clampMinMax(rMin,rMax);
  var s = clampMinMax(sunMin,sunMax);
  if (aRangeTxt && isFinite(a[0]) && isFinite(a[1])) aRangeTxt.textContent = 'A' + fmtGrade(a[0]) + ' – A' + fmtGrade(a[1]);
  if (mRangeTxt && isFinite(m[0]) && isFinite(m[1])) mRangeTxt.textContent = 'M' + fmtGrade(m[0]) + ' – M' + fmtGrade(m[1]);
  if (wiRangeTxt && isFinite(w[0]) && isFinite(w[1])) wiRangeTxt.textContent = 'WI' + fmtGrade(w[0]) + ' – WI' + fmtGrade(w[1]);
  if (rRangeTxt && isFinite(r[0]) && isFinite(r[1])) rRangeTxt.textContent = fmtGrade(r[0]) + ' – ' + fmtGrade(r[1]);
  if (sunRangeTxt && isFinite(s[0]) && isFinite(s[1])) sunRangeTxt.textContent = s[0].toFixed(1) + ' – ' + s[1].toFixed(1) + ' h';
}

function resetFilters(){
  if (input) input.value = '';
  [[aMin,aMax],[mMin,mMax],[wiMin,wiMax],[rMin,rMax],[sunMin,sunMax]].forEach(function(pair){
    var mn = pair[0], mx = pair[1];
    if (mn && mn.min !== undefined) mn.value = mn.min;
    if (mx && mx.max !== undefined) mx.value = mx.max;
  });
}

function applyFilter(){
  if (!input || !status || !window._icefallsCluster || !window._icefallsAll) return;
  
  var term = input.value.trim().toLowerCase();
  updateRangeLabels();
  
  var a = clampMinMax(aMin,aMax);
  var m = clampMinMax(mMin,mMax);
  var w = clampMinMax(wiMin,wiMax);
  var r = clampMinMax(rMin,rMax);
  var s = clampMinMax(sunMin,sunMax);
  
  var visible = [];
  window._icefallsAll.forEach(function(layer){
    var meta = layer._mapMeta || {name:'',uid:'',difficulty:'',sun:NaN,grades:{a:NaN,m:NaN,wi:NaN,r:NaN}};
    var blob = (meta.name + ' ' + meta.uid + ' ' + meta.difficulty).toLowerCase();
    if (term && blob.indexOf(term) === -1) return;
    
    if (!inRange(meta.grades.a, a[0], a[1], aMin, aMax)) return;
    if (!inRange(meta.grades.m, m[0], m[1], mMin, mMax)) return;
    if (!inRange(meta.grades.wi,w[0], w[1], wiMin, wiMax)) return;
    if (!inRange(meta.grades.r, r[0], r[1], rMin, rMax)) return;
    if (!inRange(meta.sun,      s[0], s[1], sunMin, sunMax)) return;
    
    visible.push(layer);
  });
  
  window._icefallsCluster.clearLayers();
  window._icefallsCluster.addLayers(visible);
  
  status.textContent = visible.length + ' / ' + window._icefallsAll.length + ' Eisfälle';
}

if (input) input.addEventListener('input', applyFilter);
[aMin,aMax,mMin,mMax,wiMin,wiMax,rMin,rMax,sunMin,sunMax].forEach(function(elm){
  if (elm) elm.addEventListener('input', applyFilter);
});
if (resetBtn) resetBtn.addEventListener('click', function(){
  resetFilters(); updateRangeLabels(); applyFilter();
});

resetFilters();
updateRangeLabels();
applyFilter();
}
