#!/usr/bin/env python3
"""Build the Globe view's boundary assets from Natural Earth.

The globe draws land from plain GeoJSON rather than TopoJSON: the reason to
reach for TopoJSON is size, and quantising coordinates to a grid the globe
cannot resolve anyway gets to the same place without a topology decoder in
the browser. Nothing here runs at request time — the output is committed.

Usage, with the Natural Earth GeoJSON releases downloaded alongside:

    python3 scripts/build-geo.py <ne_110m_admin_0_countries.geojson> \
                                 <ne_10m_admin_1_states_provinces.geojson> \
                                 public/geo

Source: https://github.com/nvkelso/natural-earth-vector (public domain).
"""
import json, os, sys, math, unicodedata, re

# Coordinates are snapped to a grid finer than the globe can resolve. At the
# widest the sphere is ~900px for 360°, and a drilled country ~600px for 20°,
# so 0.01° at world level and 0.04° inside a country are both sub-pixel — but
# the world file is small enough to keep the crisper grid either way.
WORLD_STEP = 0.01
ADMIN_STEP = 0.04
MIN_RING_POINTS = 4
MIN_RING_SPAN = 0.25          # degrees; drops specks that render as one pixel
ADMIN_MIN_SPAN = 0.5


def norm(s):
    s = unicodedata.normalize('NFD', str(s or ''))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9]+', '', s.lower())


def rings(geom):
    t, c = geom.get('type'), geom.get('coordinates') or []
    if t == 'Polygon':
        return [c[0]] if c else []
    if t == 'MultiPolygon':
        return [p[0] for p in c if p]
    return []


def clean(ring, step, min_span=MIN_RING_SPAN):
    out, last = [], None
    dp = max(0, -int(math.floor(math.log10(step))))
    for x, y in ring:
        p = [round(round(x / step) * step, dp), round(round(y / step) * step, dp)]
        if p != last:
            out.append(p)
            last = p
    if len(out) < MIN_RING_POINTS:
        return None
    xs = [p[0] for p in out]
    ys = [p[1] for p in out]
    if max(xs) - min(xs) < min_span and max(ys) - min(ys) < min_span:
        return None
    return out


def ring_area(ring):
    """Twice the signed area — used to pick the mainland and its centroid."""
    a = 0.0
    for i in range(len(ring) - 1):
        a += ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1]
    return abs(a) / 2


def centroid(ring):
    cx = cy = a = 0.0
    for i in range(len(ring) - 1):
        x0, y0 = ring[i]
        x1, y1 = ring[i + 1]
        f = x0 * y1 - x1 * y0
        a += f
        cx += (x0 + x1) * f
        cy += (y0 + y1) * f
    if abs(a) < 1e-9:
        xs = [p[0] for p in ring]
        ys = [p[1] for p in ring]
        return [sum(xs) / len(xs), sum(ys) / len(ys)]
    a *= 0.5
    return [round(cx / (6 * a), 3), round(cy / (6 * a), 3)]


def build_world(src, out_dir):
    d = json.load(open(src))
    # Grouped by ISO code, because Natural Earth files France and Clipperton
    # Island under FRA, and the last one written would otherwise both name the
    # country and decide where its bar stands.
    groups = {}
    for f in d['features']:
        p = f['properties']
        iso3 = p.get('ISO_A3')
        if iso3 in ('-99', None, ''):
            iso3 = p.get('ISO_A3_EH') or ''
        if iso3 in ('-99', None, ''):
            iso3 = p.get('ADM0_A3') or ''
        if not iso3 or iso3 == '-99':
            continue
        rs = [r for r in (clean(r, WORLD_STEP) for r in rings(f['geometry'])) if r]
        if not rs:
            continue
        g = groups.setdefault(iso3, {'id': iso3, 'name': None, 'pop': -1, 'r': []})
        g['r'].extend(rs)
        pop = p.get('POP_EST') or 0
        if pop > g['pop']:
            g['pop'] = pop
            g['name'] = p.get('NAME') or iso3
    feats = []
    for g in groups.values():
        # The centroid of the largest ring, not of the whole country: an
        # average over every island puts the United States in the Pacific.
        main = max(g['r'], key=ring_area)
        feats.append({'id': g['id'], 'name': g['name'], 'c': centroid(main), 'r': g['r']})
    feats.sort(key=lambda x: x['id'])
    path = os.path.join(out_dir, 'countries-110m.json')
    json.dump({'v': 1, 'step': WORLD_STEP, 'features': feats}, open(path, 'w'), separators=(',', ':'))
    print('world  %4d countries  %6.0f KB  %s' % (len(feats), os.path.getsize(path) / 1024, path))
    return {f['id']: f['c'] for f in feats}


def build_admin1(src, out_dir):
    d = json.load(open(src))
    by_country = {}
    for f in d['features']:
        p = f['properties']
        iso3 = p.get('adm0_a3')
        if not iso3:
            continue
        # The UK's admin-1 units in Natural Earth are local authority
        # districts, which is not the division anybody means by "region" and
        # not what a postcode resolves to. Its geonunit is: the four nations.
        group = p.get('geonunit') if iso3 == 'GBR' else None
        name = group or p.get('name') or p.get('iso_3166_2') or ''
        if not name:
            continue
        rs = [r for r in (clean(r, ADMIN_STEP, ADMIN_MIN_SPAN) for r in rings(f['geometry'])) if r]
        if not rs:
            continue
        keys = {norm(name)}
        for k in ('postal', 'iso_3166_2', 'gn_name', 'name_alt'):
            v = p.get(k)
            if not v or group:
                continue
            for part in str(v).split('|'):
                if part.strip():
                    keys.add(norm(part))
                    if '-' in part:
                        keys.add(norm(part.split('-')[-1]))
        bucket = by_country.setdefault(iso3, {})
        unit = bucket.get(name)
        if unit:
            unit['r'].extend(rs)
            unit['k'] |= keys
        else:
            bucket[name] = {'name': name, 'k': keys, 'r': rs}

    admin_dir = os.path.join(out_dir, 'admin1')
    os.makedirs(admin_dir, exist_ok=True)
    total = 0
    index = {}
    for iso3, units in sorted(by_country.items()):
        feats = []
        for u in sorted(units.values(), key=lambda x: x['name']):
            main = max(u['r'], key=ring_area)
            feats.append({'name': u['name'], 'k': sorted(u['k']),
                          'c': centroid(main), 'r': u['r']})
        path = os.path.join(admin_dir, iso3 + '.json')
        json.dump({'v': 1, 'iso3': iso3, 'features': feats}, open(path, 'w'), separators=(',', ':'))
        size = os.path.getsize(path)
        total += size
        index[iso3] = len(feats)
    json.dump({'v': 1, 'units': index}, open(os.path.join(out_dir, 'admin1-index.json'), 'w'),
              separators=(',', ':'))
    print('admin1 %4d countries  %6.0f KB total  %s/' % (len(index), total / 1024, admin_dir))


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(2)
    world_src, admin_src, out = sys.argv[1], sys.argv[2], sys.argv[3]
    os.makedirs(out, exist_ok=True)
    build_world(world_src, out)
    build_admin1(admin_src, out)
