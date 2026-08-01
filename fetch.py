#!/usr/bin/env python3
"""Enumerate JLCPCB's in-stock catalog and publish it as gzipped shards.

Why this exists: the jlcparts dataset (which Kiln's search index is built from) silently drops any
component whose cached record has an empty category. Its `buildtables` step selects per
(category, subcategory) pair, so an uncategorised part matches no pair and is never emitted. That is
~124k parts, ~121k of them in stock. JLC's own API has categories for all of them.

Approach: JLC's component-list endpoint caps pagination at 100 pages, so a single walk can only reach
100k of ~704k in-stock parts. The category facet (searchType 1) partitions that population exactly,
so we walk per category, splitting the two families that exceed the cap into their children.

Not collected: parametric attributes as named fields (capacitance, tolerance, …). JLC's API doesn't
expose them; upstream gets those from LCSC's authenticated agent API. The `description` field does
carry the same values as text ("-30\u2103~+70\u2103 150V 200k\u03a9 540nm"), and on a sample of 400 parts
~99% of the numeric specs in upstream's attribute table were recoverable by parsing it, so a consumer
can derive range-searchable values without them.
"""

import argparse, gzip, hashlib, json, math, os, sys, time
from concurrent.futures import ThreadPoolExecutor
from urllib import request, error

API = "https://jlcpcb.com/api/overseas-pcb-order/v1/shoppingCart/smtGood/selectSmtComponentList/v2"
PAGE_SIZE = 1000
PAGE_CAP = 100                      # the API returns nothing beyond page 100
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# The columns we keep, in shard order. First line of each shard is this list, so a reader never
# guesses at positions.
COLUMNS = ["lcsc", "mfr", "manufacturer", "package", "family", "category",
           "description", "stock", "price", "class", "moq"]


def call(body, attempts=4):
    """POST with backoff. Their API occasionally 502s under load; a failed page must not lose a run."""
    payload = json.dumps(body).encode()
    for attempt in range(attempts):
        try:
            req = request.Request(API, data=payload,
                                  headers={"Content-Type": "application/json", "User-Agent": UA})
            with request.urlopen(req, timeout=60) as r:
                return json.load(r).get("data") or {}
        except (error.URLError, TimeoutError, json.JSONDecodeError) as e:
            if attempt == attempts - 1:
                raise
            time.sleep(2 ** attempt)
    return {}


def base_body(**extra):
    body = {"currentPage": 1, "pageSize": 1, "keyword": None, "componentLibraryType": None,
            "stockFlag": True, "stockSort": None, "searchSource": "search", "searchType": 3,
            "sortASC": "", "sortMode": "", "componentBrandList": [], "componentSpecificationList": [],
            "componentAttributeList": [], "paramList": [], "startStockNumber": None}
    body.update(extra)
    return body


def partitions():
    """The category facet, split so every partition fits inside the 100-page window.

    Returns [(family, subcategory|None, expected_count)]. The facet's counts sum exactly to the
    unfiltered in-stock total, which is what makes this an exhaustive partition rather than a guess.
    """
    data = call(base_body(searchType=1))
    facets = data.get("sortAndCountVoList") or []
    if not facets:
        raise RuntimeError("no category facet returned. the API shape has changed")
    out = []
    for f in facets:
        count = f.get("componentCount") or 0
        if count <= PAGE_SIZE * PAGE_CAP:
            out.append((f["sortName"], None, count))
            continue
        children = [c for c in (f.get("childSortList") or []) if isinstance(c, dict)]
        covered = sum(c.get("componentCount") or 0 for c in children)
        if covered < count:
            raise RuntimeError(f"{f['sortName']}: children cover {covered} of {count}; would lose parts")
        for c in children:
            out.append((f["sortName"], c.get("sortName"), c.get("componentCount") or 0))
    return out


def part_class(row):
    """JLC's tiers: componentLibraryType is base/expand, Preferred rides on its own flag."""
    if (row.get("componentLibraryType") or "").lower() == "base":
        return "basic"
    return "preferred" if row.get("preferredComponentFlag") else "extended"


def walk(family, sub, expected, throttle):
    """Page through one partition. Returns rows as COLUMNS-ordered lists."""
    extra = {"firstSortName": family}
    if sub:
        extra["secondSortName"] = sub
    rows, pages = [], max(1, math.ceil(expected / PAGE_SIZE))
    if pages > PAGE_CAP:
        raise RuntimeError(f"{family}/{sub}: {expected} parts needs {pages} pages, over the cap")
    for page in range(1, pages + 1):
        data = call(base_body(currentPage=page, pageSize=PAGE_SIZE, **extra))
        got = ((data.get("componentPageInfo") or {}).get("list")) or []
        if not got:
            break
        for r in got:
            rows.append([
                r.get("componentCode") or "",
                r.get("componentModelEn") or "",
                r.get("componentBrandEn") or "",
                r.get("componentSpecificationEn") or "",
                family,
                sub or r.get("componentTypeEn") or "",
                r.get("describe") or "",
                r.get("stockCount") or 0,
                r.get("initialPrice"),
                part_class(r),
                r.get("minPurchaseNum") or 0,
            ])
        time.sleep(throttle)
    return rows


JLCPARTS_DATA = "https://yaqwsx.github.io/jlcparts/data/"


def published_codes():
    """The LCSC codes already in the jlcparts dataset, so we publish only what it lacks.

    Their shards are gzipped jsonlines whose first line maps field names to column indices. About
    40 MB for the lot, which is nothing in CI, and it keeps the published artifact to the difference.
    """
    manifest = json.load(request.urlopen(JLCPARTS_DATA + "manifest.json", timeout=60))
    shards, seen = [], set()
    for c in manifest.get("categories", []):
        for s in (c.get("browseShards") or c.get("shards") or []):
            if s not in seen:
                seen.add(s)
                shards.append(s)

    def codes(shard):
        for attempt in range(3):
            try:
                with request.urlopen(JLCPARTS_DATA + shard, timeout=90) as r:
                    text = gzip.decompress(r.read()).decode("utf8", "replace")
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        lines = text.splitlines()
        if not lines:
            return []
        index = json.loads(lines[0]).get("lcsc")
        if index is None:
            return []
        out = []
        for line in lines[1:]:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if index < len(row) and isinstance(row[index], str):
                out.append(row[index])
        return out

    found = set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for got in pool.map(codes, shards):
            found.update(got)
    print(f"jlcparts publishes {len(found):,} parts across {len(shards)} shards", flush=True)
    # A collapsed set would make us republish the whole catalog as though it were missing.
    if len(found) < 400_000:
        raise RuntimeError(f"only {len(found)} published codes read; refusing to diff against that")
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="build", help="output directory")
    ap.add_argument("--exclude-published", action="store_true",
                    help="omit parts the jlcparts dataset already carries")
    ap.add_argument("--workers", type=int, default=2, help="concurrent partitions (be gentle)")
    ap.add_argument("--throttle", type=float, default=0.3, help="seconds between pages per worker")
    ap.add_argument("--only", help="run a single family, for testing")
    args = ap.parse_args()

    started = time.time()
    plan = partitions()
    if args.only:
        plan = [p for p in plan if p[0] == args.only]
        if not plan:
            sys.exit(f"no partition named {args.only!r}")
    expected_total = sum(p[2] for p in plan)
    print(f"{len(plan)} partitions, {expected_total:,} parts expected", flush=True)

    os.makedirs(args.out, exist_ok=True)
    known = published_codes() if args.exclude_published else set()
    shards, seen, total, walked = [], set(known), 0, 0

    def run(p):
        family, sub, count = p
        return p, walk(family, sub, count, args.throttle)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for (family, sub, count), rows in pool.map(run, plan):
            walked += len(rows)
            # Dedupe across partitions: a part filed under two categories would otherwise appear twice.
            fresh = [r for r in rows if r[0] and r[0] not in seen]
            seen.update(r[0] for r in fresh)
            name = f"{family}-{sub or 'all'}".lower()
            name = "".join(c if c.isalnum() else "-" for c in name).strip("-")[:80]
            path = os.path.join(args.out, f"{name}.jsonl.gz")
            with gzip.open(path, "wt", encoding="utf8") as fh:
                fh.write(json.dumps(COLUMNS) + "\n")
                for r in sorted(fresh):
                    fh.write(json.dumps(r, separators=(",", ":")) + "\n")
            digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
            shards.append({"file": os.path.basename(path), "family": family, "subcategory": sub,
                           "count": len(fresh), "expected": count, "sha256": digest,
                           "bytes": os.path.getsize(path)})
            total += len(fresh)
            print(f"  {family}/{sub or '*'}: {len(fresh)} new of {len(rows)} in stock "
                  f"(JLC says {count})", flush=True)

    manifest = {
        "version": 1,
        "excludesPublished": bool(known),
        "publishedCount": len(known),
        "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "totalComponents": total,
        "inStockWalked": walked,
        "inStockExpected": expected_total,
        "columns": COLUMNS,
        "shards": sorted(shards, key=lambda s: s["file"]),
        "note": "In-stock JLCPCB components with categories, including those the jlcparts dataset "
                "omits. No parametric attributes.",
    }
    with open(os.path.join(args.out, "manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=1)

    size = sum(s["bytes"] for s in shards)
    print(f"\nwalked {walked:,} in-stock parts, {total:,} of them absent from jlcparts", flush=True)
    print(f"{total:,} parts in {len(shards)} shards, {size/1e6:.1f} MB gzipped, "
          f"{time.time()-started:.0f}s", flush=True)
    # Guard the COVERAGE of the walk, not the size of the diff. The diff is legitimately a small
    # fraction of what we walk; a short walk is the failure worth refusing to publish on.
    if walked < expected_total * 0.95:
        sys.exit(f"walked only {walked:,} of {expected_total:,} in-stock parts; "
                 "refusing to publish a partial index")


if __name__ == "__main__":
    main()
