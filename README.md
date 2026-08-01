# jlc-parts-supplement

A weekly index of in-stock JLCPCB parts that the
[jlcparts](https://github.com/yaqwsx/jlcparts) dataset doesn't publish.

## Why

jlcparts builds its shards by selecting components per `(category, subcategory)` pair:

```sql
WHERE present = 1 AND category = ? AND subcategory = ? AND last_on_stock > (now - 120 days)
```

A component with an empty category matches no pair, so it never reaches a shard. That currently
drops 123,942 components, 121,343 of them in stock. They aren't obscure parts: `C41378685` is a
stocked G-Switch battery connector, listed on JLC's own site under Connectors.

The cause is upstream of jlcparts. JLC's assembly API returned no type names for those records,
even for parts fetched in the same batch as their categorised neighbours.

## Contents

`manifest.json` and gzipped JSON-lines shards, one per category partition. The first line of a
shard names the columns; every later line is an array in that order.

| column | notes |
| --- | --- |
| `lcsc` | LCSC part code, e.g. `C41378685` |
| `mfr` | manufacturer part number |
| `manufacturer` | brand |
| `package` | JLC's specification string, e.g. `SMD,P=1.27mm` |
| `family` / `category` | JLC's two-level classification |
| `description` | JLC's description, which lists the part's specs as text |
| `stock` | units in stock at fetch time |
| `price` | unit price at quantity 1, USD |
| `class` | `basic`, `preferred` or `extended` |
| `moq` | minimum order quantity |

There are no parametric attribute fields, because JLC's API doesn't expose any. The `description`
carries the same values as text, and on a sample of 400 parts about 99% of the numeric specs in
upstream's attribute table were recoverable by parsing it.

## How it works

JLC's list endpoint caps pagination at 100 pages of 1000, so no single query can reach the ~704k
in-stock components. The category facet partitions that population exactly, its counts summing to
the unfiltered total, so the walk runs per category and splits the two families over the cap
(Connectors, Resistors) into their children. That comes to 103 partitions and about 770 requests.

It runs weekly. Since this is someone else's undocumented API, the fetcher is unhurried, retries
with backoff, and aborts rather than publishing a partial index.

## Use

```
https://elan.github.io/jlc-parts-supplement/manifest.json
```

## Licence

Code is MIT. The data is JLCPCB's product information from their public catalog.
