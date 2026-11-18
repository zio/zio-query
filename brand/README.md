# ZIO Query brand

The mark is a tally turned on its side. Four horizontal lines are four requests; one cobalt strike crosses them all, the way ZIO Query sends many requests to a data source as one batch. The first line, the last line and the strike draw the Z of ZIO, so in the full logo the mark is the Z of "ZIO", followed by "IO Query" in Sora: "IO" bold, "Query" regular. Lines and strike share one weight; only the strike's angle and colour set it apart.

## Files

**Logo and mark**
- `zio-query-logo-light.svg`, `zio-query-logo-dark.svg`: full logo, horizontal (the default), and the README header. The mark at 0.82 em, "IO" in Sora 700, a word space, "Query" in Sora 400, −2% tracking. Text converted to outlines.
- `zio-query-logo-stacked-light.svg`, `zio-query-logo-stacked-dark.svg`: "ZIO" over "Query", for square spaces and covers.
- `zio-query-mark-light.svg`, `zio-query-mark-dark.svg`: the bare mark, for building a lockup in live type.
- `zio-query-mark-mono.svg`: one colour (`currentColor`), for stamps, engraving and single-ink print.
- `zio-query-symbol.svg`: the cobalt tile with tint lines and a white strike, the default symbol.
- `zio-query-symbol-dark.svg`: the ink tile, for cobalt or busy grounds.

**Icons**
- `favicon.svg`: the browser tab icon. It uses the small cut (three heavier lines and the strike), which stays legible at 16 px. One file for light and dark tabs.
- `favicon.ico` (16, 32, 48), `favicon-16.png`, `favicon-32.png`, `favicon-48.png`: the small cut as rasters.
- `apple-touch-icon.png` (180): square corners, iOS rounds them.
- `icon-192.png`, `icon-512.png`: the rounded tile, for the web app manifest.
- `icon-maskable.svg`, `icon-maskable-512.png`: full-bleed square, mark at 56% so it stays inside Android's safe circle.
- `avatar-500.png`: full-bleed square for GitHub, Slack, X and Discord, which crop it to a circle or rounded square.

**Cards** (PNG at 2×, slide at 1×). One template shared with ZIO Config and ZIO Blocks: the logo, the tagline (38 px semibold), a supporting line (29 px), a label (25 px) over a short cobalt rule, the data sources and the address (25 px), with a faint mark bleeding off the right edge.
- `social-og-light.png`, `social-og-dark.png`: 1200 × 630, `og:image`.
- `social-x-light.png`, `social-x-dark.png`: 1200 × 600, `twitter:image`.
- `social-linkedin-light.png`, `social-linkedin-dark.png`: 1200 × 627, LinkedIn shares.
- `social-github-light.png`, `social-github-dark.png`: 1280 × 640, repository Settings › Social preview.
- `release-card.png`: 1200 × 630 release posts. Version and highlights are placeholders.
- `slide-title.png`: 1920 × 1080 talk title slide. Title, speaker and event are placeholders.

**Tokens and sheet**
- `tokens.css`: every colour, font and radius as `--zq-*` CSS variables, light by default and dark through `prefers-color-scheme`.
- `brand-sheet.html`, `brand-sheet.png`: the one-page overview.
- `sheet/`: the eleven brand-sheet boards (overview, logo, icons, colour, type, voice, imagery, hero, social, browser, assets) as canvas sources (`.dc.html`) and PNG.

## Palette

| Role | Light | Dark |
|---|---|---|
| Accent: the strike, buttons, tile | Cobalt `#2563EB` (5.17:1 on white) | Cobalt 400 `#60A5FA` (7.17:1 on ink) |
| Links, code keywords | Cobalt 700 `#1D4ED8` (6.70:1) | Cobalt 300 `#93C5FD` (10.12:1) |
| Text on cobalt | White `#FFFFFF` (5.17:1) | Ink `#101521` |
| Text / page | Ink `#101521` on Paper `#FFFFFF` (18.24:1) | Snow `#F4F5F8` on Ink `#101521` (16.73:1) |
| Secondary text, panel, line | `#566074` (6.32:1), `#F4F5F8`, `#E3E6EC` | `#A7AEBD` (8.19:1), `#171C28`, `#232833` |
| Lines of the mark on the tile | Tint `#BFDBFE` (3.64:1 on cobalt, shapes only) | same |

Cobalt belongs on the strike and in one accent per layout. The ramp follows the Tailwind blue scale. In the ZIO family, HTTP is mint, JSON raspberry, Kafka orange and Schema violet; ZIO Blocks also uses a blue, so compare the two side by side before shipping.

## Docs site `<head>`

```html
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="icon" href="/favicon.ico" sizes="32x32">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="manifest" href="/site.webmanifest">
<meta name="theme-color" content="#FFFFFF" media="(prefers-color-scheme: light)">
<meta name="theme-color" content="#101521" media="(prefers-color-scheme: dark)">
<meta property="og:image" content="/social-og-dark.png">
```

## README header

The repository README is generated from `docs/index.md` by `sbt docs/generateReadme`, so the header is set in `build.sbt` through the `readmeBanner` key of `zio-sbt-website`. It is the horizontal logo alone, with no tagline or background, at 520 px wide:

```html
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/zio/zio-query/series/2.x/brand/zio-query-logo-dark.svg">
  <img alt="ZIO Query" src="https://raw.githubusercontent.com/zio/zio-query/series/2.x/brand/zio-query-logo-light.svg" width="520">
</picture>
```

The logo spells the name, so the plugin leaves out the `# ZIO Query` heading.

## Rebuild

Needs `uv`, headless Chromium, and the two open-source fonts from the Google Fonts repository: `Sora[wght].ttf` (`ofl/sora`) and `JetBrainsMono[wght].ttf` (`ofl/jetbrainsmono`), both under the SIL Open Font License.

```bash
uv run --with fonttools --with uharfbuzz python tools/build_brand.py \
  <Sora[wght].ttf> <JetBrainsMono[wght].ttf> . sheet
```

The script writes the SVG sources (wordmark shaped with HarfBuzz, kerning on, then outlined) and `tokens.css`, rasterises the icons with Chromium, builds `favicon.ico`, cuts the social cards, release card and slide out of board 8 (drawn there at full size inside scaled previews), and renders every board in `sheet/` to PNG. To rebuild from changed boards, pass the folder that holds the edited `Brand*.dc.html` files as the last argument.

Every surface uses one tagline: "Batching, pipelining and caching for any data source". "Many requests, one round trip" stays as a secondary line for body copy and talks. The tagline, the `zio.dev/zio-query` address and the sample copy are proposals for the maintainers to confirm.
