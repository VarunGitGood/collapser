# site

The project page, deployed to GitHub Pages by `.github/workflows/pages.yml`
on any push to `main` that touches this directory.

A single static HTML file plus a checked-in Grafana capture—no build step, no
dependencies.
Preview it locally with:

    python3 -m http.server 8000 --directory site
