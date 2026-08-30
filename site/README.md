# site

The project page, deployed to GitHub Pages by `.github/workflows/pages.yml`
on any push to `main` that touches this directory.

A single static HTML file plus two images — no build step, no dependencies.
Preview it locally with:

    python3 -m http.server 8000 --directory site

The images are captures of Istio's own Grafana and Kiali, taken while the load
generator in `deploy/k8s/loadgen.yaml` was running against the cluster created
by `make cluster && make deploy`.
