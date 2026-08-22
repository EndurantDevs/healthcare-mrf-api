# ARC CI image

This directory defines the public job-container image used by trusted
`main`-branch CI on ephemeral Actions Runner Controller workers.

The image contains public toolchains only. It must never contain repository
source, credentials, private package configuration, deployment metadata, or
runtime data. Base images and workflow references are immutable digest or
commit pins. Workflows consume the published image by digest.

Direct pull-request jobs remain on GitHub-hosted runners. A later caller may
request ARC only through this workflow on protected `main`; the default-false
input is combined with exact repository, base-branch, non-fork, trusted-human
association, and non-bot checks. An organization runner group independently
limits self-hosted use to this repository and to `.github/workflows/ci.yml` on
`refs/heads/main`.

Trusted main and manual CI assign Kubernetes-compatible jobs to
`HEALTHCARE_MRF_CI_RUNNER`; only container packaging and security use
`HEALTHCARE_MRF_DIND_CI_RUNNER`. Direct pull-request CI and all forks keep the
hosted fallback until the protected caller and required-check migration are
separately reviewed and activated.
