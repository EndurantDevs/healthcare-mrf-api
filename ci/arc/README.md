# ARC CI image

This directory defines the public job-container image used by trusted
`main`-branch CI on ephemeral Actions Runner Controller workers.

The image contains public toolchains only. It must never contain repository
source, credentials, private package configuration, deployment metadata, or
runtime data. Base images and workflow references are immutable digest or
commit pins. Workflows consume the published image by digest.

Pull-request jobs remain on GitHub-hosted runners. The self-hosted boundary is
also enforced outside this repository by an organization runner group that is
limited to this repository and to the `CI` workflow on `refs/heads/main`.
