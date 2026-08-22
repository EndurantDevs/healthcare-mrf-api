# ARC CI image

This directory defines the public job-container image used by trusted
same-repository CI on ephemeral Actions Runner Controller workers.

The image contains public toolchains only. It must never contain repository
source, credentials, private package configuration, deployment metadata, or
runtime data. Base images and workflow references are immutable digest or
commit pins. Workflows consume the published image by digest.

Pull requests from forks remain on GitHub-hosted runners. The self-hosted
boundary is also enforced outside this repository by an organization runner
group limited to this repository and the `CI` workflow. Jobs that require a
Docker daemon, service containers, or toolchains outside the pinned image stay
GitHub-hosted.
