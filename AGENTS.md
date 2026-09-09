# Repository guidance for coding agents

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Keep changes focused and preserve
unrelated work.

- Start feature and fix branches from `dev`; target `dev` with normal pull requests.
- Use focused local checks for changed behavior. GitHub CI on the current pull
  request head is the required full-validation gate. Never invoke local pre-push
  suites or hooks; use `git push --no-verify` when a push is authorized.
- Verify merged changes in DEV and coordinate changes to shared runtime state
  with its owner.
- Treat promotion from `dev` to stable `main` as a separate release requiring an
  explicit human request. Use a release pull request with **Rebase and merge**.
- Keep public code, logs, examples, and documentation free of credentials,
  personal data, and internal operational details. Use synthetic test fixtures.
