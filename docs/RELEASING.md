# Releasing Gateway

A release is a `vMAJOR.MINOR.PATCH` git tag on `main`. The `Publish Docker
image` workflow (`.github/workflows/publish-image.yml`) turns it into Docker
Hub tags and a GitHub release.

| Image tag | Moves when | For |
| --- | --- | --- |
| `spekoai/gateway:0.1.1` | Never | Production pins |
| `spekoai/gateway:0.1` | A newer `0.1.x` is released | Production, fixes only |
| `latest`, `edge`, `sha-<commit>` | Every merge to `main` | Testing. Not a release |

## Cut a release

1. Make sure `main` is green and the change works against the production
   control plane. A field the control plane does not know yet breaks every
   session with `invalid_request` (HTTP 400).
2. Pick the version:
   - Patch (`0.1.0` to `0.1.1`) for fixes only.
   - Minor (`0.1.x` to `0.2.0`) for new features or any change to the session
     protocol, environment variables or image layout.
3. Tag `main` and push the tag:

   ```sh
   git checkout main && git pull
   git tag -a v0.1.1 -m "v0.1.1"
   git push origin v0.1.1
   ```

4. Wait for the workflow run to finish. It pushes the `0.1.1` and `0.1` image
   tags, then creates the GitHub release with notes generated from the merged
   pull requests.
5. Edit the release notes on the releases page. Start with a short "What
   changed for you" section in plain words, and call out anything a customer
   must do.
6. Post the release link in the shared Slack channel of every customer it
   affects.

## Rules

- Only `vMAJOR.MINOR.PATCH` tags publish. Prerelease tags do not.
- The workflow refuses a tag that already has a GitHub release, so a released
  image never changes. Cut a new patch instead.
- The workflow refuses a tag older than a released patch on the same minor,
  so the minor alias never moves back.
- Cut one release at a time. Push the next tag only after the previous run
  has finished.
