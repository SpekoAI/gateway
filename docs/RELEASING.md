# Releasing Gateway

Customers pin release tags such as `spekoai/gateway:0.1.0` or `:0.1`.
`latest`, `edge` and `sha-<commit>` move on every merge to `main` and are not
a release.

## Cut a release

1. Make sure `main` is green and the change works against the production
   control plane. A field the control plane does not know yet breaks every
   session with `invalid_request` (HTTP 400). This happened with
   `sha-1506cd4` on 2026-11-04.
2. Pick the version:
   - Patch (`0.1.0` → `0.1.1`) for fixes only.
   - Minor (`0.1.x` → `0.2.0`) for new features or any change to the session
     protocol, environment variables or image layout.
3. Tag `main` and push the tag:

   ```sh
   git checkout main && git pull
   git tag -a v0.1.1 -m "v0.1.1"
   git push origin v0.1.1
   ```

4. The `Publish Docker image` workflow builds the image and pushes the
   `0.1.1` and `0.1` tags. It then creates the GitHub release with notes
   generated from the merged pull requests.
5. Edit the release notes on the releases page. Start with a short
   "What changed for you" section in plain words, and call out anything a
   customer must do.
6. Post the release link in the shared Slack channel of every customer it
   affects.
