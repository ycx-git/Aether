# Personal fork development

Use `origin` (`ycx-git/Aether`) for development and `upstream` (`Science-Discovery/Aether`) for synchronization. The base branch is `dev`. Issues stay optional and disabled; existing templates remain available. Merged branches are retained as development evidence.

`CODEOWNERS`, the review team and CI-maintainer lists identify `ycx-git`. AI commands inherit the active model configuration. The collaboration skill targets this fork and does not require an issue before a PR.

## Local checks

Install dependencies with the repository's normal Bun setup. To initialize only the Git hooks after an install with lifecycle scripts disabled, run `bun node_modules/husky/bin.js`. The local hooks path is `.husky/_`; hooks are not installed globally.

Before pushing, the existing hook checks the Bun version range from `package.json` and runs the repository typecheck. It uses executables on `PATH`, without a machine-specific Node directory. Existing formatting and version conventions remain unchanged.

## GitHub checks

The required `dev` check is `typecheck` from GitHub Actions, with the branch required to be up to date. It applies to administrators, blocks force pushes and deletion, and does not require another person's review. Other inherited checks remain visible but are not declared required until their reliability is established.

GitHub protection and repository settings are stored on GitHub rather than in this checkout. Keep Issues and automatic branch deletion disabled. The optional model-backed review workflow still requires its own provider configuration and secret; no local credentials are copied by this setup.
