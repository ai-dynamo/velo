# Documentation

This book is the documentation of Velo. The source is in `docs/src/`. A GitHub Actions workflow builds it and publishes it to GitHub Pages on each push to `main`.

## Where text goes

| Text | Place |
|---|---|
| How a subsystem works, and why | A chapter in this book |
| Measured numbers, with their setup | A chapter under Operations |
| Rejected designs and negative results | A chapter under Development |
| The reason for a line of code | A short code comment |
| A hard lesson ("X breaks when Y") | A test. Its doc comment can be long. |
| Plans, work in progress, session notes | `agent-docs/` on a work branch. **Never merged to `main`.** |

## Rules

- **`agent-docs/` never merges to `main`.** Use it for notes while work is in progress. Before the PR merges, move the lasting parts into this book and delete the directory from the branch.
- **Keep only what lasts.** Write how it works, why, the numbers with their setup, and the results that did not work. Do not copy plans, phase or workstream labels, PR numbers, or handoff notes. History is in git.
- **Write in simple English.** Use the `simple-english` skill for the book, README files, and comments.
- **Keep comments short.** A comment says why, or which fault the code avoids. It does not repeat the code.
- **Update the book in the same PR.** Each PR that changes behavior updates the chapters that describe it and the top-level `README.md`.

## Build the book locally

Install the tools at the versions that CI uses:

```bash
cargo install --locked mdbook --version 0.4.52
cargo install --locked mdbook-mermaid --version 0.16.2
cargo install --locked mdbook-linkcheck --version 0.7.7
```

Then build and serve the book:

```bash
mdbook-mermaid install docs
mdbook serve docs --open
```

`mdbook build docs` also runs the link check. A broken internal link fails the build.
